import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import re
import json

# pandas==2.3.2
# beautifulsoup4==4.13.5
# aiohttp==3.12.15


'''Quote's scraper'''

async def fetch(session, url):
    async with session.get(url) as content:
        try:
            req = await content.text()
            soup = BeautifulSoup(req, 'html.parser')
            quotes = soup.select('div.quote')
            quote = []

            for q in quotes:
                    qute = q.find(attrs={"class":"text"})
                    athr = q.find(attrs={"class":"author"})
                    quote.append([qute.get_text(), athr.get_text()])
                
            return quote

        except Exception as e:
            print(f"Error in page: {e}")


async def scrap_quotes(base_url, max_page, session):
    tasks = []

    for i in range(1, max_page+1):
        if i == 1:
            url = base_url
        else:
            url = base_url + f'/page/{i}/'  
        tasks.append(fetch(session, url))
    
    results = await asyncio.gather(*tasks)

    quotes_data = []   #move all quotes in to a single list.
    for page in results:
        for quote in page:
            quotes_data.append(quote)

    return quotes_data


def create_table(row_data, columns, output_file='quote_2.csv'):  
    df = pd.DataFrame(row_data, columns= columns)
    print(df.tail(20))
    print('________________________________________')
    print('Observation on data')
    print(' ')
    print(f"> Number of column {df.shape[1]}")
    print(f"> Number of row {df.shape[0]}")
    df.to_csv(output_file, index=False)
    print(df.info())



'''Book Scraper'''

async def fetch_books(page_links, category, session):
    try:
        book_database = dict()
        books_links = []

        for k in range(len(page_links)):
            url = 'https://books.toscrape.com/' + page_links[k]
            async with session.get(url) as content:
            # print(url)
                req = await content.text()                
                soup = BeautifulSoup(req, 'html.parser')

                book_datas = soup.select('ol.row li h3 a')
                inner_page_links = [b['href'] for b in book_datas]

                pattern = "../../.."
                for d in inner_page_links:
                    books_links.append(re.sub(pattern, '', d))

                title_price_availability = await asyncio.gather(fetch_book_data(books_links, session))
                book_database[category[k]] = title_price_availability
                
                print(f"Fetched {len(title_price_availability)} books from category: {category[k]}")
            
        return book_database

    except Exception as e:
        print(f"Error fetching category links {e}")


async def fetch_book_data(book_links, session):

    book_records = []
    try:        
        for link in book_links:
            inner_url = 'https://books.toscrape.com/catalogue' + link
            # print(inner_url)

            async with session.get(inner_url) as content:
                req = await content.text()    
                soup = BeautifulSoup(req, 'html.parser')

                data = soup.find(attrs={'class':'col-sm-6 product_main'})
                # print(data)

                title = data.find('h1').get_text(strip=True)
                price = re.sub('Â', '', data.find('p').get_text(strip=True))     
                availability = data.find(
                    attrs={'class':'instock availability'}
                ).get_text(strip=True)

                # book_records.append([title, price, availability])
                book_records.append({
                    "title": title,
                    "price":price,
                    "availability":availability
                })

        return book_records

    except Exception as e:
        print(f"Error fetching book data: {e}")


async def scrape_all_books(base_url, session):

    async with session.get(base_url) as response:
        req = await response.text()
        soup = BeautifulSoup(req, 'html.parser')

        links = soup.select('div.side_categories a')
        category = [l.get_text(strip=True) for l in links[1:]]
        page_links = [l['href'] for l in links[1:]]    
        
        book_details = await asyncio.gather(fetch_books(page_links, category, session))

        if not book_details:
            print("error fetching book data")
        
        with open('book_data_2.json', 'w', encoding='utf-8') as f:
            # f.write(book_details)
            json.dump(book_details, f, indent=4, ensure_ascii=False)
        
        return book_details



async def main():
    base_url = 'https://quotes.toscrape.com/'

    async with aiohttp.ClientSession() as session:
        return await scrap_quotes(base_url=base_url, max_page= 10, session=session)
    
async def second_main():
    base_url = 'https://books.toscrape.com/index.html'
    async with aiohttp.ClientSession() as session:
        return await scrape_all_books(base_url, session)
    


if __name__ == "__main__":
    start = time.perf_counter()
        
    quote_data =  asyncio.run(main()) 
    create_table(row_data= quote_data, columns= ['Quote', 'Author'])
    print("Scraper 1 ran successfully")
    print('') # 2 sec    

    asyncio.run(second_main())
    print("Scraper 2 ran successfully")
    print(' ') # 14 min(1-15)    

    print(f"Time taken {round(time.perf_counter() - start, 2)} sec") # 16 minute
    




