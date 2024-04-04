---
slug: /en/getting-started/example-datasets/amazon-reviews
sidebar_label: Amazon customer reviews
---

# Amazon customer reviews dataset

[**Amazon Customer Reviews**](https://s3.amazonaws.com/amazon-reviews-pds/readme.html) (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. This makes Amazon Customer Reviews a rich source of information for academic researchers in the fields of Natural Language Processing (NLP), Information Retrieval (IR), and Machine Learning (ML), amongst others. By accessing the dataset, you agree to the [license terms](https://s3.amazonaws.com/amazon-reviews-pds/license.txt).

The data is in a tab-separated format in gzipped files are up in AWS S3. Let's walk through the steps to insert it into ClickHouse.

:::note
The queries below were executed on a **Production** instance of [ClickHouse Cloud](https://clickhouse.cloud).
:::


1. Without inserting the data into ClickHouse, we can query it in place. Let's grab some rows so we can see what they look like:

```sql
SELECT *
FROM s3('https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Wireless_v1_00.tsv.gz',
    'TabSeparatedWithNames',
    'marketplace String,
    customer_id Int64,
    review_id String,
    product_id String,
    product_parent Int64,
    product_title String,
    product_category String,
    star_rating Int64,
    helpful_votes Int64,
    total_votes Int64,
    vine Bool,
    verified_purchase Bool,
    review_headline String,
    review_body String,
    review_date Date'
)
LIMIT 10;
```

The rows look like:

```response
┌─marketplace─┬─customer_id─┬─review_id──────┬─product_id─┬─product_parent─┬─product_title──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─product_category─┬─star_rating─┬─helpful_votes─┬─total_votes─┬─vine──┬─verified_purchase─┬─review_headline───────────┬─review_body────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─review_date─┐
│ US          │    16414143 │ R3W4P9UBGNGH1U │ B00YL0EKWE │      852431543 │ LG G4 Case Hard Transparent Slim Clear Cover for LG G4                                                                                                                                                                                                     │ Wireless         │           2 │             1 │           3 │ false │ true              │ Looks good, functions meh │ 2 issues  - Once I turned on the circle apps and installed this case,  my battery drained twice as fast as usual.  I ended up turning off the circle apps, which kind of makes the case just a case...  with a hole in it.  Second,  the wireless charging doesn't work.  I have a Motorola 360 watch and a Qi charging pad. The watch charges fine but this case doesn't. But hey, it looks nice. │  2015-08-31 │
│ US          │    50800750 │ R15V54KBMTQWAY │ B00XK95RPQ │      516894650 │ Selfie Stick Fiblastiq&trade; Extendable Wireless Bluetooth Selfie Stick with built-in Bluetooth Adjustable Phone Holder                                                                                                                                   │ Wireless         │           4 │             0 │           0 │ false │ false             │ A fun little gadget       │ I’m embarrassed to admit that until recently, I have had a very negative opinion about “selfie sticks” aka “monopods” aka “narcissticks.” But having reviewed a number of them recently, they’re growing on me. This one is pretty nice and simple to set up and with easy instructions illustrated on the back of the box (not sure why some reviewers have stated that there are no instructions when they are clearly printed on the box unless they received different packaging than I did). Once assembled, the pairing via bluetooth and use of the stick are easy and intuitive. Nothing to it.<br /><br />The stick comes with a USB charging cable but arrived with a charge so you can use it immediately, though it’s probably a good idea to charge it right away so that you have no interruption of use out of the box. Make sure the stick is switched to on (it will light up) and extend your stick to the length you desire up to about a yard’s length and snap away.<br /><br />The phone clamp held the phone sturdily so I wasn’t worried about it slipping out. But the longer you extend the stick, the harder it is to maneuver.  But that will happen with any stick and is not specific to this one in particular.<br /><br />Two things that could improve this: 1) add the option to clamp this in portrait orientation instead of having to try and hold the stick at the portrait angle, which makes it feel unstable; 2) add the opening for a tripod so that this can be used to sit upright on a table for skyping and facetime eliminating the need to hold the phone up with your hand, causing fatigue.<br /><br />But other than that, this is a nice quality monopod for a variety of picture taking opportunities.<br /><br />I received a sample in exchange for my honest opinion. │  2015-08-31 │
│ US          │    15184378 │ RY8I449HNXSVF  │ B00SXRXUKO │      984297154 │ Tribe AB40 Water Resistant Sports Armband with Key Holder for 4.7-Inch iPhone 6S/6/5/5S/5C, Galaxy S4 + Screen Protector - Dark Pink                                                                                                                       │ Wireless         │           5 │             0 │           0 │ false │ true              │ Five Stars                │ Fits iPhone 6 well                                                                                                                                                                                                                                         │  2015-08-31 │
│ US          │    10203548 │ R18TLJYCKJFLSR │ B009V5X1CE │      279912704 │ RAVPower® Element 10400mAh External Battery USB Portable Charger (Dual USB Outputs, Ultra Compact Design), Travel Charger for iPhone 6,iPhone 6 plus,iPhone 5, 5S, 5C, 4S, 4, iPad Air, 4, 3, 2, Mini 2 (Apple adapters not included); Samsung Galaxy S5, S4, S3, S2, Note 3, Note 2; HTC One, EVO, Thunderbolt, Incredible, Droid DNA, Motorola ATRIX, Droid, Moto X, Google Glass, Nexus 4, Nexus 5, Nexus 7, │ Wireless         │           5 │             0 │           0 │ false │ true              │ Great charger             │ Great charger.  I easily get 3+ charges on a Samsung Galaxy 3.  Works perfectly for camping trips or long days on the boat.                                                                                                                                │  2015-08-31 │
│ US          │      488280 │ R1NK26SWS53B8Q │ B00D93OVF0 │      662791300 │ Fosmon Micro USB Value Pack Bundle for Samsung Galaxy Exhilarate - Includes Home / Travel Charger, Car / Vehicle Charger and USB Cable                                                                                                                     │ Wireless         │           5 │             0 │           0 │ false │ true              │ Five Stars                │ Great for the price :-)                                                                                                                                                                                                                                    │  2015-08-31 │
│ US          │    13334021 │ R11LOHEDYJALTN │ B00XVGJMDQ │      421688488 │ iPhone 6 Case, Vofolen Impact Resistant Protective Shell iPhone 6S Wallet Cover Shockproof Rubber Bumper Case Anti-scratches Hard Cover Skin Card Slot Holder for iPhone 6 6S                                                                              │ Wireless         │           5 │             0 │           0 │ false │ true              │ Five Stars                │ Great Case, better customer service!                                                                                                                                                                                                                       │  2015-08-31 │
│ US          │    27520697 │ R3ALQVQB2P9LA7 │ B00KQW1X1C │      554285554 │ Nokia Lumia 630 RM-978 White Factory Unlocked - International Version No Warranty                                                                                                                                                                          │ Wireless         │           4 │             0 │           0 │ false │ true              │ Four Stars                │ Easy to set up and use. Great functions for the price                                                                                                                                                                                                      │  2015-08-31 │
│ US          │    48086021 │ R3MWLXLNO21PDQ │ B00IP1MQNK │      488006702 │ Lumsing 10400mah external battery                                                                                                                                                                                                                          │ Wireless         │           5 │             0 │           0 │ false │ true              │ Five Stars                │ Works great                                                                                                                                                                                                                                                │  2015-08-31 │
│ US          │    12738196 │ R2L15IS24CX0LI │ B00HVORET8 │      389677711 │ iPhone 5S Battery Case - iPhone 5 Battery Case , Maxboost Atomic S [MFI Certified] External Protective Battery Charging Case Power Bank Charger All Versions of Apple iPhone 5/5S [Juice Battery Pack]                                                     │ Wireless         │           5 │             0 │           0 │ false │ true              │ So far so good            │ So far so good. It is essentially identical to the one it replaced from another company. That one stopped working after 7 months so I am a bit apprehensive about this one.                                                                                │  2015-08-31 │
│ US          │    15867807 │ R1DJ8976WPWVZU │ B00HX3G6J6 │      299654876 │ HTC One M8 Screen Protector, Skinomi TechSkin Full Coverage Screen Protector for HTC One M8 Clear HD Anti-Bubble Film │ Wireless         │           3 │             0 │           0 │ false │ true              │ seems durable but these are always harder to get on ... │ seems durable but these are always harder to get on right than people make them out to be. also send to curl up at the edges after a while. with today's smartphones, you hardly need screen protectors anyway. │  2015-08-31 │
└─────────────┴─────────────┴────────────────┴────────────┴────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────┴─────────────┴───────────────┴─────────────┴───────┴───────────────────┴─────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────┘
```

:::note
Normally you would not need to pass in the schema into the `s3` table function - ClickHouse can infer the names and data types of the columns. However, this particular dataset uses a non-standard tab-separated format, but the `s3` function seems to work fine with this non-standard format if you include the schema.
:::

2. Let's define a new table named `amazon_reviews`. We'll optimize some of the column data types - and choose a primary key (the `ORDER BY` clause):

```sql
CREATE TABLE amazon_reviews
(
    review_date Date,
    marketplace LowCardinality(String),
    customer_id UInt64,
    review_id String,
    product_id String,
    product_parent UInt64,
    product_title String,
    product_category LowCardinality(String),
    star_rating UInt8,
    helpful_votes UInt32,
    total_votes UInt32,
    vine Bool,
    verified_purchase Bool,
    review_headline String,
    review_body String
)
ENGINE = MergeTree
ORDER BY (marketplace, review_date, product_category);
```

3. We are now ready to insert the data into ClickHouse. Before we do, check out the [list of files in the dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt) and decide which ones you want to include.

4. We will insert all of the US reviews - which is about 151M rows. The following `INSERT` command uses the `s3Cluster` table function, which allows the processing of multiple S3 files in parallel using all the nodes of your cluster. We also use a wildcard to insert any file that starts with the name `https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_`:

```sql
INSERT INTO amazon_reviews
WITH
   transform(vine, ['Y','N'],[true, false]) AS vine,
   transform(verified_purchase, ['Y','N'],[true, false]) AS verified_purchase
SELECT
   *
FROM s3Cluster(
    'default',
    'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_*.tsv.gz',
    'TSVWithNames',
    'review_date Date,
    marketplace LowCardinality(String),
    customer_id UInt64,
    review_id String,
    product_id String,
    product_parent UInt64,
    product_title String,
    product_category LowCardinality(String),
    star_rating UInt8,
    helpful_votes UInt32,
    total_votes UInt32,
    vine FixedString(1),
    verified_purchase FixedString(1),
    review_headline String,
    review_body String'
    )
SETTINGS input_format_allow_errors_num = 1000000;
```

:::tip
In ClickHouse Cloud, there is a cluster named `default`. Change `default` to the name of your cluster...or use the `s3` table function (instead of `s3Cluster`) if you do not have a cluster.
:::

5. That query doesn't take long - within 5 minutes or so you should see all the rows inserted:

```sql
SELECT formatReadableQuantity(count())
FROM amazon_reviews
```

```response
┌─formatReadableQuantity(count())─┐
│ 150.96 million                  │
└─────────────────────────────────┘
```

6. Let's see how much space our data is using:

```sql
SELECT
    disk_name,
    formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
    round(usize / size, 2) AS compr_rate,
    sum(rows) AS rows,
    count() AS part_count
FROM system.parts
WHERE (active = 1) AND (table = 'amazon_reviews')
GROUP BY disk_name
ORDER BY size DESC;
```
The original data was about 70G, but compressed in ClickHouse it takes up about 30G:

```response
┌─disk_name─┬─compressed─┬─uncompressed─┬─compr_rate─┬──────rows─┬─part_count─┐
│ s3disk    │ 30.00 GiB  │ 70.61 GiB    │       2.35 │ 150957260 │          9 │
└───────────┴────────────┴──────────────┴────────────┴───────────┴────────────┘
```

7. Let's run some queries...here are the top 10 most-helpful reviews on Amazon:

```sql
SELECT
    product_title,
    review_headline
FROM amazon_reviews
ORDER BY helpful_votes DESC
LIMIT 10;
```

Notice the query has to process all 151M rows, and it takes about 17 seconds:

```response
┌─product_title────────────────────────────────────────────────────────────────────────────┬─review_headline───────────────────────────────────────────────────────┐
│ Kindle: Amazon's Original Wireless Reading Device (1st generation)                       │ Why and how the Kindle changes everything                             │
│ BIC Cristal For Her Ball Pen, 1.0mm, Black, 16ct (MSLP16-Blk)                            │ FINALLY!                                                              │
│ The Mountain Kids 100% Cotton Three Wolf Moon T-Shirt                                    │ Dual Function Design                                                  │
│ Kindle Keyboard 3G, Free 3G + Wi-Fi, 6" E Ink Display                                    │ Kindle vs. Nook (updated)                                             │
│ Kindle Fire HD 7", Dolby Audio, Dual-Band Wi-Fi                                          │ You Get What You Pay For                                              │
│ Kindle Fire (Previous Generation - 1st)                                                  │ A great device WHEN you consider price and function, with a few flaws │
│ Fifty Shades of Grey: Book One of the Fifty Shades Trilogy (Fifty Shades of Grey Series) │ Did a teenager write this???                                          │
│ Wheelmate Laptop Steering Wheel Desk                                                     │ Perfect for an Starfleet Helmsman                                     │
│ Kindle Wireless Reading Device (6" Display, U.S. Wireless)                               │ BEWARE of the SIGNIFICANT DIFFERENCES between Kindle 1 and Kindle 2!  │
│ Tuscan Dairy Whole Vitamin D Milk, Gallon, 128 oz                                        │ Make this your only stock and store                                   │
└──────────────────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────┘

10 rows in set. Elapsed: 17.595 sec. Processed 150.96 million rows, 15.36 GB (8.58 million rows/s., 872.89 MB/s.)
```

8. Here are the top 10 products in Amazon with the most reviews:

```sql
SELECT
    any(product_title),
    count()
FROM amazon_reviews
GROUP BY product_id
ORDER BY 2 DESC
LIMIT 10;
```

```response
┌─any(product_title)────────────────────────────┬─count()─┐
│ Candy Crush Saga                              │   50051 │
│ The Secret Society® - Hidden Mystery          │   41255 │
│ Google Chromecast HDMI Streaming Media Player │   35977 │
│ Minecraft                                     │   35129 │
│ Bosch Season 1                                │   33610 │
│ Gone Girl: A Novel                            │   33240 │
│ Subway Surfers                                │   32328 │
│ The Fault in Our Stars                        │   30149 │
│ Amazon.com eGift Cards                        │   28879 │
│ Crossy Road                                   │   28111 │
└───────────────────────────────────────────────┴─────────┘

10 rows in set. Elapsed: 16.684 sec. Processed 195.05 million rows, 20.86 GB (11.69 million rows/s., 1.25 GB/s.)
```

9. Here are the average review ratings per month for each product (an actual [Amazon job interview question](https://datalemur.com/questions/sql-avg-review-ratings)!):

```sql
SELECT
    toStartOfMonth(review_date) AS month,
    any(product_title),
    avg(star_rating) AS avg_stars
FROM amazon_reviews
GROUP BY
    month,
    product_id
ORDER BY
    month DESC,
    product_id ASC
LIMIT 20;
```

It calculates all the monthly averages for each product, but we only returned 20 rows:

```response
┌──────month─┬─any(product_title)──────────────────────────────────────────────────────────────────────┬─avg_stars─┐
│ 2015-08-01 │ Mystiqueshapes Girls Ballet Tutu Neon Lime Green                                        │         4 │
│ 2015-08-01 │ Adult Ballet Tutu Yellow                                                                │         5 │
│ 2015-08-01 │ The Way Things Work: An Illustrated Encyclopedia of Technology                          │         5 │
│ 2015-08-01 │ Hilda Boswell's Treasury of Poetry                                                      │         5 │
│ 2015-08-01 │ Treasury of Poetry                                                                      │         5 │
│ 2015-08-01 │ Uncle Remus Stories                                                                     │         5 │
│ 2015-08-01 │ The Book of Daniel                                                                      │         5 │
│ 2015-08-01 │ Berenstains' B Book                                                                     │         5 │
│ 2015-08-01 │ The High Hills (Brambly Hedge)                                                          │       4.5 │
│ 2015-08-01 │ Fuzzypeg Goes to School (The Little Grey Rabbit library)                                │         5 │
│ 2015-08-01 │ Dictionary in French: The Cat in the Hat (Beginner Series)                              │         5 │
│ 2015-08-01 │ Windfallen                                                                              │         5 │
│ 2015-08-01 │ The Monk Who Sold His Ferrari: A Remarkable Story About Living Your Dreams              │         5 │
│ 2015-08-01 │ Illustrissimi: The Letters of Pope John Paul I                                          │         5 │
│ 2015-08-01 │ Social Contract: A Personal Inquiry into the Evolutionary Sources of Order and Disorder │         5 │
│ 2015-08-01 │ Mexico The Beautiful Cookbook: Authentic Recipes from the Regions of Mexico             │       4.5 │
│ 2015-08-01 │ Alanbrooke                                                                              │         5 │
│ 2015-08-01 │ Back to Cape Horn                                                                       │         4 │
│ 2015-08-01 │ Ovett: An Autobiography (Willow books)                                                  │         5 │
│ 2015-08-01 │ The Birds of West Africa (Collins Field Guides)                                         │         4 │
└────────────┴─────────────────────────────────────────────────────────────────────────────────────────┴───────────┘

20 rows in set. Elapsed: 52.827 sec. Processed 251.46 million rows, 35.26 GB (4.76 million rows/s., 667.55 MB/s.)
```

10. Here are the total number of votes per product category. This query is fast because `product_category` is in the primary key:

```sql
SELECT
    sum(total_votes),
    product_category
FROM amazon_reviews
GROUP BY product_category
ORDER BY 1 DESC;
```

```response
┌─sum(total_votes)─┬─product_category─────────┐
│        103877874 │ Books                    │
│         25330411 │ Digital_Ebook_Purchase   │
│         23065953 │ Video DVD                │
│         18048069 │ Music                    │
│         17292294 │ Mobile_Apps              │
│         15977124 │ Health & Personal Care   │
│         13554090 │ PC                       │
│         13065746 │ Kitchen                  │
│         12537926 │ Home                     │
│         11067538 │ Beauty                   │
│         10418643 │ Wireless                 │
│          9089085 │ Toys                     │
│          9071484 │ Sports                   │
│          7335647 │ Electronics              │
│          6885504 │ Apparel                  │
│          6710085 │ Video Games              │
│          6556319 │ Camera                   │
│          6305478 │ Lawn and Garden          │
│          5954422 │ Office Products          │
│          5339437 │ Home Improvement         │
│          5284343 │ Outdoors                 │
│          5125199 │ Pet Products             │
│          4733251 │ Grocery                  │
│          4697750 │ Shoes                    │
│          4666487 │ Automotive               │
│          4361518 │ Digital_Video_Download   │
│          4033550 │ Tools                    │
│          3559010 │ Baby                     │
│          3317662 │ Home Entertainment       │
│          2559501 │ Video                    │
│          2204328 │ Furniture                │
│          2157587 │ Musical Instruments      │
│          1881662 │ Software                 │
│          1676081 │ Jewelry                  │
│          1499945 │ Watches                  │
│          1224071 │ Digital_Music_Purchase   │
│           847918 │ Luggage                  │
│           503939 │ Major Appliances         │
│           392001 │ Digital_Video_Games      │
│           348990 │ Personal_Care_Appliances │
│           321372 │ Digital_Software         │
│           169585 │ Mobile_Electronics       │
│            72970 │ Gift Card                │
└──────────────────┴──────────────────────────┘

43 rows in set. Elapsed: 0.423 sec. Processed 150.96 million rows, 756.20 MB (356.70 million rows/s., 1.79 GB/s.)
```

11. Let's find the products with the word **"awful"** occurring most frequently in the review. This is a big task - over 151M strings have to be parsed looking for a single word:

```sql
SELECT
    product_id,
    any(product_title),
    avg(star_rating),
    count() AS count
FROM amazon_reviews
WHERE position(review_body, 'awful') > 0
GROUP BY product_id
ORDER BY count DESC
LIMIT 50;
```

The query takes a couple of minutes, but the results are a fun read:

```response

┌─product_id─┬─any(product_title)───────────────────────────────────────────────────────────────────────┬───avg(star_rating)─┬─count─┐
│ 0345803485 │ Fifty Shades of Grey: Book One of the Fifty Shades Trilogy (Fifty Shades of Grey Series) │ 1.3870967741935485 │   248 │
│ B007J4T2G8 │ Fifty Shades of Grey (Fifty Shades, Book 1)                                              │ 1.4439834024896265 │   241 │
│ B006LSZECO │ Gone Girl: A Novel                                                                       │ 2.2986425339366514 │   221 │
│ B00008OWZG │ St. Anger                                                                                │ 1.6565656565656566 │   198 │
│ B00BD99JMW │ Allegiant (Divergent Trilogy, Book 3)                                                    │ 1.8342541436464088 │   181 │
│ B0000YUXI0 │ Mavala Switzerland Mavala Stop Nail Biting                                               │  4.473684210526316 │   171 │
│ B004S8F7QM │ Cards Against Humanity                                                                   │  4.753012048192771 │   166 │
│ 031606792X │ Breaking Dawn (The Twilight Saga, Book 4)                                                │           1.796875 │   128 │
│ 006202406X │ Allegiant (Divergent Series)                                                             │ 1.4242424242424243 │    99 │
│ B0051VVOB2 │ Kindle Fire (Previous Generation - 1st)                                                  │ 2.7448979591836733 │    98 │
│ B00I3MP3SG │ Pilot                                                                                    │ 1.8762886597938144 │    97 │
│ 030758836X │ Gone Girl                                                                                │            2.15625 │    96 │
│ B0009X29WK │ Precious Cat Ultra Premium Clumping Cat Litter                                           │ 3.0759493670886076 │    79 │
│ B00JB3MVCW │ Noah                                                                                     │ 1.2027027027027026 │    74 │
│ B00BAXFECK │ The Goldfinch: A Novel (Pulitzer Prize for Fiction)                                      │  2.643835616438356 │    73 │
│ B00N28818A │ Amazon Prime Video                                                                       │ 1.4305555555555556 │    72 │
│ B007FTE2VW │ SimCity - Limited Edition                                                                │ 1.2794117647058822 │    68 │
│ 0439023513 │ Mockingjay (The Hunger Games)                                                            │ 2.6417910447761193 │    67 │
│ B00178630A │ Diablo III - PC/Mac                                                                      │           1.671875 │    64 │
│ B000OCEWGW │ Liquid Ass                                                                               │             4.8125 │    64 │
│ B005ZOBNOI │ The Fault in Our Stars                                                                   │  4.316666666666666 │    60 │
│ B00L9B7IKE │ The Girl on the Train: A Novel                                                           │ 2.0677966101694913 │    59 │
│ B007S6Y6VS │ Garden of Life Raw Organic Meal                                                          │ 2.8793103448275863 │    58 │
│ B0064X7B4A │ Words With Friends                                                                       │ 2.2413793103448274 │    58 │
│ B003WUYPPG │ Unbroken: A World War II Story of Survival, Resilience, and Redemption                   │  4.620689655172414 │    58 │
│ B00006HBUJ │ Star Wars: Episode II - Attack of the Clones (Widescreen Edition)                        │ 2.2982456140350878 │    57 │
│ B000XUBFE2 │ The Book Thief                                                                           │  4.526315789473684 │    57 │
│ B0006399FS │ How to Dismantle an Atomic Bomb                                                          │ 1.9821428571428572 │    56 │
│ B003ZSJ212 │ Star Wars: The Complete Saga (Episodes I-VI) (Packaging May Vary) [Blu-ray]              │  2.309090909090909 │    55 │
│ 193700788X │ Dead Ever After (Sookie Stackhouse/True Blood)                                           │ 1.5185185185185186 │    54 │
│ B004FYEZMQ │ Mass Effect 3                                                                            │  2.056603773584906 │    53 │
│ B000CFYAMC │ The Room                                                                                 │ 3.9615384615384617 │    52 │
│ B0031JK95S │ Garden of Life Raw Organic Meal                                                          │ 3.3137254901960786 │    51 │
│ B0012JY4G4 │ Color Oops Hair Color Remover Extra Strength 1 Each                                      │ 3.9019607843137254 │    51 │
│ B007VTVRFA │ SimCity - Limited Edition                                                                │ 1.2040816326530612 │    49 │
│ B00CE18P0K │ Pilot                                                                                    │ 1.7142857142857142 │    49 │
│ 0316015849 │ Twilight (The Twilight Saga, Book 1)                                                     │ 1.8979591836734695 │    49 │
│ B00DR0PDNE │ Google Chromecast HDMI Streaming Media Player                                            │ 2.5416666666666665 │    48 │
│ B000056OWC │ The First Years: 4-Stage Bath System                                                     │ 1.2127659574468086 │    47 │
│ B007IXWKUK │ Fifty Shades Darker (Fifty Shades, Book 2)                                               │ 1.6304347826086956 │    46 │
│ 1892112000 │ To Train Up a Child                                                                      │ 1.4130434782608696 │    46 │
│ 043935806X │ Harry Potter and the Order of the Phoenix (Book 5)                                       │  3.977272727272727 │    44 │
│ B00BGO0Q9O │ Fitbit Flex Wireless Wristband with Sleep Function, Black                                │ 1.9318181818181819 │    44 │
│ B003XF1XOQ │ Mockingjay (Hunger Games Trilogy, Book 3)                                                │  2.772727272727273 │    44 │
│ B00DD2B52Y │ Spring Breakers                                                                          │ 1.2093023255813953 │    43 │
│ B0064X7FVE │ The Weather Channel: Forecast, Radar & Alerts                                            │ 1.5116279069767442 │    43 │
│ B0083PWAPW │ Kindle Fire HD 7", Dolby Audio, Dual-Band Wi-Fi                                          │  2.627906976744186 │    43 │
│ B00192KCQ0 │ Death Magnetic                                                                           │ 3.5714285714285716 │    42 │
│ B007S6Y74O │ Garden of Life Raw Organic Meal                                                          │  3.292682926829268 │    41 │
│ B0052QYLUM │ Infant Optics DXR-5 Portable Video Baby Monitor                                          │ 2.1463414634146343 │    41 │
└────────────┴──────────────────────────────────────────────────────────────────────────────────────────┴────────────────────┴───────┘

50 rows in set. Elapsed: 60.052 sec. Processed 150.96 million rows, 68.93 GB (2.51 million rows/s., 1.15 GB/s.)
```

12. We can run the same query again, except this time we search for **awesome** in the reviews:

```sql
SELECT
    product_id,
    any(product_title),
    avg(star_rating),
    count() AS count
FROM amazon_reviews
WHERE position(review_body, 'awesome') > 0
GROUP BY product_id
ORDER BY count DESC
LIMIT 50;
```

It runs quite a bit faster - which means the cache is helping us out here:

```response

┌─product_id─┬─any(product_title)────────────────────────────────────────────────────┬───avg(star_rating)─┬─count─┐
│ B00992CF6W │ Minecraft                                                             │  4.848130353039482 │  4787 │
│ B009UX2YAC │ Subway Surfers                                                        │  4.866720955483171 │  3684 │
│ B00QW8TYWO │ Crossy Road                                                           │  4.935217903415784 │  2547 │
│ B00DJFIMW6 │ Minion Rush: Despicable Me Official Game                              │  4.850450450450451 │  2220 │
│ B00AREIAI8 │ My Horse                                                              │  4.865313653136531 │  2168 │
│ B00I8Q77Y0 │ Flappy Wings (not Flappy Bird)                                        │ 4.8246561886051085 │  2036 │
│ B0054JZC6E │ 101-in-1 Games                                                        │  4.792542016806722 │  1904 │
│ B00G5LQ5MU │ Escape The Titanic                                                    │  4.724673710379117 │  1609 │
│ B0086700CM │ Temple Run                                                            │   4.87636130685458 │  1561 │
│ B009HKL4B8 │ The Sims Freeplay                                                     │  4.763942931258106 │  1542 │
│ B00I6IKSZ0 │ Pixel Gun 3D (Pocket Edition) - multiplayer shooter with skin creator │  4.849894291754757 │  1419 │
│ B006OC2ANS │ BLOOD & GLORY                                                         │ 4.8561538461538465 │  1300 │
│ B00FATEJYE │ Injustice: Gods Among Us (Kindle Tablet Edition)                      │  4.789265982636149 │  1267 │
│ B00B2V66VS │ Temple Run 2                                                          │  4.764705882352941 │  1173 │
│ B00JOT3HQ2 │ Geometry Dash Lite                                                    │  4.909747292418772 │  1108 │
│ B00DUGCLY4 │ Guess The Emoji                                                       │  4.813606710158434 │  1073 │
│ B00DR0PDNE │ Google Chromecast HDMI Streaming Media Player                         │  4.607276119402985 │  1072 │
│ B00FAPF5U0 │ Candy Crush Saga                                                      │  4.825757575757576 │  1056 │
│ B0051VVOB2 │ Kindle Fire (Previous Generation - 1st)                               │  4.600407747196738 │   981 │
│ B007JPG04E │ FRONTLINE COMMANDO                                                    │             4.8125 │   912 │
│ B00PTB7B34 │ Call of Duty®: Heroes                                                 │  4.876404494382022 │   890 │
│ B00846GKTW │ Style Me Girl - Free 3D Fashion Dressup                               │  4.785714285714286 │   882 │
│ B004S8F7QM │ Cards Against Humanity                                                │  4.931034482758621 │   754 │
│ B00FAX6XQC │ DEER HUNTER CLASSIC                                                   │  4.700272479564033 │   734 │
│ B00PSGW79I │ Buddyman: Kick                                                        │  4.888736263736264 │   728 │
│ B00CTQ6SIG │ The Simpsons: Tapped Out                                              │  4.793948126801153 │   694 │
│ B008JK6W5K │ Logo Quiz                                                             │  4.782106782106782 │   693 │
│ B00EDTSKLU │ Geometry Dash                                                         │  4.942028985507246 │   690 │
│ B00CSR2J9I │ Hill Climb Racing                                                     │  4.880059970014993 │   667 │
│ B005ZXWMUS │ Netflix                                                               │  4.722306525037936 │   659 │
│ B00CRFAAYC │ Fab Tattoo Artist FREE                                                │  4.907435508345979 │   659 │
│ B00DHQHQCE │ Battle Beach                                                          │  4.863287250384024 │   651 │
│ B00BGA9WK2 │ PlayStation 4 500GB Console [Old Model]                               │  4.688751926040061 │   649 │
│ B008Y7SMQU │ Logo Quiz - Fun Plus Free                                             │             4.7888 │   625 │
│ B0083PWAPW │ Kindle Fire HD 7", Dolby Audio, Dual-Band Wi-Fi                       │  4.593900481540931 │   623 │
│ B008XG1X18 │ Pinterest                                                             │ 4.8148760330578515 │   605 │
│ B007SYWFRM │ Ice Age Village                                                       │ 4.8566666666666665 │   600 │
│ B00K7WGUKA │ Don't Tap The White Tile (Piano Tiles)                                │  4.922689075630252 │   595 │
│ B00BWYQ9YE │ Kindle Fire HDX 7", HDX Display (Previous Generation - 3rd)           │  4.649913344887349 │   577 │
│ B00IZLM8MY │ High School Story                                                     │  4.840425531914893 │   564 │
│ B004MC8CA2 │ Bible                                                                 │  4.884476534296029 │   554 │
│ B00KNWYDU8 │ Dragon City                                                           │  4.861111111111111 │   540 │
│ B009ZKSPDK │ Survivalcraft                                                         │  4.738317757009346 │   535 │
│ B00A4O6NMG │ My Singing Monsters                                                   │  4.845559845559846 │   518 │
│ B002MQYOFW │ The Hunger Games (Hunger Games Trilogy, Book 1)                       │  4.846899224806202 │   516 │
│ B005ZFOOE8 │ iHeartRadio – Free Music & Internet Radio                             │  4.837301587301587 │   504 │
│ B00AIUUXHC │ Hungry Shark Evolution                                                │  4.846311475409836 │   488 │
│ B00E8KLWB4 │ The Secret Society® - Hidden Mystery                                  │  4.669438669438669 │   481 │
│ B006D1ONE4 │ Where's My Water?                                                     │  4.916317991631799 │   478 │
│ B00G6ZTM3Y │ Terraria                                                              │  4.728421052631579 │   475 │
└────────────┴───────────────────────────────────────────────────────────────────────┴────────────────────┴───────┘

50 rows in set. Elapsed: 33.954 sec. Processed 150.96 million rows, 68.95 GB (4.45 million rows/s., 2.03 GB/s.)
```
