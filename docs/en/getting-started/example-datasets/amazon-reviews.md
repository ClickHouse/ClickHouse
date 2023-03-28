---
slug: /en/getting-started/example-datasets/amazon-reviews
sidebar_label: Amazon customer reviews
description:
---

# Amazon customer reviews dataset

**Amazon Customer Reviews** (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. This makes Amazon Customer Reviews a rich source of information for academic researchers in the fields of Natural Language Processing (NLP), Information Retrieval (IR), and Machine Learning (ML), amongst others.

The data is in a tab-separated format in gzipped files are up in AWS S3. Let's walk through the steps to insert it into ClickHouse.

:::note
The queries below were executed on a **Production** instance of [ClickHouse Cloud](https://clickhouse.cloud).
:::


1. Without inserting the data into ClickHouse, we can query it in place. Let's grab some rows so we can see what they look like:

```sql
SELECT * FROM s3('https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Wireless_v1_00.tsv.gz',
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
                        review_date Date')
                    LIMIT 10;
```

The rows look like:

```response
┌─marketplace─┬─customer_id─┬─review_id──────┬─product_id─┬─product_parent─┬─product_title──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─product_category─┬─star_rating─┬─helpful_votes─┬─total_votes─┬─vine──┬─verified_purchase─┬─review_headline───────────┬─review_body────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─review_date─┐
│ US          │    16414143 │ R3W4P9UBGNGH1U │ B00YL0EKWE │      852431543 │ LG G4 Case Hard Transparent Slim Clear Cover for LG G4                                                                                                                                                                                                     │ Wireless         │           2 │             1 │           3 │ false │ true              │ Looks good, functions meh │ 2 issues  -  Once I turned on the circle apps and installed this case,  my battery drained twice as fast as usual.  I ended up turning off the circle apps, which kind of makes the case just a case...  with a hole in it.  Second,  the wireless charging doesn't work.  I have a Motorola 360 watch and a Qi charging pad. The watch charges fine but this case doesn't. But hey, it looks nice. │  2015-08-31 │
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
    vine FixedString(1),
    verified_purchase FixedString(1),
    review_headline String,
    review_body String
)
ENGINE = MergeTree
ORDER BY (marketplace, review_date, product_category);
```

3. We are now ready to insert the data into ClickHouse. Before we do, check out the [list of files in the dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt) and decide which ones you want to include.

4. We will insert all of the US reviews - which is about 151M rows. The following `INSERT` command uses the `s3Cluster` table function, which allows the processing of mulitple S3 files in parallel using all the nodes of your cluster. We also use a wildcard to insert any file that starts with the name `https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_`:

```sql
INSERT INTO amazon_reviews
SELECT
   * REPLACE(vine = 'Y' AS vine, verified_purchase = 'Y' AS verified_purchase)
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
    );
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

6. Let's run some queries...
