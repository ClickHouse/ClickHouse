---
toc_priority: 20
toc_title: UK Property Price Paid
---

# UK Property Price Paid

The dataset contains data about prices paid for real-estate property in England and Wales. The data is available since year 1995.
The size of the dataset in uncompressed form is about 4 GiB and it will take about 226 MiB in ClickHouse.

Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
Description of the fields: https://www.gov.uk/guidance/about-the-price-paid-data

Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.

## Download the Dataset

```
wget http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv
```

Download will take about 2 minutes with good internet connection.

## Create the Table

```
CREATE TABLE uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String),
    category UInt8
) ENGINE = MergeTree ORDER BY (postcode1, postcode2, addr1, addr2);
```

## Preprocess and Import Data

We will use `clickhouse-local` tool for data preprocessing and `clickhouse-client` to upload it.

In this example, we define the structure of source data from the CSV file and specify a query to preprocess the data with `clickhouse-local`.

The preprocessing is:
- splitting the postcode to two different columns `postcode1` and `postcode2` that is better for storage and queries;
- coverting the `time` field to date as it only contains 00:00 time;
- ignoring the `uuid` field because we don't need it for analysis;
- transforming `type` and `duration` to more readable Enum fields with function `transform`;
- transforming `is_new` and `category` fields from single-character string (`Y`/`N` and `A`/`B`) to UInt8 field with 0 and 1.

Preprocessed data is piped directly to `clickhouse-client` to be inserted into ClickHouse table in streaming fashion.

```
clickhouse-local --input-format CSV --structure '
    uuid String,
    price UInt32,
    time DateTime,
    postcode String,
    a String,
    b String,
    c String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    d String,
    e String
' --query "
    WITH splitByChar(' ', postcode) AS p
    SELECT
        price,
        toDate(time) AS date,
        p[1] AS postcode1,
        p[2] AS postcode2,
        transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
        b = 'Y' AS is_new,
        transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
        addr1,
        addr2,
        street,
        locality,
        town,
        district,
        county,
        d = 'B' AS category
    FROM table" --date_time_input_format best_effort < pp-complete.csv | clickhouse-client --query "INSERT INTO uk_price_paid FORMAT TSV"
```

It will take about 40 seconds.

## Validate the Data

```
SELECT count() FROM uk_price_paid
26248711
```

The size of dataset in ClickHouse is just 226 MiB:

```
SELECT formatReadableSize(total_bytes) FROM system.tables WHERE name = 'uk_price_paid'
226.40 MiB
```

## Run Some Queries

### Average price per year:

```
SELECT toYear(date) AS year, round(avg(price)) AS price, bar(price, 0, 1000000, 80) FROM uk_price_paid GROUP BY year ORDER BY year

┌─year─┬──price─┬─bar(round(avg(price)), 0, 1000000, 80)─┐
│ 1995 │  67932 │ █████▍                                 │
│ 1996 │  71505 │ █████▋                                 │
│ 1997 │  78532 │ ██████▎                                │
│ 1998 │  85435 │ ██████▋                                │
│ 1999 │  96036 │ ███████▋                               │
│ 2000 │ 107478 │ ████████▌                              │
│ 2001 │ 118886 │ █████████▌                             │
│ 2002 │ 137940 │ ███████████                            │
│ 2003 │ 155888 │ ████████████▍                          │
│ 2004 │ 178885 │ ██████████████▎                        │
│ 2005 │ 189350 │ ███████████████▏                       │
│ 2006 │ 203528 │ ████████████████▎                      │
│ 2007 │ 219377 │ █████████████████▌                     │
│ 2008 │ 217056 │ █████████████████▎                     │
│ 2009 │ 213419 │ █████████████████                      │
│ 2010 │ 236110 │ ██████████████████▊                    │
│ 2011 │ 232804 │ ██████████████████▌                    │
│ 2012 │ 238366 │ ███████████████████                    │
│ 2013 │ 256931 │ ████████████████████▌                  │
│ 2014 │ 279917 │ ██████████████████████▍                │
│ 2015 │ 297264 │ ███████████████████████▋               │
│ 2016 │ 313197 │ █████████████████████████              │
│ 2017 │ 346070 │ ███████████████████████████▋           │
│ 2018 │ 350117 │ ████████████████████████████           │
│ 2019 │ 351010 │ ████████████████████████████           │
│ 2020 │ 368974 │ █████████████████████████████▌         │
│ 2021 │ 384351 │ ██████████████████████████████▋        │
└──────┴────────┴────────────────────────────────────────┘

27 rows in set. Elapsed: 0.027 sec. Processed 26.25 million rows, 157.49 MB (955.96 million rows/s., 5.74 GB/s.)
```

### Average price per year in London:

```
SELECT toYear(date) AS year, round(avg(price)) AS price, bar(price, 0, 2000000, 100) FROM uk_price_paid WHERE town = 'LONDON' GROUP BY year ORDER BY year

┌─year─┬───price─┬─bar(round(avg(price)), 0, 2000000, 100)───────────────┐
│ 1995 │  109112 │ █████▍                                                │
│ 1996 │  118667 │ █████▊                                                │
│ 1997 │  136518 │ ██████▋                                               │
│ 1998 │  152983 │ ███████▋                                              │
│ 1999 │  180633 │ █████████                                             │
│ 2000 │  215830 │ ██████████▋                                           │
│ 2001 │  232996 │ ███████████▋                                          │
│ 2002 │  263672 │ █████████████▏                                        │
│ 2003 │  278394 │ █████████████▊                                        │
│ 2004 │  304665 │ ███████████████▏                                      │
│ 2005 │  322875 │ ████████████████▏                                     │
│ 2006 │  356192 │ █████████████████▋                                    │
│ 2007 │  404055 │ ████████████████████▏                                 │
│ 2008 │  420741 │ █████████████████████                                 │
│ 2009 │  427754 │ █████████████████████▍                                │
│ 2010 │  480306 │ ████████████████████████                              │
│ 2011 │  496274 │ ████████████████████████▋                             │
│ 2012 │  519441 │ █████████████████████████▊                            │
│ 2013 │  616209 │ ██████████████████████████████▋                       │
│ 2014 │  724144 │ ████████████████████████████████████▏                 │
│ 2015 │  792112 │ ███████████████████████████████████████▌              │
│ 2016 │  843568 │ ██████████████████████████████████████████▏           │
│ 2017 │  982566 │ █████████████████████████████████████████████████▏    │
│ 2018 │ 1016845 │ ██████████████████████████████████████████████████▋   │
│ 2019 │ 1043277 │ ████████████████████████████████████████████████████▏ │
│ 2020 │ 1003963 │ ██████████████████████████████████████████████████▏   │
│ 2021 │  940794 │ ███████████████████████████████████████████████       │
└──────┴─────────┴───────────────────────────────────────────────────────┘

27 rows in set. Elapsed: 0.024 sec. Processed 26.25 million rows, 76.88 MB (1.08 billion rows/s., 3.15 GB/s.)
```

Something happened in 2013. I don't have a clue. Maybe you have a clue what happened in 2020?

### The most expensive neighborhoods:

```
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM uk_price_paid
WHERE date >= '2020-01-01'
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 100

┌─town─────────────────┬─district───────────────┬────c─┬───price─┬─bar(round(avg(price)), 0, 5000000, 100)────────────────────────────┐
│ LONDON               │ CITY OF WESTMINSTER    │ 3372 │ 3305225 │ ██████████████████████████████████████████████████████████████████ │
│ LONDON               │ CITY OF LONDON         │  257 │ 3294478 │ █████████████████████████████████████████████████████████████████▊ │
│ LONDON               │ KENSINGTON AND CHELSEA │ 2367 │ 2342422 │ ██████████████████████████████████████████████▋                    │
│ LEATHERHEAD          │ ELMBRIDGE              │  108 │ 1927143 │ ██████████████████████████████████████▌                            │
│ VIRGINIA WATER       │ RUNNYMEDE              │  142 │ 1868819 │ █████████████████████████████████████▍                             │
│ LONDON               │ CAMDEN                 │ 2815 │ 1736788 │ ██████████████████████████████████▋                                │
│ THORNTON HEATH       │ CROYDON                │  521 │ 1733051 │ ██████████████████████████████████▋                                │
│ WINDLESHAM           │ SURREY HEATH           │  103 │ 1717255 │ ██████████████████████████████████▎                                │
│ BARNET               │ ENFIELD                │  115 │ 1503458 │ ██████████████████████████████                                     │
│ OXFORD               │ SOUTH OXFORDSHIRE      │  298 │ 1275200 │ █████████████████████████▌                                         │
│ LONDON               │ ISLINGTON              │ 2458 │ 1274308 │ █████████████████████████▍                                         │
│ COBHAM               │ ELMBRIDGE              │  364 │ 1260005 │ █████████████████████████▏                                         │
│ LONDON               │ HOUNSLOW               │  618 │ 1215682 │ ████████████████████████▎                                          │
│ ASCOT                │ WINDSOR AND MAIDENHEAD │  379 │ 1215146 │ ████████████████████████▎                                          │
│ LONDON               │ RICHMOND UPON THAMES   │  654 │ 1207551 │ ████████████████████████▏                                          │
│ BEACONSFIELD         │ BUCKINGHAMSHIRE        │  307 │ 1186220 │ ███████████████████████▋                                           │
│ RICHMOND             │ RICHMOND UPON THAMES   │  805 │ 1100420 │ ██████████████████████                                             │
│ LONDON               │ HAMMERSMITH AND FULHAM │ 2888 │ 1062959 │ █████████████████████▎                                             │
│ WEYBRIDGE            │ ELMBRIDGE              │  607 │ 1027161 │ ████████████████████▌                                              │
│ RADLETT              │ HERTSMERE              │  265 │ 1015896 │ ████████████████████▎                                              │
│ SALCOMBE             │ SOUTH HAMS             │  124 │ 1014393 │ ████████████████████▎                                              │
│ BURFORD              │ WEST OXFORDSHIRE       │  102 │  993100 │ ███████████████████▋                                               │
│ ESHER                │ ELMBRIDGE              │  454 │  969770 │ ███████████████████▍                                               │
│ HINDHEAD             │ WAVERLEY               │  128 │  967786 │ ███████████████████▎                                               │
│ BROCKENHURST         │ NEW FOREST             │  121 │  967046 │ ███████████████████▎                                               │
│ LEATHERHEAD          │ GUILDFORD              │  191 │  964489 │ ███████████████████▎                                               │
│ GERRARDS CROSS       │ BUCKINGHAMSHIRE        │  376 │  958555 │ ███████████████████▏                                               │
│ EAST MOLESEY         │ ELMBRIDGE              │  181 │  943457 │ ██████████████████▋                                                │
│ OLNEY                │ MILTON KEYNES          │  220 │  942892 │ ██████████████████▋                                                │
│ CHALFONT ST GILES    │ BUCKINGHAMSHIRE        │  135 │  926950 │ ██████████████████▌                                                │
│ HENLEY-ON-THAMES     │ SOUTH OXFORDSHIRE      │  509 │  905732 │ ██████████████████                                                 │
│ KINGSTON UPON THAMES │ KINGSTON UPON THAMES   │  889 │  899689 │ █████████████████▊                                                 │
│ BELVEDERE            │ BEXLEY                 │  313 │  895336 │ █████████████████▊                                                 │
│ CRANBROOK            │ TUNBRIDGE WELLS        │  404 │  888190 │ █████████████████▋                                                 │
│ LONDON               │ EALING                 │ 2460 │  865893 │ █████████████████▎                                                 │
│ MAIDENHEAD           │ BUCKINGHAMSHIRE        │  114 │  863814 │ █████████████████▎                                                 │
│ LONDON               │ MERTON                 │ 1958 │  857192 │ █████████████████▏                                                 │
│ GUILDFORD            │ WAVERLEY               │  131 │  854447 │ █████████████████                                                  │
│ LONDON               │ HACKNEY                │ 3088 │  846571 │ ████████████████▊                                                  │
│ LYMM                 │ WARRINGTON             │  285 │  839920 │ ████████████████▋                                                  │
│ HARPENDEN            │ ST ALBANS              │  606 │  836994 │ ████████████████▋                                                  │
│ LONDON               │ WANDSWORTH             │ 6113 │  832292 │ ████████████████▋                                                  │
│ LONDON               │ SOUTHWARK              │ 3612 │  831319 │ ████████████████▋                                                  │
│ BERKHAMSTED          │ DACORUM                │  502 │  830356 │ ████████████████▌                                                  │
│ KINGS LANGLEY        │ DACORUM                │  137 │  821358 │ ████████████████▍                                                  │
│ TONBRIDGE            │ TUNBRIDGE WELLS        │  339 │  806736 │ ████████████████▏                                                  │
│ EPSOM                │ REIGATE AND BANSTEAD   │  157 │  805903 │ ████████████████                                                   │
│ WOKING               │ GUILDFORD              │  161 │  803283 │ ████████████████                                                   │
│ STOCKBRIDGE          │ TEST VALLEY            │  168 │  801973 │ ████████████████                                                   │
│ TEDDINGTON           │ RICHMOND UPON THAMES   │  539 │  798591 │ ███████████████▊                                                   │
│ OXFORD               │ VALE OF WHITE HORSE    │  329 │  792907 │ ███████████████▋                                                   │
│ LONDON               │ BARNET                 │ 3624 │  789583 │ ███████████████▋                                                   │
│ TWICKENHAM           │ RICHMOND UPON THAMES   │ 1090 │  787760 │ ███████████████▋                                                   │
│ LUTON                │ CENTRAL BEDFORDSHIRE   │  196 │  786051 │ ███████████████▋                                                   │
│ TONBRIDGE            │ MAIDSTONE              │  277 │  785746 │ ███████████████▋                                                   │
│ TOWCESTER            │ WEST NORTHAMPTONSHIRE  │  186 │  783532 │ ███████████████▋                                                   │
│ LONDON               │ LAMBETH                │ 4832 │  783422 │ ███████████████▋                                                   │
│ LUTTERWORTH          │ HARBOROUGH             │  515 │  781775 │ ███████████████▋                                                   │
│ WOODSTOCK            │ WEST OXFORDSHIRE       │  135 │  777499 │ ███████████████▌                                                   │
│ ALRESFORD            │ WINCHESTER             │  196 │  775577 │ ███████████████▌                                                   │
│ LONDON               │ NEWHAM                 │ 2942 │  768551 │ ███████████████▎                                                   │
│ ALDERLEY EDGE        │ CHESHIRE EAST          │  168 │  768280 │ ███████████████▎                                                   │
│ MARLOW               │ BUCKINGHAMSHIRE        │  301 │  762784 │ ███████████████▎                                                   │
│ BILLINGSHURST        │ CHICHESTER             │  134 │  760920 │ ███████████████▏                                                   │
│ LONDON               │ TOWER HAMLETS          │ 4183 │  759635 │ ███████████████▏                                                   │
│ MIDHURST             │ CHICHESTER             │  245 │  759101 │ ███████████████▏                                                   │
│ THAMES DITTON        │ ELMBRIDGE              │  227 │  753347 │ ███████████████                                                    │
│ POTTERS BAR          │ WELWYN HATFIELD        │  163 │  752926 │ ███████████████                                                    │
│ REIGATE              │ REIGATE AND BANSTEAD   │  555 │  740961 │ ██████████████▋                                                    │
│ TADWORTH             │ REIGATE AND BANSTEAD   │  477 │  738997 │ ██████████████▋                                                    │
│ SEVENOAKS            │ SEVENOAKS              │ 1074 │  734658 │ ██████████████▋                                                    │
│ PETWORTH             │ CHICHESTER             │  138 │  732432 │ ██████████████▋                                                    │
│ BOURNE END           │ BUCKINGHAMSHIRE        │  127 │  730742 │ ██████████████▌                                                    │
│ PURLEY               │ CROYDON                │  540 │  727721 │ ██████████████▌                                                    │
│ OXTED                │ TANDRIDGE              │  320 │  726078 │ ██████████████▌                                                    │
│ LONDON               │ HARINGEY               │ 2988 │  724573 │ ██████████████▍                                                    │
│ BANSTEAD             │ REIGATE AND BANSTEAD   │  373 │  713834 │ ██████████████▎                                                    │
│ PINNER               │ HARROW                 │  480 │  712166 │ ██████████████▏                                                    │
│ MALMESBURY           │ WILTSHIRE              │  293 │  707747 │ ██████████████▏                                                    │
│ RICKMANSWORTH        │ THREE RIVERS           │  732 │  705400 │ ██████████████                                                     │
│ SLOUGH               │ BUCKINGHAMSHIRE        │  359 │  705002 │ ██████████████                                                     │
│ GREAT MISSENDEN      │ BUCKINGHAMSHIRE        │  214 │  704904 │ ██████████████                                                     │
│ READING              │ SOUTH OXFORDSHIRE      │  295 │  701697 │ ██████████████                                                     │
│ HYTHE                │ FOLKESTONE AND HYTHE   │  457 │  700334 │ ██████████████                                                     │
│ WELWYN               │ WELWYN HATFIELD        │  217 │  699649 │ █████████████▊                                                     │
│ CHIGWELL             │ EPPING FOREST          │  242 │  697869 │ █████████████▊                                                     │
│ BARNET               │ BARNET                 │  906 │  695680 │ █████████████▊                                                     │
│ HASLEMERE            │ CHICHESTER             │  120 │  694028 │ █████████████▊                                                     │
│ LEATHERHEAD          │ MOLE VALLEY            │  748 │  692026 │ █████████████▋                                                     │
│ LONDON               │ BRENT                  │ 1945 │  690799 │ █████████████▋                                                     │
│ HASLEMERE            │ WAVERLEY               │  258 │  690765 │ █████████████▋                                                     │
│ NORTHWOOD            │ HILLINGDON             │  252 │  690753 │ █████████████▋                                                     │
│ WALTON-ON-THAMES     │ ELMBRIDGE              │  871 │  689431 │ █████████████▋                                                     │
│ INGATESTONE          │ BRENTWOOD              │  150 │  688345 │ █████████████▋                                                     │
│ OXFORD               │ OXFORD                 │ 1761 │  686114 │ █████████████▋                                                     │
│ CHISLEHURST          │ BROMLEY                │  410 │  682892 │ █████████████▋                                                     │
│ KINGS LANGLEY        │ THREE RIVERS           │  109 │  682320 │ █████████████▋                                                     │
│ ASHTEAD              │ MOLE VALLEY            │  280 │  680483 │ █████████████▌                                                     │
│ WOKING               │ SURREY HEATH           │  269 │  679035 │ █████████████▌                                                     │
│ ASCOT                │ BRACKNELL FOREST       │  160 │  678632 │ █████████████▌                                                     │
└──────────────────────┴────────────────────────┴──────┴─────────┴────────────────────────────────────────────────────────────────────┘

100 rows in set. Elapsed: 0.039 sec. Processed 26.25 million rows, 278.03 MB (674.32 million rows/s., 7.14 GB/s.)
```

### Test it in Playground

The data is uploaded to ClickHouse Playground, [example](https://gh-api.clickhouse.tech/play?user=play#U0VMRUNUIHRvd24sIGRpc3RyaWN0LCBjb3VudCgpIEFTIGMsIHJvdW5kKGF2ZyhwcmljZSkpIEFTIHByaWNlLCBiYXIocHJpY2UsIDAsIDUwMDAwMDAsIDEwMCkgRlJPTSB1a19wcmljZV9wYWlkIFdIRVJFIGRhdGUgPj0gJzIwMjAtMDEtMDEnIEdST1VQIEJZIHRvd24sIGRpc3RyaWN0IEhBVklORyBjID49IDEwMCBPUkRFUiBCWSBwcmljZSBERVNDIExJTUlUIDEwMA==).

## Let's speed up queries using projections

[Projections](https://../../sql-reference/statements/alter/projection/) allow to improve queries speed by storing pre-aggregated data.

### Build a projection 

```
-- create an aggregate projection by dimensions (toYear(date), district, town)

ALTER TABLE uk_price_paid
    ADD PROJECTION projection_by_year_district_town
    (
        SELECT
            toYear(date),
            district,
            town,
            avg(price),
            sum(price),
            count()
        GROUP BY
            toYear(date),
            district,
            town
    );

-- populate the projection for existing data (without it projection will be 
--  created for only newly inserted data)

ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION projection_by_year_district_town
SETTINGS mutations_sync = 1;
```

## Test performance

Let's run the same 3 queries.

```
-- enable projections for selects
set allow_experimental_projection_optimization=1;

-- Q1) Average price per year:

SELECT
    toYear(date) AS year,
    round(avg(price)) AS price,
    bar(price, 0, 1000000, 80)
FROM uk_price_paid
GROUP BY year
ORDER BY year ASC;

┌─year─┬──price─┬─bar(round(avg(price)), 0, 1000000, 80)─┐
│ 1995 │  67932 │ █████▍                                 │
│ 1996 │  71505 │ █████▋                                 │
│ 1997 │  78532 │ ██████▎                                │
│ 1998 │  85435 │ ██████▋                                │
│ 1999 │  96036 │ ███████▋                               │
│ 2000 │ 107478 │ ████████▌                              │
│ 2001 │ 118886 │ █████████▌                             │
│ 2002 │ 137940 │ ███████████                            │
│ 2003 │ 155888 │ ████████████▍                          │
│ 2004 │ 178885 │ ██████████████▎                        │
│ 2005 │ 189350 │ ███████████████▏                       │
│ 2006 │ 203528 │ ████████████████▎                      │
│ 2007 │ 219377 │ █████████████████▌                     │
│ 2008 │ 217056 │ █████████████████▎                     │
│ 2009 │ 213419 │ █████████████████                      │
│ 2010 │ 236110 │ ██████████████████▊                    │
│ 2011 │ 232804 │ ██████████████████▌                    │
│ 2012 │ 238366 │ ███████████████████                    │
│ 2013 │ 256931 │ ████████████████████▌                  │
│ 2014 │ 279917 │ ██████████████████████▍                │
│ 2015 │ 297264 │ ███████████████████████▋               │
│ 2016 │ 313197 │ █████████████████████████              │
│ 2017 │ 346070 │ ███████████████████████████▋           │
│ 2018 │ 350117 │ ████████████████████████████           │
│ 2019 │ 351010 │ ████████████████████████████           │
│ 2020 │ 368974 │ █████████████████████████████▌         │
│ 2021 │ 384351 │ ██████████████████████████████▋        │
└──────┴────────┴────────────────────────────────────────┘

27 rows in set. Elapsed: 0.003 sec. Processed 106.87 thousand rows, 3.21 MB (31.92 million rows/s., 959.03 MB/s.)

-- Q2) Average price per year in London:

SELECT
    toYear(date) AS year,
    round(avg(price)) AS price,
    bar(price, 0, 2000000, 100)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY year
ORDER BY year ASC;

┌─year─┬───price─┬─bar(round(avg(price)), 0, 2000000, 100)───────────────┐
│ 1995 │  109112 │ █████▍                                                │
│ 1996 │  118667 │ █████▊                                                │
│ 1997 │  136518 │ ██████▋                                               │
│ 1998 │  152983 │ ███████▋                                              │
│ 1999 │  180633 │ █████████                                             │
│ 2000 │  215830 │ ██████████▋                                           │
│ 2001 │  232996 │ ███████████▋                                          │
│ 2002 │  263672 │ █████████████▏                                        │
│ 2003 │  278394 │ █████████████▊                                        │
│ 2004 │  304665 │ ███████████████▏                                      │
│ 2005 │  322875 │ ████████████████▏                                     │
│ 2006 │  356192 │ █████████████████▋                                    │
│ 2007 │  404055 │ ████████████████████▏                                 │
│ 2008 │  420741 │ █████████████████████                                 │
│ 2009 │  427754 │ █████████████████████▍                                │
│ 2010 │  480306 │ ████████████████████████                              │
│ 2011 │  496274 │ ████████████████████████▋                             │
│ 2012 │  519441 │ █████████████████████████▊                            │
│ 2013 │  616209 │ ██████████████████████████████▋                       │
│ 2014 │  724144 │ ████████████████████████████████████▏                 │
│ 2015 │  792112 │ ███████████████████████████████████████▌              │
│ 2016 │  843568 │ ██████████████████████████████████████████▏           │
│ 2017 │  982566 │ █████████████████████████████████████████████████▏    │
│ 2018 │ 1016845 │ ██████████████████████████████████████████████████▋   │
│ 2019 │ 1043277 │ ████████████████████████████████████████████████████▏ │
│ 2020 │ 1003963 │ ██████████████████████████████████████████████████▏   │
│ 2021 │  940794 │ ███████████████████████████████████████████████       │
└──────┴─────────┴───────────────────────────────────────────────────────┘

27 rows in set. Elapsed: 0.005 sec. Processed 106.87 thousand rows, 3.53 MB (23.49 million rows/s., 775.95 MB/s.)

-- Q3) The most expensive neighborhoods:
-- the condition (date >= '2020-01-01') needs to be modified to match projection dimension (toYear(date) >= 2020)

SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM uk_price_paid
WHERE toYear(date) >= 2020
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 100

┌─town─────────────────┬─district───────────────┬────c─┬───price─┬─bar(round(avg(price)), 0, 5000000, 100)────────────────────────────┐
│ LONDON               │ CITY OF WESTMINSTER    │ 3372 │ 3305225 │ ██████████████████████████████████████████████████████████████████ │
│ LONDON               │ CITY OF LONDON         │  257 │ 3294478 │ █████████████████████████████████████████████████████████████████▊ │
│ LONDON               │ KENSINGTON AND CHELSEA │ 2367 │ 2342422 │ ██████████████████████████████████████████████▋                    │
│ LEATHERHEAD          │ ELMBRIDGE              │  108 │ 1927143 │ ██████████████████████████████████████▌                            │
│ VIRGINIA WATER       │ RUNNYMEDE              │  142 │ 1868819 │ █████████████████████████████████████▍                             │
│ LONDON               │ CAMDEN                 │ 2815 │ 1736788 │ ██████████████████████████████████▋                                │
│ THORNTON HEATH       │ CROYDON                │  521 │ 1733051 │ ██████████████████████████████████▋                                │
│ WINDLESHAM           │ SURREY HEATH           │  103 │ 1717255 │ ██████████████████████████████████▎                                │
│ BARNET               │ ENFIELD                │  115 │ 1503458 │ ██████████████████████████████                                     │
│ OXFORD               │ SOUTH OXFORDSHIRE      │  298 │ 1275200 │ █████████████████████████▌                                         │
│ LONDON               │ ISLINGTON              │ 2458 │ 1274308 │ █████████████████████████▍                                         │
│ COBHAM               │ ELMBRIDGE              │  364 │ 1260005 │ █████████████████████████▏                                         │
│ LONDON               │ HOUNSLOW               │  618 │ 1215682 │ ████████████████████████▎                                          │
│ ASCOT                │ WINDSOR AND MAIDENHEAD │  379 │ 1215146 │ ████████████████████████▎                                          │
│ LONDON               │ RICHMOND UPON THAMES   │  654 │ 1207551 │ ████████████████████████▏                                          │
│ BEACONSFIELD         │ BUCKINGHAMSHIRE        │  307 │ 1186220 │ ███████████████████████▋                                           │
│ RICHMOND             │ RICHMOND UPON THAMES   │  805 │ 1100420 │ ██████████████████████                                             │
│ LONDON               │ HAMMERSMITH AND FULHAM │ 2888 │ 1062959 │ █████████████████████▎                                             │
│ WEYBRIDGE            │ ELMBRIDGE              │  607 │ 1027161 │ ████████████████████▌                                              │
│ RADLETT              │ HERTSMERE              │  265 │ 1015896 │ ████████████████████▎                                              │
│ SALCOMBE             │ SOUTH HAMS             │  124 │ 1014393 │ ████████████████████▎                                              │
│ BURFORD              │ WEST OXFORDSHIRE       │  102 │  993100 │ ███████████████████▋                                               │
│ ESHER                │ ELMBRIDGE              │  454 │  969770 │ ███████████████████▍                                               │
│ HINDHEAD             │ WAVERLEY               │  128 │  967786 │ ███████████████████▎                                               │
│ BROCKENHURST         │ NEW FOREST             │  121 │  967046 │ ███████████████████▎                                               │
│ LEATHERHEAD          │ GUILDFORD              │  191 │  964489 │ ███████████████████▎                                               │
│ GERRARDS CROSS       │ BUCKINGHAMSHIRE        │  376 │  958555 │ ███████████████████▏                                               │
│ EAST MOLESEY         │ ELMBRIDGE              │  181 │  943457 │ ██████████████████▋                                                │
│ OLNEY                │ MILTON KEYNES          │  220 │  942892 │ ██████████████████▋                                                │
│ CHALFONT ST GILES    │ BUCKINGHAMSHIRE        │  135 │  926950 │ ██████████████████▌                                                │
│ HENLEY-ON-THAMES     │ SOUTH OXFORDSHIRE      │  509 │  905732 │ ██████████████████                                                 │
│ KINGSTON UPON THAMES │ KINGSTON UPON THAMES   │  889 │  899689 │ █████████████████▊                                                 │
│ BELVEDERE            │ BEXLEY                 │  313 │  895336 │ █████████████████▊                                                 │
│ CRANBROOK            │ TUNBRIDGE WELLS        │  404 │  888190 │ █████████████████▋                                                 │
│ LONDON               │ EALING                 │ 2460 │  865893 │ █████████████████▎                                                 │
│ MAIDENHEAD           │ BUCKINGHAMSHIRE        │  114 │  863814 │ █████████████████▎                                                 │
│ LONDON               │ MERTON                 │ 1958 │  857192 │ █████████████████▏                                                 │
│ GUILDFORD            │ WAVERLEY               │  131 │  854447 │ █████████████████                                                  │
│ LONDON               │ HACKNEY                │ 3088 │  846571 │ ████████████████▊                                                  │
│ LYMM                 │ WARRINGTON             │  285 │  839920 │ ████████████████▋                                                  │
│ HARPENDEN            │ ST ALBANS              │  606 │  836994 │ ████████████████▋                                                  │
│ LONDON               │ WANDSWORTH             │ 6113 │  832292 │ ████████████████▋                                                  │
│ LONDON               │ SOUTHWARK              │ 3612 │  831319 │ ████████████████▋                                                  │
│ BERKHAMSTED          │ DACORUM                │  502 │  830356 │ ████████████████▌                                                  │
│ KINGS LANGLEY        │ DACORUM                │  137 │  821358 │ ████████████████▍                                                  │
│ TONBRIDGE            │ TUNBRIDGE WELLS        │  339 │  806736 │ ████████████████▏                                                  │
│ EPSOM                │ REIGATE AND BANSTEAD   │  157 │  805903 │ ████████████████                                                   │
│ WOKING               │ GUILDFORD              │  161 │  803283 │ ████████████████                                                   │
│ STOCKBRIDGE          │ TEST VALLEY            │  168 │  801973 │ ████████████████                                                   │
│ TEDDINGTON           │ RICHMOND UPON THAMES   │  539 │  798591 │ ███████████████▊                                                   │
│ OXFORD               │ VALE OF WHITE HORSE    │  329 │  792907 │ ███████████████▋                                                   │
│ LONDON               │ BARNET                 │ 3624 │  789583 │ ███████████████▋                                                   │
│ TWICKENHAM           │ RICHMOND UPON THAMES   │ 1090 │  787760 │ ███████████████▋                                                   │
│ LUTON                │ CENTRAL BEDFORDSHIRE   │  196 │  786051 │ ███████████████▋                                                   │
│ TONBRIDGE            │ MAIDSTONE              │  277 │  785746 │ ███████████████▋                                                   │
│ TOWCESTER            │ WEST NORTHAMPTONSHIRE  │  186 │  783532 │ ███████████████▋                                                   │
│ LONDON               │ LAMBETH                │ 4832 │  783422 │ ███████████████▋                                                   │
│ LUTTERWORTH          │ HARBOROUGH             │  515 │  781775 │ ███████████████▋                                                   │
│ WOODSTOCK            │ WEST OXFORDSHIRE       │  135 │  777499 │ ███████████████▌                                                   │
│ ALRESFORD            │ WINCHESTER             │  196 │  775577 │ ███████████████▌                                                   │
│ LONDON               │ NEWHAM                 │ 2942 │  768551 │ ███████████████▎                                                   │
│ ALDERLEY EDGE        │ CHESHIRE EAST          │  168 │  768280 │ ███████████████▎                                                   │
│ MARLOW               │ BUCKINGHAMSHIRE        │  301 │  762784 │ ███████████████▎                                                   │
│ BILLINGSHURST        │ CHICHESTER             │  134 │  760920 │ ███████████████▏                                                   │
│ LONDON               │ TOWER HAMLETS          │ 4183 │  759635 │ ███████████████▏                                                   │
│ MIDHURST             │ CHICHESTER             │  245 │  759101 │ ███████████████▏                                                   │
│ THAMES DITTON        │ ELMBRIDGE              │  227 │  753347 │ ███████████████                                                    │
│ POTTERS BAR          │ WELWYN HATFIELD        │  163 │  752926 │ ███████████████                                                    │
│ REIGATE              │ REIGATE AND BANSTEAD   │  555 │  740961 │ ██████████████▋                                                    │
│ TADWORTH             │ REIGATE AND BANSTEAD   │  477 │  738997 │ ██████████████▋                                                    │
│ SEVENOAKS            │ SEVENOAKS              │ 1074 │  734658 │ ██████████████▋                                                    │
│ PETWORTH             │ CHICHESTER             │  138 │  732432 │ ██████████████▋                                                    │
│ BOURNE END           │ BUCKINGHAMSHIRE        │  127 │  730742 │ ██████████████▌                                                    │
│ PURLEY               │ CROYDON                │  540 │  727721 │ ██████████████▌                                                    │
│ OXTED                │ TANDRIDGE              │  320 │  726078 │ ██████████████▌                                                    │
│ LONDON               │ HARINGEY               │ 2988 │  724573 │ ██████████████▍                                                    │
│ BANSTEAD             │ REIGATE AND BANSTEAD   │  373 │  713834 │ ██████████████▎                                                    │
│ PINNER               │ HARROW                 │  480 │  712166 │ ██████████████▏                                                    │
│ MALMESBURY           │ WILTSHIRE              │  293 │  707747 │ ██████████████▏                                                    │
│ RICKMANSWORTH        │ THREE RIVERS           │  732 │  705400 │ ██████████████                                                     │
│ SLOUGH               │ BUCKINGHAMSHIRE        │  359 │  705002 │ ██████████████                                                     │
│ GREAT MISSENDEN      │ BUCKINGHAMSHIRE        │  214 │  704904 │ ██████████████                                                     │
│ READING              │ SOUTH OXFORDSHIRE      │  295 │  701697 │ ██████████████                                                     │
│ HYTHE                │ FOLKESTONE AND HYTHE   │  457 │  700334 │ ██████████████                                                     │
│ WELWYN               │ WELWYN HATFIELD        │  217 │  699649 │ █████████████▊                                                     │
│ CHIGWELL             │ EPPING FOREST          │  242 │  697869 │ █████████████▊                                                     │
│ BARNET               │ BARNET                 │  906 │  695680 │ █████████████▊                                                     │
│ HASLEMERE            │ CHICHESTER             │  120 │  694028 │ █████████████▊                                                     │
│ LEATHERHEAD          │ MOLE VALLEY            │  748 │  692026 │ █████████████▋                                                     │
│ LONDON               │ BRENT                  │ 1945 │  690799 │ █████████████▋                                                     │
│ HASLEMERE            │ WAVERLEY               │  258 │  690765 │ █████████████▋                                                     │
│ NORTHWOOD            │ HILLINGDON             │  252 │  690753 │ █████████████▋                                                     │
│ WALTON-ON-THAMES     │ ELMBRIDGE              │  871 │  689431 │ █████████████▋                                                     │
│ INGATESTONE          │ BRENTWOOD              │  150 │  688345 │ █████████████▋                                                     │
│ OXFORD               │ OXFORD                 │ 1761 │  686114 │ █████████████▋                                                     │
│ CHISLEHURST          │ BROMLEY                │  410 │  682892 │ █████████████▋                                                     │
│ KINGS LANGLEY        │ THREE RIVERS           │  109 │  682320 │ █████████████▋                                                     │
│ ASHTEAD              │ MOLE VALLEY            │  280 │  680483 │ █████████████▌                                                     │
│ WOKING               │ SURREY HEATH           │  269 │  679035 │ █████████████▌                                                     │
│ ASCOT                │ BRACKNELL FOREST       │  160 │  678632 │ █████████████▌                                                     │
└──────────────────────┴────────────────────────┴──────┴─────────┴────────────────────────────────────────────────────────────────────┘

100 rows in set. Elapsed: 0.005 sec. Processed 12.85 thousand rows, 813.40 KB (2.73 million rows/s., 172.95 MB/s.)
```

All 3 queries work much faster and read fewer rows.

```
Q1) 
no projection: 27 rows in set. Elapsed: 0.027 sec. Processed 26.25 million rows, 157.49 MB (955.96 million rows/s., 5.74 GB/s.)
   projection: 27 rows in set. Elapsed: 0.003 sec. Processed 106.87 thousand rows, 3.21 MB (31.92 million rows/s., 959.03 MB/s.)
```
