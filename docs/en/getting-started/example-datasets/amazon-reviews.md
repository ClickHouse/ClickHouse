---
slug: /en/getting-started/example-datasets/amazon-reviews
sidebar_label: Amazon customer reviews
description:
---

# Amazon customer reviews dataset

**Amazon Customer Reviews** (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. This makes Amazon Customer Reviews a rich source of information for academic researchers in the fields of Natural Language Processing (NLP), Information Retrieval (IR), and Machine Learning (ML), amongst others.

The data is in a JSON format, and the files are up in AWS S3. Let's walk through the steps to insert it into ClickHouse.

:::note
The queries below were executed on a **Production** instance of [ClickHouse Cloud](https://clickhouse.cloud).
:::

1. Let's check out the format of the data in the files. Notice the `s3Cluster` table function returns a table where the data types are inferred - helpful when you are researching a new dataset:

```sql

```


```response

```

2. Let's define a new table named `amazon_reviews`. We'll optimize some of the inferred column data types - and choose a primary key (the `ORDER BY` clause):

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
    vine UInt8,
    verified_purchase UInt8,
    review_headline String,
    review_body String
)
ENGINE = MergeTree
ORDER BY (marketplace, review_date, product_category);
```

3. The following `INSERT` query inserts all of the files in our S3 bucket into `amazon_reviews`. If you do not want all of the data, simply add a `LIMIT` clause.

:::tip
In ClickHouse Cloud, there is a cluster named `default`. Change `default` to the name of your cluster...or use the `s3` table function (instead of `s3Cluster`) if you do not have a cluster.
:::

```sql

```
