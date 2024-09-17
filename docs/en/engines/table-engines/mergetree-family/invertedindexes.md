---
slug: /en/engines/table-engines/mergetree-family/invertedindexes
sidebar_label:  Full-text Indexes
description: Quickly find search terms in text.
keywords: [full-text search, text search, index, indices]
---

# Full-text Search using Full-text Indexes [experimental]

Full-text indexes are an experimental type of [secondary indexes](/docs/en/engines/table-engines/mergetree-family/mergetree.md/#available-types-of-indices) which provide fast text search
capabilities for [String](/docs/en/sql-reference/data-types/string.md) or [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)
columns. The main idea of a full-text index is to store a mapping from "terms" to the rows which contain these terms. "Terms" are
tokenized cells of the string column. For example, the string cell "I will be a little late" is by default tokenized into six terms "I", "will",
"be", "a", "little" and "late". Another kind of tokenizer is n-grams. For example, the result of 3-gram tokenization will be 21 terms "I w",
" wi", "wil", "ill", "ll ", "l b", " be" etc. The more fine-granular the input strings are tokenized, the bigger but also the more
useful the resulting full-text index will be.

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/O_MnyUkrIq8"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

:::note
Full-text indexes are experimental and should not be used in production environments yet. They may change in the future in backward-incompatible
ways, for example with respect to their DDL/DQL syntax or performance/compression characteristics.
:::

## Usage

To use full-text indexes, first enable them in the configuration:

```sql
SET allow_experimental_full_text_index = true;
```

An full-text index can be defined on a string column using the following syntax

``` sql
CREATE TABLE tab
(
    `key` UInt64,
    `str` String,
    INDEX inv_idx(str) TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
```

:::note
In earlier versions of ClickHouse, the corresponding index type name was `inverted`.
:::

where `N` specifies the tokenizer:

- `full_text(0)` (or shorter: `full_text()`) set the tokenizer to "tokens", i.e. split strings along spaces,
- `full_text(N)` with `N` between 2 and 8 sets the tokenizer to "ngrams(N)"

The maximum rows per postings list can be specified as the second parameter. This parameter can be used to control postings list sizes to avoid generating huge postings list files. The following variants exist:

- `full_text(ngrams, max_rows_per_postings_list)`: Use given max_rows_per_postings_list (assuming it is not 0)
- `full_text(ngrams, 0)`: No limitation of maximum rows per postings list
- `full_text(ngrams)`: Use a default maximum rows which is 64K.

Being a type of skipping index, full-text indexes can be dropped or added to a column after table creation:

``` sql
ALTER TABLE tab DROP INDEX inv_idx;
ALTER TABLE tab ADD INDEX inv_idx(s) TYPE full_text(2);
```

To use the index, no special functions or syntax are required. Typical string search predicates automatically leverage the index. As
examples, consider:

```sql
INSERT INTO tab(key, str) values (1, 'Hello World');
SELECT * from tab WHERE str == 'Hello World';
SELECT * from tab WHERE str IN ('Hello', 'World');
SELECT * from tab WHERE str LIKE '%Hello%';
SELECT * from tab WHERE multiSearchAny(str, ['Hello', 'World']);
SELECT * from tab WHERE hasToken(str, 'Hello');
```

The full-text index also works on columns of type `Array(String)`, `Array(FixedString)`, `Map(String)` and `Map(String)`.

Like for other secondary indices, each column part has its own full-text index. Furthermore, each full-text index is internally divided into
"segments". The existence and size of the segments are generally transparent to users but the segment size determines the memory consumption
during index construction (e.g. when two parts are merged). Configuration parameter "max_digestion_size_per_segment" (default: 256 MB)
controls the amount of data read consumed from the underlying column before a new segment is created. Incrementing the parameter raises the
intermediate memory consumption for index construction but also improves lookup performance since fewer segments need to be checked on
average to evaluate a query.

## Full-text search of the Hacker News dataset

Let's look at the performance improvements of full-text indexes on a large dataset with lots of text. We will use 28.7M rows of comments on the popular Hacker News website. Here is the table without an full-text index:

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

The 28.7M rows are in a Parquet file in S3 - let's insert them into the `hackernews` table:

```sql
INSERT INTO hackernews
	SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
	dead UInt8,
	parent UInt64,
	poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

Consider the following simple search for the term `ClickHouse` (and its varied upper and lower cases) in the `comment` column:

```sql
SELECT count()
FROM hackernews
WHERE hasToken(lower(comment), 'clickhouse');
```

Notice it takes 3 seconds to execute the query:

```response
┌─count()─┐
│    1145 │
└─────────┘

1 row in set. Elapsed: 3.001 sec. Processed 28.74 million rows, 9.75 GB (9.58 million rows/s., 3.25 GB/s.)
```

We will use `ALTER TABLE` and add an full-text index on the lowercase of the `comment` column, then materialize it (which can take a while - wait for it to materialize):

```sql
ALTER TABLE hackernews
     ADD INDEX comment_lowercase(lower(comment)) TYPE full_text;

ALTER TABLE hackernews MATERIALIZE INDEX comment_lowercase;
```

We run the same query...

```sql
SELECT count()
FROM hackernews
WHERE hasToken(lower(comment), 'clickhouse')
```

...and notice the query executes 4x faster:

```response
┌─count()─┐
│    1145 │
└─────────┘

1 row in set. Elapsed: 0.747 sec. Processed 4.49 million rows, 1.77 GB (6.01 million rows/s., 2.37 GB/s.)
```

We can also search for one or all of multiple terms, i.e., disjunctions or conjunctions:

```sql
-- multiple OR'ed terms
SELECT count(*)
FROM hackernews
WHERE multiSearchAny(lower(comment), ['oltp', 'olap']);

-- multiple AND'ed terms
SELECT count(*)
FROM hackernews
WHERE hasToken(lower(comment), 'avx') AND hasToken(lower(comment), 'sve');
```

:::note
Unlike other secondary indices, full-text indexes (for now) map to row numbers (row ids) instead of granule ids. The reason for this design
is performance. In practice, users often search for multiple terms at once. For example, filter predicate `WHERE s LIKE '%little%' OR s LIKE
'%big%'` can be evaluated directly using an full-text index by forming the union of the row id lists for terms "little" and "big". This also
means that the parameter `GRANULARITY` supplied to index creation has no meaning (it may be removed from the syntax in the future).
:::

## Related Content

- Blog: [Introducing Inverted Indices in ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
