---
description: 'Quickly find search terms in text.'
keywords: ['full-text search', 'text index', 'index', 'indices']
sidebar_label: 'Full-text Search using Text Indexes'
slug: /engines/table-engines/mergetree-family/invertedindexes
title: 'Full-text Search using Text Indexes'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# Full-text Search using Text Indexes

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

Text indexes are an experimental type of [secondary indexes](/engines/table-engines/mergetree-family/mergetree.md/#available-types-of-indices) which provide fast text search capabilities for [String](/sql-reference/data-types/string.md) or [FixedString](/sql-reference/data-types/fixedstring.md) columns.
The main idea of a text index is to store a mapping from "terms" to the rows which contain these terms.
"Terms" are tokenized cells of the string column.
For example, the string cell "I will be a little late" is by default tokenized into six terms "I", "will", "be", "a", "little" and "late". Another kind of tokenizer is n-grams.
For example, the result of 3-gram tokenization will be 21 terms "I w", " wi", "wil", "ill", "ll ", "l b", " be" etc.
The more fine-granular the input strings are tokenized, the bigger but also the more useful the resulting text index will be.

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
Text indexes are experimental and should not be used in production environments yet.
They may change in the future in backward-incompatible ways, for example with respect to their DDL/DQL syntax or performance/compression characteristics.
:::

## Creating a Text Index {#creating-a-text-index}

To use text indexes, first enable them in the configuration:

```sql
SET allow_experimental_full_text_index = true;
```

An text index can be defined on a string column using the following syntax:

```sql
CREATE TABLE tab
(
    `key` UInt64,
    `str` String,
    INDEX inv_idx(str) TYPE text(tokenizer = 'default|ngram|split|no_op' [, ngram_size = N] [, separators = []] [, max_rows_per_postings_list = M]) [GRANULARITY 64]
)
ENGINE = MergeTree
ORDER BY key
```

`tokenizer` specifies the tokenizer:

- `default` set the tokenizer to "tokens('default')", i.e. split strings along non-alphanumeric characters.
- `ngram` set the tokenizer to "tokens('ngram')". i.e. split strings into equally large n-grams.
- `split` set the tokenizer to "tokens('split')", i.e. split strings along certain user-defined separator strings.
- `no_op` set the tokenizer to "tokens('no_op')", i.e. no tokenization takes place (every row value is a token).

The ngram size for the `ngram` tokenizer can be specified via the optional `ngram_size` parameter:

- `ngram_size = N`: with `N` between 2 and 8 sets the tokenizer to "tokens('ngram', N)".
- If unspecified: Use a default ngram size which is 3.

The separators for the `split` tokenizer can be specified via the optional `separators` parameter:

- `separators = []`: A list of strings, e.g. `separators = [', ', '; ', '\n', '\\']`.
- If not specified: Use a default separator which is a space (`[' ']`).

Unlike other skipping indexes, text indexes have a default index GRANULARITY of 64.
This value has been chosen empirically and it provides good performance for most use cases.
Advanced users may specify a different index GRANULARITY and improve the performance for specific data sets and search terms further.

:::note
In case of the `split` tokenizer: if the tokens do not form a [prefix code](https://en.wikipedia.org/wiki/Prefix_code), you likely want that the matching prefers longer separators first.
To do so, pass the separators in order of descending length.
For example, with separators = `['%21', '%']` string `%21abc` would be tokenized as `['abc']`, whereas separators = `['%', '%21']` would tokenize to `['21ac']` (which is likely not what you wanted).
:::

The maximum rows per postings list can be specified via the optional `max_rows_per_postings_list` parameter.
The parameter can be used to control postings list sizes to avoid generating huge postings list files.

- `max_rows_per_postings_list = 0`: No limitation of maximum rows per postings list.
- `max_rows_per_postings_list = M`: with `M` should be at least 8192.
- If not specified: Use a default maximum rows which is 64K.

Being a type of skipping index, text indexes can be dropped or added to a column after table creation:

```sql
ALTER TABLE tab DROP INDEX inv_idx;
ALTER TABLE tab ADD INDEX inv_idx(s) TYPE text(tokenizer = 'default');
```

## Using a Text Index {#using-a-text-index}

To use the index, no special functions or syntax are required.
Typical string search predicates automatically leverage the index. As examples, consider:

```sql
INSERT INTO tab(key, str) VALUES (1, 'Hello World');

SELECT * from tab WHERE str == 'Hello World';
SELECT * from tab WHERE str IN ('Hello', 'World');
SELECT * from tab WHERE str LIKE '%Hello%';
SELECT * from tab WHERE multiSearchAny(str, ['Hello', 'World']);
SELECT * from tab WHERE hasToken(str, 'Hello');
```

The text index also works on columns of type `Array(String)`, `Array(FixedString)`, `Map(String)` and `Map(String)`.

Like for other secondary indices, each column part has its own text index.
Furthermore, each text index is internally divided into "segments".
The existence and size of the segments are generally transparent to users but the segment size determines the memory consumption during index construction (e.g. when two parts are merged).
Configuration parameter `max_digestion_size_per_segment` (default: 256 MB) controls the amount of data read from the underlying column before a new segment is created.
The default value of the parameter provides a good balance between memory usage and performance for most use cases.
Incrementing it raises the intermediate memory consumption for index construction but also improves lookup performance since fewer segments need to be checked on average to evaluate a query.

### Functions Support {#functions-support}

The conditions in the `WHERE` clause contains calls of the functions that operate with columns.
If the column is a part of an index, ClickHouse tries to use this index when performing the functions.
ClickHouse supports different subsets of functions for the `text` index.

#### equals and notEquals {#functions-example-equals-notequals}

Functions `=` (equals) and `!=` (notEquals) check if the column contains rows which match the entire search term.

#### in and notIn {#functions-example-in-notin}

Functions `IN` (in) and `NOT IN` (`notIn`) are similar to functions `equals` and `notEquals` respectively.
Instead of matching a single term, they return true if any (`IN`) or no (`NOT IN`) search term matches a row value.

#### like, notLike and match {#functions-example-like-notlike-match}

:::note
Currently, these functions use the text index for filtering only if the index tokenizer is either `default` or `ngram`.
:::

In order to use functions `like`, `notLike`, and `match` with the `text` index, the search term should be in a way that complete tokens can be extracted from it.

Example:

```sql
SELECT count() FROM hackernews WHERE lower(comment) LIKE '% clickhouse support%';
```

In the example, only `clickhouse` is a complete token.
As `support` is followed by a `%`, it could match `support`, `supports`, `supporting` etc.
As a result, the lookup in the text index will only consider token `clickhouse`.

#### startsWith and endsWith {#functions-example-startswith-endswith}

Similar to `like`, the search term should be in a way that complete tokens can be extracted from it.

Example:

```sql
SELECT count() FROM hackernews WHERE startsWith(lower(comment), 'clickhouse support');
```

As in the previous example, the index lookup will only search for token `clickhouse` as `support` could match `support`, `supports`, `supporting` etc.
To search for a row value starting with `clickhouse supports`, use syntax `startsWith(lower(comment), 'clickhouse supports ')` (note the trailing space).

```sql
SELECT count() FROM hackernews WHERE endsWith(lower(comment), 'olap engine');
```

Similarly, if you like to search a column value ending with `olap engine`, use syntax `endsWith(lower(comment), ' olap engine')` (note the leading space).

:::note
Index lookups for functions `startsWith` and `endWidth` are generally less efficient than for functions `like`/`notLike`/`match`.
:::

#### multiSearchAny {#functions-example-multisearchany}

Function `multiSearchAny` searches the provided search term as a substring in the column value.
As a result, search term should be a complete token to use with the `text` index.
This can be achieved by putting a space before and after the input needle.

Example:

```sql
SELECT count() FROM hackernews WHERE multiSearchAny(lower(comment), [' clickhouse ', ' chdb ']);
```

#### hasToken and hasTokenOrNull {#functions-example-hastoken-hastokenornull}

Functions `hasToken` and `hasTokenOrNull` check if the column contains rows which match the search term or `NULL` (`hasTokenOrNull`).

Compared to other functions, `hasToken` and `hasTokenOrNull` do not tokenize the search term, i.e. they assume the input is a single token.

Example:

```sql
SELECT count() FROM hackernews WHERE hasToken(lower(comment), 'clickhouse');
```

These functions are the most performant options to use with the `text` index.

#### searchAny and searchAll {#functions-example-searchany-searchall}

Functions `searchAny` and `searchAll` check if the column contains rows which match any or all of search terms.

Compared to `hasToken`, these functions accept multiple search terms.

Example:

```sql
SELECT count() FROM hackernews WHERE searchAny(lower(comment), 'clickhouse chdb');

SELECT count() FROM hackernews WHERE searchAll(lower(comment), 'clickhouse chdb');
```

#### has {#functions-example-has}

Function `has` is also similar to `equals` in terms of matching the entire value.
Instead it operates on `Array` type, therefore it can be used with `Array(String)` or `Array(FixedString)` in `text` index.

Example how to define a `text` index to use with the `has` function:

```sql
CREATE TABLE tbl (
    id UInt64,
    values Array(String),
    INDEX idx_values(values) TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY id;
```

## Full-text search of the Hacker News dataset {#full-text-search-of-the-hacker-news-dataset}

Let's look at the performance improvements of text indexes on a large dataset with lots of text.
We will use 28.7M rows of comments on the popular Hacker News website. Here is the table without an text index:

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

We will use `ALTER TABLE` and add an text index on the lowercase of the `comment` column, then materialize it (which can take a while - wait for it to materialize):

```sql
ALTER TABLE hackernews
     ADD INDEX comment_lowercase(lower(comment)) TYPE text;

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
Unlike other secondary indices, text indexes (for now) map to row numbers (row ids) instead of granule ids.
The reason for this design is performance.
In practice, users often search for multiple terms at once.
For example, filter predicate `WHERE s LIKE '%little%' OR s LIKE '%big%'` can be evaluated directly using a text index by forming the union of the row id lists for terms "little" and "big".
:::

## Related Content {#related-content}

- Blog: [Introducing Inverted Indices in ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
