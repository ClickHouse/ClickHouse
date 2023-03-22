# Inverted indexes [experimental] {#table_engines-ANNIndex}

Inverted indexes are an experimental type of [secondary indexes](mergetree.md#available-types-of-indices) which provide fast text search
capabilities for [String](../../../sql-reference/data-types/string.md) or [FixedString](../../../sql-reference/data-types/fixedstring.md)
columns. The main idea of an inverted indexes is to store a mapping from "terms" to the rows which contains these terms. "Terms" are
tokenized cells of the string column. For example, string cell "I will be a little late" is by default tokenized into six terms "I", "will",
"be", "a", "little" and "late". Another kind of tokenizer are n-grams. For example, the result of 3-gram tokenization will be 21 terms "I w",
" wi", "wil", "ill", "ll ", "l b", " be" etc. The more fine-granular the input strings are tokenized, the bigger but also the more
useful the resulting inverted index will be. 

:::warning
Inverted indexes are experimental and should not be used in production environment yet. They may change in future in backwards-incompatible
ways, for example with respect to their DDL/DQL syntax or performance/compression characteristics.
:::

## Usage

To use inverted indexes, first enable them in the configuration:

```sql
SET allow_experimental_inverted_index = true;
```

An inverted index can be defined on a string column using the following syntax

``` sql
CREATE TABLE tab (key UInt64, str String, INDEX inv_idx(s) TYPE inverted(N) GRANULARITY 1) Engine=MergeTree ORDER BY (k);
```

where `N` specifies the tokenizer:

- `inverted(0)` (or shorter: `inverted()`) set the tokenizer to "tokens", i.e. split strings along spaces,
- `inverted(N)` with `N` between 2 and 8 sets the tokenizer to "ngrams(N)"

Being a type of skipping indexes, inverted indexes can be dropped or added to a column after table creation:

``` sql
ALTER TABLE tbl DROP INDEX inv_idx;
ALTER TABLE tbl ADD INDEX inv_idx(s) TYPE inverted(2) GRANULARITY 1;
```

To use the index, no special functions or syntax are required. Typical string search predicates automatically leverage the index. As
examples, consider:

```sql
SELECT * from tab WHERE s == 'Hello World;
SELECT * from tab WHERE s IN (‘Hello’, ‘World’);
SELECT * from tab WHERE s LIKE ‘%Hello%’;
SELECT * from tab WHERE multiSearchAny(s, ‘Hello’, ‘World’);
SELECT * from tab WHERE hasToken(s, ‘Hello’);
SELECT * from tab WHERE multiSearchAll(s, [‘Hello’, ‘World’]);
```

The inverted index also works on columns of type `Array(String)`, `Array(FixedString)`, `Map(String)` and `Map(String)`.

Like for other secondary indices, each column part has its own inverted index. Furthermore, each inverted index is internally divided into
"segments". The existence and size of the segments is generally transparent to users but the segment size determines the memory consumption
during index construction (e.g. when two parts are merged). Configuration parameter "max_digestion_size_per_segment" (default: 256 MB)
controls the amount of data read consumed from the underlying column before a new segment is created. Incrementing the parameter raises the
intermediate memory consumption for index constuction but also improves lookup performance since fewer segments need to be checked on
average to evaluate a query.

Unlike other secondary indices, inverted indexes (for now) map to row numbers (row ids) instead of granule ids. The reason for this design
is performance. In practice, users often search for multiple terms at once. For example, filter predicate `WHERE s LIKE '%little%' OR s LIKE
'%big%'` can be evaluated directly using an inverted index by forming the union of the rowid lists for terms "little" and "big". This also
means that parameter `GRANULARITY` supplied to index creation has no meaning (it may be removed from the syntax in future).
