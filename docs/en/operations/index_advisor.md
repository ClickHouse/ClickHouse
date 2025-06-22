# Index Advisor ⚠️ Experimental

The **Index Advisor** is a tool that automatically recommends an efficient `PRIMARY KEY` or set of data skipping indexes based on analysis of a static list of queries (*workload*).
Currently supports only the **MergeTree** engine family.

> ⚠️ **Experimental feature** – the behavior may change in future releases.

---

## Purpose

The advisor analyzes query patterns for a given table and suggests a primary key or data skipping indexes that can reduce the number of read marks and improve query performance.

---

## How to Use

### Submit Your Workload

First, collect the queries you commonly use. Start recording with:

```sql
START COLLECTING WORKLOAD;
```

All executed queries will be logged into a file specified by the `collection_file_path` setting (default: `/tmp/workload_collection.txt`).

After running the queries you want to analyze, stop recording with:

```sql
FINISH COLLECTING WORKLOAD;
```

This returns a list of the best primary keys or data skipping indexes, depending on your settings.

---

### Control the Algorithm

You can configure what the advisor should optimize for using the following settings:

| Setting                                | Description                                                                   | Default |
| -------------------------------------- | ----------------------------------------------------------------------------- | ------- |
| `index_advisor_find_best_pk`           | Whether to search for an optimal primary key                                  | `false` |
| `index_advisor_find_best_index`        | Whether to search for optimal data skipping indexes                           | `false` |
| `index_advisor_sampling_proportion`    | Fraction of data to sample (from 0.0 to 1.0). Only affects primary key search | `0.01`  |
| `max_index_advisor_pk_columns_count`   | Maximum number of columns in suggested primary key                            | `5`     |
| `max_index_advise_index_columns_count` | Maximum number of columns in suggested index                                  | `3`     |

> **Note:** You must enable at least one of `index_advisor_find_best_pk` or `index_advisor_find_best_index` to receive any results.

---

## How It Works

### Primary Key Search

1. A sample clone of the table is created with a candidate `PRIMARY KEY`.
2. A subset of the collected queries is executed using `EXPLAIN ESTIMATE`.
3. The advisor evaluates estimated read costs and suggests the best-performing column combination.

> **Note:** Primary key search is resource-intensive due to sorting. Higher `index_advisor_sampling_proportion` gives better results but increases runtime.

---

### Data Skipping Index Search

1. The advisor uses the **original table**.
2. Each supported index type is tested individually.
3. A subset of the queries is evaluated using `EXPLAIN ESTIMATE`, and the advisor ranks the results.

Supported index types include:

* `minmax`
* `set(256)`
* `bloom_filter(0.01)`
* `tokenbf_v1(2048, 3, 1)`
* `ngrambf_v1(3, 2048, 3, 1)`

> **Note:** Index hyperparameter tuning and granularity search are **not supported**.
> For safety, you may want to test on a clone of your original table.

---

## Example

```sql
-- Create a test table
CREATE TABLE example (
    i Int32,
    s String,
    PRIMARY KEY s
) ENGINE = MergeTree()
SETTINGS index_granularity = 1;

-- Set advisor parameters
SET max_index_advisor_pk_columns_count = 1;
SET max_index_advise_index_columns_count = 4;
SET index_advisor_find_best_pk = 1;
SET index_advisor_find_best_index = 1;

-- Execute sample queries
START COLLECTING WORKLOAD;
SELECT * FROM example WHERE i < 3;
SELECT * FROM example WHERE i > 3;
SELECT * FROM example WHERE s LIKE 'Hello, world%';
FINISH COLLECTING WORKLOAD;
```

**Sample Output:**

```text
┌─table_name─┬─index_type──┬─columns for index─┐
│ example    │ primary_key │ (i)               │
│ example    │ minmax      │ i                 │
│ example    │ set(256)    │ i                 │
│ example    │ minmax      │ s                 │
│ example    │ set(256)    │ s                 │
└────────────┴─────────────┴───────────────────┘
```

### Explanation

* Column `i` appears in two queries and significantly reduces read cost, so it is suggested as the best `PRIMARY KEY` and index target.
* Column `s` also helps with filtering, but is less effective than `i`, so it is ranked lower.
