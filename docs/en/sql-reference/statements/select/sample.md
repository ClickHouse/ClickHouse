---
description: 'Documentation for SAMPLE Clause'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'SAMPLE Clause'
doc_type: 'reference'
---

The `SAMPLE` clause allows for approximated `SELECT` query processing.

When data sampling is enabled, the query is not performed on all the data, but only on a certain fraction of data (sample). For example, if you need to calculate statistics for all the visits, it is enough to execute the query on the 1/10 fraction of all the visits and then multiply the result by 10.

Approximated query processing can be useful in the following cases:

- When you have strict latency requirements (like below 100ms) but you can't justify the cost of additional hardware resources to meet them.
- When your raw data is not accurate, so approximation does not noticeably degrade the quality.
- Business requirements target approximate results (for cost-effectiveness, or to market exact results to premium users).

:::note
You can only use sampling with the tables in the [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) family, and only if the sampling expression was specified during table creation (see [MergeTree engine](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)). For `MergeTree` tables created without a `SAMPLE BY` key, see the experimental [Bernoulli Sampling](#bernoulli-sampling) section below.
:::

The features of data sampling are listed below:

- Data sampling is a deterministic mechanism. The result of the same `SELECT .. SAMPLE` query is always the same.
- Sampling works consistently for different tables. For tables with a single sampling key, a sample with the same coefficient always selects the same subset of possible data. For example, a sample of user IDs takes rows with the same subset of all the possible user IDs from different tables. This means that you can use the sample in subqueries in the [IN](../../../sql-reference/operators/in.md) clause. Also, you can join samples using the [JOIN](../../../sql-reference/statements/select/join.md) clause.
- Sampling allows reading less data from a disk. Note that you must specify the sampling key correctly. For more information, see [Creating a MergeTree Table](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

For the `SAMPLE` clause the following syntax is supported:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                    |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`   | Here `k` is the number from 0 to 1. The query is executed on `k` fraction of data. For example, `SAMPLE 0.1` runs the query on 10% of data. [Read more](#sample-k)                                                                             |
| `SAMPLE n`    | Here `n` is a sufficiently large integer. The query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows. [Read more](#sample-n) |
| `SAMPLE k OFFSET m`  | Here `k` and `m` are the numbers from 0 to 1. The query is executed on a sample of `k` fraction of the data. The data used for the sample is offset by `m` fraction. [Read more](#sample-k-offset-m)                                           |

## SAMPLE K {#sample-k}

Here `k` is the number from 0 to 1 (both fractional and decimal notations are supported). For example, `SAMPLE 1/2` or `SAMPLE 0.5`.

In a `SAMPLE k` clause, the sample is taken from the `k` fraction of data. The example is shown below:

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

In this example, the query is executed on a sample from 0.1 (10%) of data. Values of aggregate functions are not corrected automatically, so to get an approximate result, the value `count()` is manually multiplied by 10.

## SAMPLE N {#sample-n}

Here `n` is a sufficiently large integer. For example, `SAMPLE 10000000`.

In this case, the query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows.

Since the minimum unit for data reading is one granule (its size is set by the `index_granularity` setting), it makes sense to set a sample that is much larger than the size of the granule.

When using the `SAMPLE n` clause, you do not know which relative percent of data was processed. So you do not know the coefficient the aggregate functions should be multiplied by. Use the `_sample_factor` virtual column to get the approximate result.

The `_sample_factor` column contains relative coefficients that are calculated dynamically. This column is created automatically when you [create](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) a table with the specified sampling key. The usage examples of the `_sample_factor` column are shown below.

Let's consider the table `visits`, which contains the statistics about site visits. The first example shows how to calculate the number of page views:

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

The next example shows how to calculate the total number of visits:

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

The example below shows how to calculate the average session duration. Note that you do not need to use the relative coefficient to calculate the average values.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

## SAMPLE K OFFSET M {#sample-k-offset-m}

Here `k` and `m` are numbers from 0 to 1. Examples are shown below.

**Example 1**

```sql
SAMPLE 1/10
```

In this example, the sample is 1/10th of all data:

`[++------------]`

**Example 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Here, a sample of 10% is taken from the second half of the data.

`[------++------]`

## Bernoulli Sampling (Experimental) {#bernoulli-sampling}

An alternative execution strategy for the `SAMPLE` clause that works on
`MergeTree`-family tables created **without** a `SAMPLE BY` key. Each row is
kept independently with probability `p`, where `p` is the value passed to
`SAMPLE`.

:::note Experimental
Enable for a session with:

```sql
SET allow_experimental_bernoulli_sample = 1;
```
:::

### How it differs from `SAMPLE BY` {#bernoulli-vs-sample-by}

- There is no sampling key, so two Bernoulli samples on different tables are
  statistically independent and not possible to join via `IN` subqueries - unlike
  `SAMPLE BY`, which gives the cross-table consistency property described at
  the top of this page.
- With a nonzero `bernoulli_sample_seed` and an unchanged set of parts, the
  same query returns the same rows regardless of `max_threads`; `0` re-seeds
  randomly per query.
- Both `SAMPLE k` (fraction) and `SAMPLE n` (target row count) are supported,
  and `_sample_factor` is populated as usual.
- `SAMPLE k OFFSET m` is **not** supported and still raises `SAMPLING_NOT_SUPPORTED`.
- Composes with `PREWHERE`, skip indexes, multi-part tables, the `Merge`
  engine, and parallel replicas in the default `read_tasks` mode.
- Non-`MergeTree` engines (for example `Memory`) still reject `SAMPLE`.

### Settings {#bernoulli-settings}

| Setting | Type | Default | Purpose |
|---|---|---|---|
| `allow_experimental_bernoulli_sample` | `Bool` | `false` | Enables Bernoulli sampling for `MergeTree` tables without a `SAMPLE BY` key. |
| `bernoulli_sample_seed` | `UInt64` | `1` | Seed for the Bernoulli sampler. `0` re-seeds randomly per query; any nonzero value is deterministic for a given list of parts in their natural enumeration order. Merges (and other operations that replace existing parts) will renumber surviving parts and draw a different sample for them. |

### Example {#bernoulli-example}

```sql
SET allow_experimental_bernoulli_sample = 1;

CREATE TABLE t_bernoulli (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli SELECT number FROM numbers(100000);
```

`SAMPLE k` (fractional form) keeps each row independently with probability
`k`. To estimate a full-table aggregate, multiply by `1/k` - or, equivalently,
by the `_sample_factor` virtual column:

```sql
SELECT count() AS sampled_rows, count() * 10 AS approx_total
FROM t_bernoulli SAMPLE 0.1
SETTINGS bernoulli_sample_seed = 42;
```

```response
   ┌─sampled_rows─┬─approx_total─┐
1. │         9913 │        99130 │
   └──────────────┴──────────────┘
```

`SAMPLE n` (absolute form) asks for *approximately* `n` rows. The sampler
picks a probability that yields `n` rows on average and stores its inverse in
`_sample_factor`, which is the right multiplier for unbiased estimates:

```sql
SELECT count() AS sampled_rows, sum(_sample_factor) AS approx_total
FROM t_bernoulli SAMPLE 50000
SETTINGS bernoulli_sample_seed = 42;
```

```response
   ┌─sampled_rows─┬─approx_total─┐
1. │        49926 │        99852 │
   └──────────────┴──────────────┘
```

Both estimates land near the true row count of `100000`. Changing
`bernoulli_sample_seed` (or setting it to `0` for a fresh seed per query)
draws a different random subset; everything else is identical between runs as
long as the underlying parts do not change.
