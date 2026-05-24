---
description: 'Documentation for the Row(...) data type, a single-stream column bundle used to reduce read amplification.'
sidebar_label: 'Row(name1 T1, name2 T2, ...)'
sidebar_position: 35
slug: /sql-reference/data-types/row
title: 'Row(name1 T1, name2 T2, ...)'
doc_type: 'reference'
---

## Overview {#overview}

`Row(name1 T1, name2 T2, …)` is a named, ordered bundle of typed fields that
is stored as a SINGLE physical column on disk — one length-prefixed binary
blob per row — in contrast to `Tuple`, which produces one physical column
file per element.

The intent is to **bundle columns that are frequently read together** so that
the storage layer pays one `open`/`seek` per row group instead of one per
column. This is useful for low-latency, narrow-`SELECT` workloads such as
recent-logs or single-trace lookups.

`Row` columns are typically declared with a `MATERIALIZED` expression that
mirrors the field list, so the wrapper is populated automatically on insert
and stays in sync with its source columns:

```sql
CREATE TABLE logs
(
    ts        DateTime,
    level     LowCardinality(String),
    msg       String,
    host      String,
    trace_id  String,
    combined  Row(level LowCardinality(String), msg String, host String, trace_id String)
        MATERIALIZED (level, msg, host, trace_id)
        CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY ts;
```

When you `SELECT level, msg, host, trace_id FROM logs ORDER BY ts DESC LIMIT
500`, the query optimizer detects that the `combined` wrapper covers all
four columns and reads from that single column file instead of four
separate ones — see the
[`query_plan_use_row_wrappers`](#query-plan-use-row-wrappers) setting below.

## Field naming {#field-naming}

All fields in a `Row` must be named (positional fields, allowed in `Tuple`,
are not allowed here — naming is mandatory because the field names are the
contract between the wrapper and its source columns). Field names must be
unique within the `Row`.

## Wrapper recognition rules {#wrapper-recognition-rules}

A `Row`-typed column is recognised as a **wrapper** of its source columns
if and only if:

1. The column is declared `MATERIALIZED`.
2. The materialised expression is a `tuple(...)` call whose arguments are
   bare identifiers.
3. The identifier list matches the `Row(...)` field list 1-for-1 in **order
   and name**.

Any column not meeting all three conditions remains a normal `Row` column
without the I/O optimization (it still works as a regular typed column).

Each source column may be wrapped by at most one `Row` wrapper. Declaring
two wrappers that share a source column is an error.

## Settings {#settings}

### `query_plan_use_row_wrappers` {#query-plan-use-row-wrappers}

Default: `1` (enabled).

Toggles the optimizer rule that routes column reads through a `Row(...)`
wrapper. The rule only fires when the wrapper would cover **at least two**
of the requested columns (or all of them — full hit, no waste), to avoid
reading an oversize blob for a single field. Set to `0` to compare baseline
vs. wrapper performance.

## Profile events {#profile-events}

- `RowWrapperReads` — incremented once per `ReadFromMergeTree` step that
  uses at least one wrapper.
- `RowWrapperReadFields` — incremented by the number of column reads
  avoided in that step.

## Status / known limitations {#status}

This is a draft skeleton; per-field subcolumn access (`combined.level`)
through the standard `IDataType` subcolumn machinery is not yet wired up,
and the optimizer rule currently only emits telemetry — the actual plan
rewrite is left to a follow-up commit. See the `optimizeUseRowWrappers.cpp`
source for the status of each piece.
