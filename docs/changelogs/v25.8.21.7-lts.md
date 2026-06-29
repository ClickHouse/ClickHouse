---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v25.8.21.7-lts (099badce0f7) FIXME as compared to v25.8.20.4-lts (2e1cd6354ae)

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#99728](https://github.com/ClickHouse/ClickHouse/issues/99728): Fix CHECK TABLE with sparse serialization inside Tuple with Dynamic. Closes [#96588](https://github.com/ClickHouse/ClickHouse/issues/96588). [#99351](https://github.com/ClickHouse/ClickHouse/pull/99351) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#100076](https://github.com/ClickHouse/ClickHouse/issues/100076): Fix ignoring of TABLE_UUID_MISMATCH for non analyzer. [#99380](https://github.com/ClickHouse/ClickHouse/pull/99380) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#100352](https://github.com/ClickHouse/ClickHouse/issues/100352): Fix incorrect partition pruning for `toWeek()` that caused queries with `WHERE toWeek(date, mode) = N` to return empty results for weeks 49-52 on tables partitioned by `toYYYYMM(date)`. [#99542](https://github.com/ClickHouse/ClickHouse/pull/99542) ([Takumi Hara](https://github.com/takumihara)).
* Backported in [#100093](https://github.com/ClickHouse/ClickHouse/issues/100093): Fix assertion failure (exception in debug builds, incorrect results in release builds) when multiplying `NumericIndexedVector` aggregate states by an even integer constant, caused by self-XOR on aliased Roaring bitmaps in `pointwiseAddInplace`. [#99976](https://github.com/ClickHouse/ClickHouse/pull/99976) ([Desel72](https://github.com/Desel72)).
* Backported in [#100164](https://github.com/ClickHouse/ClickHouse/issues/100164): ... [#100024](https://github.com/ClickHouse/ClickHouse/pull/100024) ([Shaohua Wang](https://github.com/tiandiwonder)).
* Backported in [#100590](https://github.com/ClickHouse/ClickHouse/issues/100590): Fix LOGICAL_ERROR exception "Stream ... not found" when inserting into a table with nested `Array(JSON)` columns in wide parts with `optimize_on_insert=0`. [#100475](https://github.com/ClickHouse/ClickHouse/pull/100475) ([Pavel Kruglov](https://github.com/Avogar)).

