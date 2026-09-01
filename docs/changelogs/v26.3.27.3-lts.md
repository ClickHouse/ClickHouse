---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v26.3.27.3-lts (22b1ff96abf) FIXME as compared to v26.3.26.3-lts (4545d1a94b0)

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#116762](https://github.com/ClickHouse/ClickHouse/issues/116762): Fix a crash for `ORDER BY ALL` in a `WINDOW VIEW`, and `UNKNOWN_IDENTIFIER` errors for `INTERPOLATE`, `WINDOW` and `LIMIT BY` in a query over a `Merge` or `Distributed` table with a join, with the old analyzer. [#113759](https://github.com/ClickHouse/ClickHouse/pull/113759) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#117149](https://github.com/ClickHouse/ClickHouse/issues/117149): Sizes declared by a server in the Native protocol no longer make the client allocate memory for data that has not arrived: exception strings are read as they arrive, the decompressed size of a block is bounded, and the maximum number of rows in a `Native` block is now a billion. [#117011](https://github.com/ClickHouse/ClickHouse/pull/117011) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

