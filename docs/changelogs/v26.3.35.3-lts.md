---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v26.3.35.3-lts (3ea8391f6b8) FIXME as compared to v26.3.34.136-lts (4c1242ee175)

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#122583](https://github.com/ClickHouse/ClickHouse/issues/122583): Fix a segmentation fault in Keeper when preallocating space for a changelog file fails because the disk is full. [#122337](https://github.com/ClickHouse/ClickHouse/pull/122337) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* `FPC(<level>)` codecs with big levels are now handled more gracefully. [#122536](https://github.com/ClickHouse/ClickHouse/pull/122536) ([Robert Schulze](https://github.com/rschu1ze)).

