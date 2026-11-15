---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v22.8.8.3-lts (ac5a6cababc) FIXME as compared to v22.8.7.34-lts (3c38e5e8ab9)

#### Bug Fix (user-visible misbehavior in official stable release)

* Backported in [#42677](https://github.com/ClickHouse/ClickHouse/issues/42677): keeper-fix: fix race in accessing logs while snapshot is being installed. [#40627](https://github.com/ClickHouse/ClickHouse/pull/40627) ([Antonio Andelic](https://github.com/antonio2368)).
