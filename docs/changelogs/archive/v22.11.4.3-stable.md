---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v22.11.4.3-stable (7f4cf554f69) FIXME as compared to v22.11.3.47-stable (1c49d124a37)

#### Bug Fix (user-visible misbehavior in official stable release)

* Backported in [#45085](https://github.com/ClickHouse/ClickHouse/issues/45085): fix alter table ttl error when wide part has light weight delete mask. [#44959](https://github.com/ClickHouse/ClickHouse/pull/44959) ([Mingliang Pan](https://github.com/liangliangpan)).
