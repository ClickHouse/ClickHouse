---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v24.1.2.5-stable (b2605dd4a5a) FIXME as compared to v24.1.1.2048-stable (5a024dfc093)

#### Bug Fix (user-visible misbehavior in an official stable release)

* Backported in [#59425](https://github.com/ClickHouse/ClickHouse/issues/59425): Fix translate() with FixedString input. Could lead to crashes as it'd return a String column (vs the expected FixedString). This issue was found through ClickHouse Bug Bounty Program YohannJardin. [#59356](https://github.com/ClickHouse/ClickHouse/pull/59356) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#59478](https://github.com/ClickHouse/ClickHouse/issues/59478): Fix stacktraces for binaries without debug symbols. [#59444](https://github.com/ClickHouse/ClickHouse/pull/59444) ([Azat Khuzhin](https://github.com/azat)).

