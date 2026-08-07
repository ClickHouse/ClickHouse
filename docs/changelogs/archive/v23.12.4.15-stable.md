---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v23.12.4.15-stable (4233d111d20) FIXME as compared to v23.12.3.40-stable (a594704ae75)

#### Bug Fix (user-visible misbehavior in an official stable release)

* Fix incorrect result of arrayElement / map[] on empty value [#59594](https://github.com/ClickHouse/ClickHouse/pull/59594) ([Raúl Marín](https://github.com/Algunenano)).
* Fix crash in topK when merging empty states [#59603](https://github.com/ClickHouse/ClickHouse/pull/59603) ([Raúl Marín](https://github.com/Algunenano)).
* Fix distributed table with a constant sharding key [#59606](https://github.com/ClickHouse/ClickHouse/pull/59606) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix leftPad / rightPad function with FixedString input [#59739](https://github.com/ClickHouse/ClickHouse/pull/59739) ([Raúl Marín](https://github.com/Algunenano)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Fix 02720_row_policy_column_with_dots [#59453](https://github.com/ClickHouse/ClickHouse/pull/59453) ([Duc Canh Le](https://github.com/canhld94)).
* Pin python dependencies in stateless tests [#59663](https://github.com/ClickHouse/ClickHouse/pull/59663) ([Raúl Marín](https://github.com/Algunenano)).

