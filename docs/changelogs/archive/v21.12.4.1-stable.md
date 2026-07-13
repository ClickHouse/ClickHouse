---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v21.12.4.1-stable FIXME as compared to v21.12.3.32-stable

#### Improvement
* Backported in [#33792](https://github.com/ClickHouse/ClickHouse/issues/33792): Create parent directories in DiskS3::restoreFileOperations method. [#33730](https://github.com/ClickHouse/ClickHouse/pull/33730) ([ianton-ru](https://github.com/ianton-ru)).

#### Bug Fix
* Backported in [#33551](https://github.com/ClickHouse/ClickHouse/issues/33551): Fix null pointer dereference in low cardinality data when deserializing LowCardinality data in the Native format. [#33021](https://github.com/ClickHouse/ClickHouse/pull/33021) ([Harry Lee](https://github.com/HarryLeeIBM)).

#### Bug Fix (user-visible misbehaviour in official stable release)

* Backported in [#33537](https://github.com/ClickHouse/ClickHouse/issues/33537): Fix ORC stripe reading. [#32929](https://github.com/ClickHouse/ClickHouse/pull/32929) ([Ernest Zaslavsky](https://github.com/kreuzerkrieg)).
* Backported in [#33654](https://github.com/ClickHouse/ClickHouse/issues/33654): Fix segfault in Avro that appears after the second insert into file. [#33566](https://github.com/ClickHouse/ClickHouse/pull/33566) ([Kruglov Pavel](https://github.com/Avogar)).
