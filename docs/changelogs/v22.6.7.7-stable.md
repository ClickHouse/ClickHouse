---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v22.6.7.7-stable (8eae2af3b9a) FIXME as compared to v22.6.6.16-stable (d2a33ebc822)

#### Build/Testing/Packaging Improvement
* Backported in [#40692](https://github.com/ClickHouse/ClickHouse/issues/40692): Fix TGZ packages. [#40681](https://github.com/ClickHouse/ClickHouse/pull/40681) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

#### Bug Fix (user-visible misbehavior in official stable release)

* Backported in [#40531](https://github.com/ClickHouse/ClickHouse/issues/40531): Proxy resolver stop on first successful request to endpoint. [#40353](https://github.com/ClickHouse/ClickHouse/pull/40353) ([Maksim Kita](https://github.com/kitaisreal)).
* Backported in [#40623](https://github.com/ClickHouse/ClickHouse/issues/40623): Fix potential dataloss due to a bug in AWS SDK (https://github.com/aws/aws-sdk-cpp/issues/658). Bug can be triggered only when clickhouse is used over S3. [#40506](https://github.com/ClickHouse/ClickHouse/pull/40506) ([alesapin](https://github.com/alesapin)).
