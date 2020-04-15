---
toc_priority: 5
toc_title: ClickHouse Features that Can Be Considered Disadvantages
---

# ClickHouse Features that Can Be Considered Disadvantages {#clickhouse-features-that-can-be-considered-disadvantages}

1.  No full-fledged transactions.
2.  Lack of ability to modify or delete already inserted data with high rate and low latency. There are batch deletes and updates available to clean up or modify data, for example to comply with [GDPR](https://gdpr-info.eu).
3.  The sparse index makes ClickHouse not so suitable for point queries retrieving single rows by their keys.

[Original article](https://clickhouse.tech/docs/en/introduction/features_considered_disadvantages/) <!--hide-->
