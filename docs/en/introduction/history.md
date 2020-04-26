---
toc_priority: 7
toc_title: History
---

# ClickHouse History {#clickhouse-history}

ClickHouse has been developed initially to power [Yandex.Metrica](https://metrica.yandex.com/), [the second largest web analytics platform in the world](http://w3techs.com/technologies/overview/traffic_analysis/all), and continues to be the core component of this system. With more than 13 trillion records in the database and more than 20 billion events daily, ClickHouse allows generating custom reports on the fly directly from non-aggregated data. This article briefly covers the goals of ClickHouse in the early stages of its development.

Yandex.Metrica builds customized reports on the fly based on hits and sessions, with arbitrary segments defined by the user. Doing so often requires building complex aggregates, such as the number of unique users. New data for building a report arrives in real-time.

As of April 2014, Yandex.Metrica was tracking about 12 billion events (page views and clicks) daily. All these events must be stored to build custom reports. A single query may require scanning millions of rows within a few hundred milliseconds, or hundreds of millions of rows in just a few seconds.

## Usage in Yandex.Metrica and Other Yandex Services {#usage-in-yandex-metrica-and-other-yandex-services}

ClickHouse serves multiple purposes in Yandex.Metrica.
Its main task is to build reports in online mode using non-aggregated data. It uses a cluster of 374 servers, which store over 20.3 trillion rows in the database. The volume of compressed data is about 2 PB, without accounting for duplicates and replicas. The volume of uncompressed data (in TSV format) would be approximately 17 PB.

ClickHouse also plays a key role in the following processes:

-   Storing data for Session Replay from Yandex.Metrica.
-   Processing intermediate data.
-   Building global reports with Analytics.
-   Running queries for debugging the Yandex.Metrica engine.
-   Analyzing logs from the API and the user interface.

Nowadays, there are multiple dozen ClickHouse installations in other Yandex services and departments: search verticals, e-commerce, advertisement, business analytics, mobile development, personal services, and others.

## Aggregated and Non-aggregated Data {#aggregated-and-non-aggregated-data}

There is a widespread opinion that to calculate statistics effectively, you must aggregate data since this reduces the volume of data.

But data aggregation comes with a lot of limitations:

-   You must have a pre-defined list of required reports.
-   The user can’t make custom reports.
-   When aggregating over a large number of distinct keys, the data volume is barely reduced, so aggregation is useless.
-   For a large number of reports, there are too many aggregation variations (combinatorial explosion).
-   When aggregating keys with high cardinality (such as URLs), the volume of data is not reduced by much (less than twofold).
-   For this reason, the volume of data with aggregation might grow instead of shrink.
-   Users do not view all the reports we generate for them. A large portion of those calculations is useless.
-   The logical integrity of data may be violated for various aggregations.

If we do not aggregate anything and work with non-aggregated data, this might reduce the volume of calculations.

However, with aggregation, a significant part of the work is taken offline and completed relatively calmly. In contrast, online calculations require calculating as fast as possible, since the user is waiting for the result.

Yandex.Metrica has a specialized system for aggregating data called Metrage, which was used for the majority of reports.
Starting in 2009, Yandex.Metrica also used a specialized OLAP database for non-aggregated data called OLAPServer, which was previously used for the report builder.
OLAPServer worked well for non-aggregated data, but it had many restrictions that did not allow it to be used for all reports as desired. These included the lack of support for data types (only numbers), and the inability to incrementally update data in real-time (it could only be done by rewriting data daily). OLAPServer is not a DBMS, but a specialized DB.

The initial goal for ClickHouse was to remove the limitations of OLAPServer and solve the problem of working with non-aggregated data for all reports, but over the years, it has grown into a general-purpose database management system suitable for a wide range of analytical tasks.

[Original article](https://clickhouse.tech/docs/en/introduction/history/) <!--hide-->
