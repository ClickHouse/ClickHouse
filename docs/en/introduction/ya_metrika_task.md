# The Yandex.Metrica task

ClickHouse currently powers [ Yandex.Metrica](https://metrika.yandex.ru/), [ the second largest platform in the world](http://w3techs.com/technologies/overview/traffic_analysis/all) for Web Analytics. With more than 13 trillion records in the database and more than 20 billion events daily, ClickHouse allows you generating custom reports on the fly directly from non-aggregated data.

We need to get custom reports based on hits and sessions, with custom segments set by the user. Data for the reports is updated in real-time. Queries must be run immediately (in online mode). We must be able to build reports for any time period. Complex aggregates must be calculated, such as the number of unique visitors.
At this time (April 2014), Yandex.Metrica receives approximately 12 billion events (pageviews and mouse clicks) daily. All these events must be stored in order to build custom reports. A single query may require scanning hundreds of millions of rows over a few seconds, or millions of rows in no more than a few hundred milliseconds.

## Usage in Yandex.Metrica and other Yandex services

ClickHouse is used for multiple purposes in Yandex.Metrica.
Its main task is to build reports in online mode using non-aggregated data. It uses a cluster of 374 servers, which store over 20.3 trillion rows in the database. The volume of compressed data, without counting duplication and replication, is about 2 PB. The volume of uncompressed data (in TSV format) would be approximately 17 PB.

ClickHouse is also used for:

- Storing data for Session Replay from Yandex.Metrica.
- Processing intermediate data.
- Building global reports with Analytics.
- Running queries for debugging the Yandex.Metrica engine.
- Analyzing logs from the API and the user interface.

ClickHouse has at least a dozen installations in other Yandex services: in search verticals, Market, Direct, business analytics, mobile development, AdFox, personal services, and others.

## Aggregated and non-aggregated data

There is a popular opinion that in order to effectively calculate statistics, you must aggregate data, since this reduces the volume of data.

But data aggregation is a very limited solution, for the following reasons:

- You must have a pre-defined list of reports the user will need.
- The user can't make custom reports.
- When aggregating a large quantity of keys, the volume of data is not reduced, and aggregation is useless.
- For a large number of reports, there are too many aggregation variations (combinatorial explosion).
- When aggregating keys with high cardinality (such as URLs), the volume of data is not reduced by much (less than twofold).
- For this reason, the volume of data with aggregation might grow instead of shrink.
- Users do not view all the reports we generate for them. A large portion of calculations are useless.
- The logical integrity of data may be violated for various aggregations.

If we do not aggregate anything and work with non-aggregated data, this might actually reduce the volume of calculations.

However, with aggregation, a significant part of the work is taken offline and completed relatively calmly. In contrast, online calculations require calculating as fast as possible, since the user is waiting for the result.

Yandex.Metrica has a specialized system for aggregating data called Metrage, which is used for the majority of reports.
Starting in 2009, Yandex.Metrica also used a specialized OLAP database for non-aggregated data called OLAPServer, which was previously used for the report builder.
OLAPServer worked well for non-aggregated data, but it had many restrictions that did not allow it to be used for all reports as desired. These included the lack of support for data types (only numbers), and the inability to incrementally update data in real-time (it could only be done by rewriting data daily). OLAPServer is not a DBMS, but a specialized DB.

To remove the limitations of OLAPServer and solve the problem of working with non-aggregated data for all reports, we developed the ClickHouse DBMS.

