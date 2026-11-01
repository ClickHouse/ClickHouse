---
slug: /integrations/postgresql/inserting-data
title: 'How to insert data from PostgreSQL'
keywords: ['postgres', 'postgresql', 'inserts']
description: 'Page describing how to insert data from PostgresSQL using ClickPipes, PeerDB or the Postgres table function'
doc_type: 'guide'
---

We recommend reading [this guide](/concepts/features/operations/insert/inserting-data) to learn best practices on inserting data to ClickHouse to optimize for insert performance.

For bulk loading data from PostgreSQL, you can use:

- using [ClickPipes](/integrations/clickpipes/postgres/index), the managed integration service for ClickHouse Cloud.
- `PeerDB by ClickHouse`, an ETL tool specifically designed for PostgreSQL database replication to both self-hosted ClickHouse and ClickHouse Cloud.
- The [Postgres Table Function](/reference/functions/table-functions/postgresql) to read data directly. This is typically appropriate for if batch replication based on a known watermark, e.g. a timestamp. is sufficient or if it's a once-off migration. This approach can scale to 10's of millions of rows. Users looking to migrate larger datasets should consider multiple requests, each dealing with a chunk of the data. Staging tables can be used for each chunk prior to its partitions being moved to a final table. This allows failed requests to be retried.  For further details on this bulk-loading strategy, see here.
- Data can be exported from Postgres in CSV format. This can then be inserted into ClickHouse from either local files or via object storage using table functions.
