---
title: 'Postgres generated columns: gotchas and best practices'
slug: /integrations/clickpipes/postgres/generated_columns
description: 'Page describing important considerations to keep in mind when using PostgreSQL generated columns in tables that are being replicated'
doc_type: 'guide'
keywords: ['clickpipes', 'postgresql', 'cdc', 'data ingestion', 'real-time sync']
integration:
  - support_level: 'core'
  - category: 'clickpipes'
---

When using PostgreSQL's generated columns in tables that are being replicated, there are some important considerations to keep in mind. These gotchas can affect the replication process and data consistency in your destination systems.

## The problem with generated columns {#the-problem-with-generated-columns}

1. **Not Published via `pgoutput`:** Generated columns aren't published through the `pgoutput` logical replication plugin. This means that when you're replicating data from PostgreSQL to another system, the values of generated columns aren't included in the replication stream.

2. **Issues with Primary Keys:** If a generated column is part of your primary key, it can cause deduplication problems on the destination. Since the generated column values aren't replicated, the destination system won't have the necessary information to properly identify and deduplicate rows.

3. **Issues with schema changes**: If you add a generated column to a table that is already being replicated, the new column won't be populated in the destination - as Postgres doesn't give us the RelationMessage for the new column. If you then add a new non-generated column to the same table, the ClickPipe, when trying to reconcile the schema, won't be able to find the generated column in the destination, leading to a failure in the replication process.

## Best practices {#best-practices}

To work around these limitations, consider the following best practices:

1. **Recreate Generated Columns on the Destination:** Instead of relying on the replication process to handle generated columns, it's recommended to recreate these columns on the destination using tools like dbt (data build tool) or other data transformation mechanisms.

2. **Avoid Using Generated Columns in Primary Keys:** When designing tables that will be replicated, it's best to avoid including generated columns as part of the primary key.

## Upcoming improvements to UI {#upcoming-improvements-to-ui}

In upcoming versions, we're planning to add a UI to help you with the following:

1. **Identify Tables with Generated Columns:** The UI will have a feature to identify tables that contain generated columns. This will help you understand which tables are affected by this issue.

2. **Documentation and Best Practices:** The UI will include best practices for using generated columns in replicated tables, including guidance on how to avoid common pitfalls.
