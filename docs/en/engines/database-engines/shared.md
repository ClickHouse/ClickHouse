---
description: 'Page describing the `Shared` database engine, available in ClickHouse Cloud'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge/>

# Shared database engine

The `Shared` database engine works in conjunction with Shared Catalog to manage databases whose tables use stateless table engines such as [`SharedMergeTree`](/cloud/reference/shared-merge-tree).
These table engines do not write persistent state to disk and are compatible with dynamic compute environments.

The `Shared` database engine in Cloud removes the dependency for local disks.
It is a purely in-memory engine, requiring only CPU and memory.

## How does it work? {#how-it-works}

The `Shared` database engine stores all database and table definitions in a central Shared Catalog backed by Keeper. Instead of writing to local disk, it maintains a single versioned global state shared across all compute nodes.

Each node tracks only the last applied version and, on startup, fetches the latest state without the need for local files or manual setup.

## Syntax {#syntax}

For end users, using Shared Catalog and the Shared database engine requires no additional configuration. Database creation is the same as always:

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud automatically assigns the Shared database engine to databases. Any tables created within such a database using stateless engines will automatically benefit from Shared Catalog’s replication and coordination capabilities.

:::tip
For more information on Shared Catalog and it's benefits, see ["Shared catalog and shared database engine"](/cloud/reference/shared-catalog) in the Cloud reference section.
:::
