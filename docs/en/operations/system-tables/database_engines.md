---
description: "System table containing a list of database engines supported by the server."
slug: /operations/system-tables/database_engines
title: "database_engines"
keywords: ["system table", "database_engines"]
---

Contains the list of database engines supported by the server.

This table contains the following columns (the column type is shown in brackets):

- `name` (String) — The name of database engine.

Example:

``` sql
SELECT *
FROM system.database_engines
WHERE name in ('Atomic', 'Lazy', 'Ordinary')
```

``` text
┌─name─────┐
│ Ordinary │
│ Atomic   │
│ Lazy     │
└──────────┘
```
