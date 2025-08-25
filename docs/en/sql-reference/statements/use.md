---
description: 'Documentation for USE Statement'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'USE Statement'
---

# USE Statement

```sql
USE db
```

Lets you set the current database for the session.

The current database is used for searching for tables if the database is not explicitly defined in the query with a dot before the table name.

This query can't be made when using the HTTP protocol, since there is no concept of a session.
