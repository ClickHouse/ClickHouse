---
slug: /sql-reference/statements/exists
sidebar_position: 45
sidebar_label: 'EXISTS'
description: 'Documentation for EXISTS Statement'
title: 'EXISTS Statement'
---

# EXISTS Statement

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `UInt8`-type column, which contains the single value `0` if the table or database does not exist, or `1` if the table exists in the specified database.
