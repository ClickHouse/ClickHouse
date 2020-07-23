---
toc_priority: 47
toc_title: EXISTS
---

# EXISTS Statement {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `UInt8`-type column, which contains the single value `0` if the table or database doesn’t exist, or `1` if the table exists in the specified database.
