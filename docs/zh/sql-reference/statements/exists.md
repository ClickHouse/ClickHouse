---
sidebar_position: 45
sidebar_label: EXISTS
---

# EXISTS 语句 {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

返回一个单独的 `UInt8`类型的列，如果表或数据库不存在，则包含一个值 `0`，如果表在指定的数据库中存在，则包含一个值 `1`。