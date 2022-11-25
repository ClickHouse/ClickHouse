---
sidebar_position: 45
sidebar_label: EXISTS
---

# EXISTS {#exists}

``` sql
EXISTS [TEMPORARY] TABLE [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Возвращает один столбец типа `UInt8`, содержащий одно значение - `0`, если таблицы или БД не существует и `1`, если таблица в указанной БД существует.


