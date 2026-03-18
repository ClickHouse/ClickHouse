---
slug: /ru/sql-reference/statements/select/offset
sidebar_label: OFFSET
---

# Секция OFFSET FETCH {#offset-fetch}

`OFFSET` и `FETCH` позволяют извлекать данные по частям. Они указывают строки, которые вы хотите получить в результате запроса.

``` sql
OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]
```

`offset_row_count` или `fetch_row_count` может быть числом или литеральной константой. Если вы не задаете `fetch_row_count` явно, используется значение по умолчанию, равное 1.

`OFFSET` указывает количество строк, которые необходимо пропустить перед началом возврата строк из запроса.

`FETCH` указывает максимальное количество строк, которые могут быть получены в результате запроса.

Опция `ONLY` используется для возврата строк, которые следуют сразу же за строками, пропущенными секцией `OFFSET`. В этом случае `FETCH` — это альтернатива [LIMIT](../../../sql-reference/statements/select/limit.md). Например, следующий запрос

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

идентичен запросу

``` sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

Опция `WITH TIES` используется для возврата дополнительных строк, которые привязываются к последней в результате запроса. Например, если `fetch_row_count` имеет значение 5 и существуют еще 2 строки с такими же значениями столбцов, указанных в `ORDER BY`, что и у пятой строки результата, то финальный набор будет содержать 7 строк.

:::note Примечание
Секция `OFFSET` должна находиться перед секцией `FETCH`, если обе присутствуют.
:::
:::note Примечание
Общее количество пропущенных строк может зависеть также от настройки [offset](../../../operations/settings/settings.md#offset).
:::
## Примеры {#examples}

Входная таблица:

``` text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Использование опции `ONLY`:

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

Результат:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Использование опции `WITH TIES`:

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

Результат:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```
