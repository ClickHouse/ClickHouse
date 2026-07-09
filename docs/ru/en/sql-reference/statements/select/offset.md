---
description: 'Документация по OFFSET'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'Клауза OFFSET FETCH'
doc_type: 'reference'
---

`OFFSET` и `FETCH` позволяют получать данные порциями. Они задают блок строк, который нужно получить одним запросом.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

Значение `offset_row_count` или `fetch_row_count` может быть числом или литеральной константой. `fetch_row_count` можно не указывать; по умолчанию оно равно 1.

`OFFSET` задает количество строк, которые нужно пропустить, прежде чем возвращать строки из результирующего набора. `OFFSET n` пропускает первые `n` строк результата.

Поддерживается и отрицательный `OFFSET`: `OFFSET -n` пропускает последние `n` строк результата.

Также поддерживается дробный `OFFSET`: `OFFSET n` — если 0 &lt; n &lt; 1, то пропускаются первые n * 100% результата.

Пример:
• `OFFSET 0.1` - пропускает первые 10% результата.

> **Note**
> • Дробное значение должно быть числом [Float64](../../data-types/float.md), меньшим 1 и большим нуля.
> • Если в результате вычисления получается дробное количество строк, оно округляется вверх до следующего целого числа.

`FETCH` задает максимальное количество строк, которое может содержаться в результате запроса.

Опция `ONLY` используется для возврата строк, которые идут сразу после строк, пропущенных с помощью `OFFSET`. В этом случае `FETCH` служит альтернативой предложению [LIMIT](../../../sql-reference/statements/select/limit.md). Например, следующий запрос

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

совпадает с запросом

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

Опция `WITH TIES` используется, чтобы вернуть все дополнительные строки, которые делят последнее место в результирующем наборе согласно предложению `ORDER BY`. Например, если `fetch_row_count` задан равным 5, но ещё две строки совпадают по значениям столбцов `ORDER BY` с пятой строкой, результирующий набор будет содержать семь строк.

:::note
Согласно стандарту, предложение `OFFSET` должно идти перед предложением `FETCH`, если присутствуют оба.
:::

:::note
Фактическое смещение также может зависеть от настройки [offset](../../../operations/settings/settings.md#offset).
:::

<div id="examples">
  ## Примеры
</div>

Исходная таблица:

```text
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

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Использование опции `WITH TIES`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```