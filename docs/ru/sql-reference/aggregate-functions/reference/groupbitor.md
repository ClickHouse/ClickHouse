---
toc_priority: 126
---

# groupBitOr {#groupbitor}

Применяет побитовое `ИЛИ` для последовательности чисел.

``` sql
groupBitOr(expr)
```

**Параметры**

`expr` – выражение, результат которого имеет тип данных `UInt*`.

**Возвращаемое значение**

Значение типа `UInt*`.

**Пример**

Тестовые данные:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Запрос:

``` sql
SELECT groupBitOr(num) FROM t
```

Где `num` — столбец с тестовыми данными.

Результат:

``` text
binary     decimal
01111101 = 125
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitor/) <!--hide-->
