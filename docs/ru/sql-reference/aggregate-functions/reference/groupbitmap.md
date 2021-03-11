---
toc_priority: 128
---

# groupBitmap {#groupbitmap}

Bitmap или агрегатные вычисления для столбца с типом данных `UInt*`, возвращают кардинальность в виде значения типа UInt64, если добавить суффикс -State, то возвращают [объект bitmap](../../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Параметры**

`expr` – выражение, результат которого имеет тип данных `UInt*`.

**Возвращаемое значение**

Значение типа `UInt64`.

**Пример**

Тестовые данные:

``` text
UserID
1
1
2
3
```

Запрос:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Результат:

``` text
num
3
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmap/) <!--hide-->
