---
sidebar_position: 63
sidebar_label: "Функции для работы с Nullable-аргументами"
---

# Функции для работы с Nullable-аргументами {#funktsii-dlia-raboty-s-nullable-argumentami}

## isNull {#isnull}

Проверяет является ли аргумент [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

Синоним: `ISNULL`.

**Аргументы**

-   `x` — значение с не составным типом данных.

**Возвращаемое значение**

-   `1`, если `x` — `NULL`.
-   `0`, если `x` — не `NULL`.

**Пример**

Входная таблица

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Запрос

``` sql
SELECT x FROM t_null WHERE isNull(y);
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

Проверяет не является ли аргумент [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**Аргументы**

-   `x` — значение с не составным типом данных.

**Возвращаемое значение**

-   `0`, если `x` — `NULL`.
-   `1`, если `x` — не `NULL`.

**Пример**

Входная таблица

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Запрос

``` sql
SELECT x FROM t_null WHERE isNotNull(y);
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## coalesce {#coalesce}

Последовательно слева-направо проверяет являются ли переданные аргументы `NULL` и возвращает первый не `NULL`.

``` sql
coalesce(x,...)
```

**Аргументы**

-   Произвольное количество параметров не составного типа. Все параметры должны быть совместимы по типу данных.

**Возвращаемые значения**

-   Первый не `NULL` аргумент.
-   `NULL`, если все аргументы — `NULL`.

**Пример**

Рассмотрим адресную книгу, в которой может быть указано несколько способов связи с клиентом.

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

Поля `mail` и `phone` имеют тип String, а поле `icq` — `UInt32`, его необходимо будет преобразовать в `String`.

Получим из адресной книги первый доступный способ связаться с клиентом:

``` sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook;
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

Возвращает альтернативное значение, если основной аргумент — `NULL`.

``` sql
ifNull(x,alt)
```

**Аргументы**

-   `x` — значение для проверки на `NULL`,
-   `alt` — значение, которое функция вернёт, если `x` — `NULL`.

**Возвращаемые значения**

-   Значение `x`, если `x` — не `NULL`.
-   Значение `alt`, если `x` — `NULL`.

**Пример**

``` sql
SELECT ifNull('a', 'b');
```

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

``` sql
SELECT ifNull(NULL, 'b');
```

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf {#nullif}

Возвращает `NULL`, если аргументы равны.

``` sql
nullIf(x, y)
```

**Аргументы**

`x`, `y` — значения для сравнивания. Они должны быть совместимых типов, иначе ClickHouse сгенерирует исключение.

**Возвращаемые значения**

-   `NULL`, если аргументы равны.
-   Значение `x`, если аргументы не равны.

**Пример**

``` sql
SELECT nullIf(1, 1);
```

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

``` sql
SELECT nullIf(1, 2);
```

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull {#assumenotnull}

Приводит значение типа [Nullable](../../sql-reference/functions/functions-for-nulls.md) к не `Nullable`, если значение не `NULL`.

``` sql
assumeNotNull(x)
```

**Аргументы**

-   `x` — исходное значение.

**Возвращаемые значения**

-   Исходное значение с не `Nullable` типом, если оно — не `NULL`.
-   Неспецифицированный результат, зависящий от реализации, если исходное значение — `NULL`.

**Пример**

Рассмотрим таблицу `t_null`.

``` sql
SHOW CREATE TABLE t_null;
```

``` text
┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Применим функцию `assumeNotNull` к столбцу `y`.

``` sql
SELECT assumeNotNull(y) FROM t_null;
```

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null;
```

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable {#tonullable}

Преобразует тип аргумента к `Nullable`.

``` sql
toNullable(x)
```

**Аргументы**

-   `x` — значение произвольного не составного типа.

**Возвращаемое значение**

-   Входное значение с типом не `Nullable`.

**Пример**

``` sql
SELECT toTypeName(10);
```

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

``` sql
SELECT toTypeName(toNullable(10));
```

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

