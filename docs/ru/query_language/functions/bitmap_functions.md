# Функции для битмапов

## bitmapBuild {#bitmap_functions-bitmapbuild}

Создаёт битовый массив из массива целочисленных значений.

```sql
bitmapBuild(array)
```

**Параметры**

- `array` – массив типа `UInt*`.

**Пример**

```sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```
```text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```


## bitmapToArray

Преобразует битовый массив в массив целочисленных значений.

```sql
bitmapToArray(bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Проверяет вхождение элемента в битовый массив.

```sql
bitmapContains(haystack, needle)
```

**Параметры**

- `haystack` – [объект Bitmap](#bitmap_functions-bitmapbuild), в котором функция ищет значение.
- `needle` – значение, которое функция ищет. Тип — [UInt32](../../data_types/int_uint.md).

**Возвращаемые значения**

- 0 — если в `haystack` нет `needle`.
- 1 — если в `haystack` есть `needle`.

Тип — `UInt8`.

**Пример**

```sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```
```text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny

Проверяет, имеют ли два битовых массива хотя бы один общий элемент.

```sql
bitmapHasAny(bitmap1, bitmap2)
```

Если вы уверены, что `bitmap2` содержит строго один элемент, используйте функцию [bitmapContains](#bitmap_functions-bitmapcontains). Она работает эффективнее.

**Параметры**

- `bitmap*` – массив любого типа с набором элементов.

**Возвращаемые значения**

- `1`, если `bitmap1` и `bitmap2` имеют хотя бы один одинаковый элемент.
- `0`, в противном случае.

**Пример**

```sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll

Аналогично функции `hasAll(array, array)` возвращает 1 если первый битовый массив содержит все элементы второго, 0 в противном случае.
Если второй аргумент является пустым битовым массивом, то возвращает 1.

```sql
bitmapHasAll(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```text
┌─res─┐
│  0  │
└─────┘
```

## bitmapAnd

Логическое И для двух битовых массивов. Результат — новый битовый массив.

```sql
bitmapAnd(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr

Логическое ИЛИ для двух битовых массивов. Результат — новый битовый массив.

```sql
bitmapOr(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor

Логическое исключающее ИЛИ для двух битовых массивов. Результат — новый битовый массив.

```sql
bitmapXor(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot

Логическое отрицание И для двух битовых массивов. Результат — новый битовый массив.

```sql
bitmapAndnot(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapCardinality

Возвращает кардинальность битового массива в виде значения типа `UInt64`.

```sql
bitmapCardinality(bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```text
┌─res─┐
│   5 │
└─────┘
```

## bitmapAndCardinality

Выполняет логическое И и возвращает кардинальность (`UInt64`) результирующего битового массива.

```sql
bitmapAndCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality

Выполняет логическое ИЛИ и возвращает кардинальность (`UInt64`) результирующего битового массива.

```sql
bitmapOrCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality

Выполняет логическое исключающее ИЛИ и возвращает кардинальность (`UInt64`) результирующего битового массива.

```sql
bitmapXorCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality

Выполняет логическое отрицание И и возвращает кардинальность (`UInt64`) результирующего битового массива.

```sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битовый массив.

**Пример**

```sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```text
┌─res─┐
│   2 │
└─────┘
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/functions/bitmap_functions/) <!--hide-->
