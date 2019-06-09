# Функции для битмапов

## bitmapBuild

Создаёт битмап из массива целочисленных значений.

```
bitmapBuild(array)
```

**Параметры**

- `array` – массив типа `UInt*`.

**Пример**

```sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res
```

## bitmapToArray

Преобразует битмап в массив целочисленных значений.

```
bitmapToArray(bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapHasAny

Аналогично функции `hasAny(array, array)` возвращает 1 если два битмапа содержат одинаковые элементы, 0 в противном случае.
Для пустых битмапов возвращает 0.

```
bitmapHasAny(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll

Аналогично функции `hasAll(array, array)` возвращает 1 если первый битмап содержит все элементы второго, 0 в противном случае.
Если второй аргумент является пустым битмапом, то возвращает 1.

```
bitmapHasAll(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```
┌─res─┐
│  0  │
└─────┘
```

## bitmapAnd

Логическое И для двух битмапов. Результат — новый битмап.

```
bitmapAnd(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr

Логическое ИЛИ для двух битмапов. Результат — новый битмап.

```
bitmapOr(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor

Логическое исключающее ИЛИ для двух битмапов. Результат — новый битмап.

```
bitmapXor(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot

Логическое отрицание И для двух битмапов. Результат — новый битмап.

```
bitmapAndnot(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapCardinality

Возвращает кардинальность битмапа в виде значения типа `UInt64`.

```
bitmapCardinality(bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─┐
│   5 │
└─────┘
```

## bitmapAndCardinality

Выполняет логическое И и возвращает кардинальность (`UInt64`) результирующего битмапа.

```
bitmapAndCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality

Выполняет логическое ИЛИ и возвращает кардинальность (`UInt64`) результирующего битмапа.

```
bitmapOrCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality

Выполняет логическое исключающее ИЛИ и возвращает кардинальность (`UInt64`) результирующего битмапа.

```
bitmapXorCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality

Выполняет логическое отрицание И и возвращает кардинальность (`UInt64`) результирующего битмапа.

```
bitmapAndnotCardinality(bitmap,bitmap)
```

**Параметры**

- `bitmap` – битмап.

**Пример**

```sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   2 │
└─────┘
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/functions/bitmap_functions/) <!--hide-->
