---
sidebar_position: 49
sidebar_label: "Функции для битмапов"
---

# Функции для битовых масок {#bitmap-functions}

## bitmapBuild {#bitmap_functions-bitmapbuild}

Создаёт битовый массив из массива целочисленных значений.

``` sql
bitmapBuild(array)
```

**Аргументы**

-   `array` – массив типа `UInt*`.

**Пример**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res);
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

Преобразует битовый массив в массив целочисленных значений.

``` sql
bitmapToArray(bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetLimit {#bitmapsubsetlimit}

Создает подмножество битмапа с n элементами, расположенными между `range_start` и `cardinality_limit`.

**Синтаксис**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**Аргументы**

-   `bitmap` – битмап. [Bitmap object](#bitmap_functions-bitmapbuild).
-   `range_start` – начальная точка подмножества. [UInt32](../../sql-reference/functions/bitmap-functions.md#bitmap-functions).
-   `cardinality_limit` – верхний предел подмножества. [UInt32](../../sql-reference/functions/bitmap-functions.md#bitmap-functions).

**Возвращаемое значение**

Подмножество битмапа.

Тип: [Bitmap object](#bitmap_functions-bitmapbuild).

**Пример**

Запрос:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

Результат:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## subBitmap {#subbitmap}

Возвращает элементы битмапа, начиная с позиции `offset`. Число возвращаемых элементов ограничивается параметром `cardinality_limit`. Аналог строковой функции [substring](string-functions.md#substring)), но для битмапа.

**Синтаксис**

``` sql
subBitmap(bitmap, offset, cardinality_limit)
```

**Аргументы**

-   `bitmap` – битмап. Тип: [Bitmap object](#bitmap_functions-bitmapbuild).
-   `offset` – позиция первого элемента возвращаемого подмножества. Тип: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – максимальное число элементов возвращаемого подмножества. Тип: [UInt32](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

Подмножество битмапа.

Тип: [Bitmap object](#bitmap_functions-bitmapbuild).

**Пример**

Запрос:

``` sql
SELECT bitmapToArray(subBitmap(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(10), toUInt32(10))) AS res;
```

Результат:

``` text
┌─res─────────────────────────────┐
│ [10,11,12,13,14,15,16,17,18,19] │
└─────────────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Проверяет вхождение элемента в битовый массив.

``` sql
bitmapContains(haystack, needle)
```

**Аргументы**

-   `haystack` – [объект Bitmap](#bitmap_functions-bitmapbuild), в котором функция ищет значение.
-   `needle` – значение, которое функция ищет. Тип — [UInt32](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   0 — если в `haystack` нет `needle`.
-   1 — если в `haystack` есть `needle`.

Тип — `UInt8`.

**Пример**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res;
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

Проверяет, имеют ли два битовых массива хотя бы один общий элемент.

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

Если вы уверены, что `bitmap2` содержит строго один элемент, используйте функцию [bitmapContains](#bitmap_functions-bitmapcontains). Она работает эффективнее.

**Аргументы**

-   `bitmap*` – массив любого типа с набором элементов.

**Возвращаемые значения**

-   `1`, если `bitmap1` и `bitmap2` имеют хотя бы один одинаковый элемент.
-   `0`, в противном случае.

**Пример**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

Аналогично функции `hasAll(array, array)` возвращает 1 если первый битовый массив содержит все элементы второго, 0 в противном случае.
Если второй аргумент является пустым битовым массивом, то возвращает 1.

``` sql
bitmapHasAll(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapAnd {#bitmapand}

Логическое И для двух битовых массивов. Результат — новый битовый массив.

``` sql
bitmapAnd(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

Логическое ИЛИ для двух битовых массивов. Результат — новый битовый массив.

``` sql
bitmapOr(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

Логическое исключающее ИЛИ для двух битовых массивов. Результат — новый битовый массив.

``` sql
bitmapXor(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot {#bitmapandnot}

Логическое отрицание И для двух битовых массивов. Результат — новый битовый массив.

``` sql
bitmapAndnot(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapCardinality {#bitmapcardinality}

Возвращает кардинальность битового массива в виде значения типа `UInt64`.

``` sql
bitmapCardinality(bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapAndCardinality {#bitmapandcardinality}

Выполняет логическое И и возвращает кардинальность (`UInt64`) результирующего битового массива.

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality {#bitmaporcardinality}

Выполняет логическое ИЛИ и возвращает кардинальность (`UInt64`) результирующего битового массива.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

Выполняет логическое исключающее ИЛИ и возвращает кардинальность (`UInt64`) результирующего битового массива.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality {#bitmapandnotcardinality}

Выполняет логическое отрицание И и возвращает кардинальность (`UInt64`) результирующего битового массива.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Аргументы**

-   `bitmap` – битовый массив.

**Пример**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

