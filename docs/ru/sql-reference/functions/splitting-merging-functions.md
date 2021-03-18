---
toc_priority: 47
toc_title: "Функции разбиения и слияния строк и массивов"
---

# Функции разбиения и слияния строк и массивов {#funktsii-razbieniia-i-sliianiia-strok-i-massivov}

## splitByChar(separator, s) {#splitbycharseparator-s}

Разбивает строку на подстроки, используя в качестве разделителя `separator`.
separator должен быть константной строкой из ровно одного символа.
Возвращается массив выделенных подстрок. Могут выделяться пустые подстроки, если разделитель идёт в начале или в конце строки, или если идёт более одного разделителя подряд.

**Синтаксис**

``` sql
splitByChar(<separator>, <s>)
```

**Аргументы**

-   `separator` — разделитель, состоящий из одного символа. [String](../../sql-reference/data-types/string.md).
-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив подстрок. Пустая подстрока, может быть возвращена, когда:

-   Разделитель находится в начале или конце строки;
-   Задано несколько последовательных разделителей;
-   Исходная строка `s` пуста.

Type: [Array](../../sql-reference/data-types/array.md) of [String](../../sql-reference/data-types/string.md).

**Пример**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString(separator, s) {#splitbystringseparator-s}

Разбивает строку на подстроки, разделенные строкой. В качестве разделителя использует константную строку `separator`, которая может состоять из нескольких символов. Если строка `separator` пуста, то функция разделит строку `s` на массив из символов.

**Синтаксис**

``` sql
splitByString(separator, s)
```

**Аргументы**

-   `separator` — разделитель. [String](../../sql-reference/data-types/string.md).
-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив подстрок. Пустая подстрока, может быть возвращена, когда:

-   Разделитель находится в начале или конце строки;
-   Задано несколько последовательных разделителей;
-   Исходная строка `s` пуста.

Тип: [Array](../../sql-reference/data-types/array.md) of [String](../../sql-reference/data-types/string.md).

**Примеры**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde')
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```


## arrayStringConcat(arr\[, separator\]) {#arraystringconcatarr-separator}

Склеивает строки, перечисленные в массиве, с разделителем separator.
separator - необязательный параметр, константная строка, по умолчанию равен пустой строке.
Возвращается строка.

## alphaTokens(s) {#alphatokenss}

Выделяет подстроки из подряд идущих байт из диапазонов a-z и A-Z.
Возвращается массив выделенных подстрок.

**Пример:**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

