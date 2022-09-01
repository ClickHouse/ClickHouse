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
splitByChar(separator, s)
```

**Аргументы**

-   `separator` — разделитель, состоящий из одного символа. [String](../../sql-reference/data-types/string.md).
-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив подстрок. Пустая подстрока, может быть возвращена, когда:

-   Разделитель находится в начале или конце строки;
-   Задано несколько последовательных разделителей;
-   Исходная строка `s` пуста.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Пример**

``` sql
SELECT splitByChar(',', '1,2,3,abcde');
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

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Примеры**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde');
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde');
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByRegexp(regexp, s) {#splitbyregexpseparator-s}

Разбивает строку на подстроки, разделенные регулярным выражением. В качестве разделителя используется строка регулярного выражения `regexp`. Если `regexp` пустая, функция разделит строку `s` на массив одиночных символов. Если для регулярного выражения совпадения не найдено, строка `s` не будет разбита.

**Синтаксис**

``` sql
splitByRegexp(regexp, s)
```

**Аргументы**

-   `regexp` — регулярное выражение. Константа. [String](../data-types/string.md) или [FixedString](../data-types/fixedstring.md).
-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив выбранных подстрок. Пустая подстрока может быть возвращена, если:

-   Непустое совпадение с регулярным выражением происходит в начале или конце строки;
-   Имеется несколько последовательных совпадений c непустым регулярным выражением;
-   Исходная строка `s` пуста, а регулярное выражение не пустое.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Примеры**

Запрос:

``` sql
SELECT splitByRegexp('\\d+', 'a12bc23de345f');
```

Результат:

``` text
┌─splitByRegexp('\\d+', 'a12bc23de345f')─┐
│ ['a','bc','de','f']                    │
└────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT splitByRegexp('', 'abcde');
```

Результат:

``` text
┌─splitByRegexp('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByWhitespace(s) {#splitbywhitespaceseparator-s}

Разбивает строку на подстроки, используя в качестве разделителей пробельные символы.

**Синтаксис**

``` sql
splitByWhitespace(s)
```

**Аргументы**

-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив подстрок.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Пример**

``` sql
SELECT splitByWhitespace('  1!  a,  b.  ');
```

``` text
┌─splitByWhitespace('  1!  a,  b.  ')─┐
│ ['1!','a,','b.']                    │
└─────────────────────────────────────┘
```

## splitByNonAlpha(s) {#splitbynonalphaseparator-s}

Разбивает строку на подстроки, используя в качестве разделителей пробельные символы и символы пунктуации.

**Синтаксис**

``` sql
splitByNonAlpha(s)
```

**Аргументы**

-   `s` — разбиваемая строка. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив подстрок.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Пример**

``` sql
SELECT splitByNonAlpha('  1!  a,  b.  ');
```

``` text
┌─splitByNonAlpha('  1!  a,  b.  ')─┐
│ ['1','a','b']                     │
└───────────────────────────────────┘
```

## arrayStringConcat(arr\[, separator\]) {#arraystringconcatarr-separator}

Склеивает строковые представления элементов массива с разделителем `separator`.
`separator` - необязательный параметр, константная строка, по умолчанию равен пустой строке.
Возвращается строка.

## alphaTokens(s) {#alphatokenss}

Выделяет подстроки из подряд идущих байт из диапазонов a-z и A-Z.
Возвращается массив выделенных подстрок.

**Пример:**

``` sql
SELECT alphaTokens('abca1abc');
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

## ngrams {#ngrams}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов.

**Синтаксис** 

``` sql
ngrams(string, ngramsize)
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `ngramsize` — размер n-грамм. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Массив с n-граммами.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Пример**

Запрос:

``` sql
SELECT ngrams('ClickHouse', 3);
```

Результат:

``` text
┌─ngrams('ClickHouse', 3)───────────────────────────┐
│ ['Cli','lic','ick','ckH','kHo','Hou','ous','use'] │
└───────────────────────────────────────────────────┘
```

## tokens {#tokens}

Разбивает строку на  токены, используя в качестве разделителей не буквенно-цифровые символы ASCII.

**Аргументы**

-   `input_string` — набор байтов. [String](../../sql-reference/data-types/string.md).

**Возвращаемые значения**

Возвращает массив токенов.

Тип: [Array](../data-types/array.md).

**Пример**

Запрос:

``` sql
SELECT tokens('test1,;\\ test2,;\\ test3,;\\   test4') AS tokens;
```

Результат:

``` text
┌─tokens────────────────────────────┐
│ ['test1','test2','test3','test4'] │
└───────────────────────────────────┘
```
