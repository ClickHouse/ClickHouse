# Функции разбиения и слияния строк и массивов {#funktsii-razbieniia-i-sliianiia-strok-i-massivov}

## splitByChar(separator, s) {#splitbycharseparator-s}

Разбивает строку на подстроки, используя в качестве разделителя separator.
separator должен быть константной строкой из ровно одного символа.
Возвращается массив выделенных подстрок. Могут выделяться пустые подстроки, если разделитель идёт в начале или в конце строки, или если идёт более одного разделителя подряд.

## splitByString(separator, s) {#splitbystringseparator-s}

То же самое, но использует строку из нескольких символов в качестве разделителя. Строка должна быть непустой.

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

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/splitting_merging_functions/) <!--hide-->
