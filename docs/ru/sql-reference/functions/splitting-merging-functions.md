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

## extractAllGroups(text, regexp) {#extractallgroups}

Выделяет все группы из неперекрывающихся подстрок, которые соответствуют регулярному выражению.

**Синтаксис** 

``` sql
extractAllGroups(text, regexp) 
```

**Параметры** 

-   `text` — [String](../data-types/string.md) или [FixedString](../data-types/fixedstring.md).
-   `regexp` — Регулярное выражение. Константа. [String](../data-types/string.md) или [FixedString](../data-types/fixedstring.md).

**Возвращаемые значения**

-   Если найдена хотя бы одна подходящая группа, функция возвращает столбец вида `Array(Array(String))`, сгруппированный по идентификатору группы (от 1 до N, где N — количество групп с захватом содержимого в `regexp`).

-   Если подходящих групп не найдено, возвращает пустой массив.

Тип: [Array](../data-types/array.md).

**Пример использования**

Запрос:

``` sql
SELECT extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Результат:

``` text
┌─extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','123'],['8','"hkl"']]                                         │
└───────────────────────────────────────────────────────────────────────┘
```
[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/splitting_merging_functions/) <!--hide-->
