---
slug: /ru/sql-reference/functions/string-search-functions
sidebar_position: 41
sidebar_label: "Функции поиска в строках"
---

# Функции поиска в строках {#funktsii-poiska-v-strokakh}

Во всех функциях, поиск регистрозависимый по умолчанию. Существуют варианты функций для регистронезависимого поиска.

## position(haystack, needle), locate(haystack, needle) {#position}

Поиск подстроки `needle` в строке `haystack`.

Возвращает позицию (в байтах) найденной подстроки в строке, начиная с 1, или 0, если подстрока не найдена.

Для поиска без учета регистра используйте функцию [positionCaseInsensitive](#positioncaseinsensitive).

**Синтаксис**

``` sql
position(haystack, needle[, start_pos])
```

``` sql
position(needle IN haystack)
```

Алиас: `locate(haystack, needle[, start_pos])`.

:::note Примечание
Синтаксис `position(needle IN haystack)` обеспечивает совместимость с SQL, функция работает так же, как `position(haystack, needle)`.
:::

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [Строка](../syntax.md#syntax-string-literal).
-   `needle` — подстрока, которую необходимо найти. [Строка](../syntax.md#syntax-string-literal).
-   `start_pos` — опциональный параметр, позиция символа в строке, с которого начинается поиск. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Начальная позиция в байтах (начиная с 1), если подстрока найдена.
-   0, если подстрока не найдена.

Тип: `Integer`.

**Примеры**

Фраза «Hello, world!» содержит набор байт, представляющий текст в однобайтовой кодировке. Функция возвращает ожидаемый результат:

Запрос:

``` sql
SELECT position('Hello, world!', '!');
```

Результат:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

Аналогичная фраза на русском содержит символы, которые не могут быть представлены в однобайтовой кодировке. Функция возвращает неожиданный результат (используйте функцию [positionUTF8](#positionutf8) для символов, которые не могут быть представлены одним байтом):

Запрос:

``` sql
SELECT position('Привет, мир!', '!');
```

Результат:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

**Примеры работы функции с синтаксисом POSITION(needle IN haystack)**

Запрос:

```sql
SELECT 1 = position('абв' IN 'абв');
```

Результат:

```text
┌─equals(1, position('абв', 'абв'))─┐
│                                 1 │
└───────────────────────────────────┘
```

Запрос:

```sql
SELECT 0 = position('абв' IN '');
```

Результат:

```text
┌─equals(0, position('', 'абв'))─┐
│                              1 │
└────────────────────────────────┘
```

## positionCaseInsensitive {#positioncaseinsensitive}

Такая же, как и [position](#position), но работает без учета регистра. Возвращает позицию в байтах найденной подстроки в строке, начиная с 1.

Работает при допущении, что строка содержит набор байт, представляющий текст в однобайтовой кодировке. Если допущение не выполнено — то возвращает неопределенный результат (не кидает исключение). Если символ может быть представлен с помощью двух байтов, он будет представлен двумя байтами и так далее.

**Синтаксис**

``` sql
positionCaseInsensitive(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [Строка](../syntax.md#syntax-string-literal).
-   `needle` — подстрока, которую необходимо найти. [Строка](../syntax.md#syntax-string-literal).
-   `start_pos` — опциональный параметр, позиция символа в строке, с которого начинается поиск. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Начальная позиция в байтах (начиная с 1), если подстрока найдена.
-   0, если подстрока не найдена.

Тип: `Integer`.

**Пример**

Запрос:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello');
```

Результат:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8 {#positionutf8}

Возвращает позицию (в кодовых точках Unicode) найденной подстроки в строке, начиная с 1.

Работает при допущении, что строка содержит набор кодовых точек, представляющий текст в кодировке UTF-8. Если допущение не выполнено — то возвращает неопределенный результат (не кидает исключение). Если символ может быть представлен с помощью двух кодовых точек, он будет представлен двумя и так далее.

Для поиска без учета регистра используйте функцию [positionCaseInsensitiveUTF8](#positioncaseinsensitiveutf8).

**Синтаксис**

``` sql
positionUTF8(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [Строка](../syntax.md#syntax-string-literal).
-   `needle` — подстрока, которую необходимо найти. [Строка](../syntax.md#syntax-string-literal).
-   `start_pos` — опциональный параметр, позиция символа в строке, с которого начинается поиск. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Начальная позиция в кодовых точках Unicode (начиная с 1), если подстрока найдена.
-   0, если подстрока не найдена.

Тип: `Integer`.

**Примеры**

Фраза «Привет, мир!» содержит набор символов, каждый из которых можно представить с помощью одной кодовой точки. Функция возвращает ожидаемый результат:

Запрос:

``` sql
SELECT positionUTF8('Привет, мир!', '!');
```

Результат:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

Фраза «Salut, étudiante!» содержит символ `é`, который может быть представлен одной кодовой точкой (`U+00E9`) или двумя (`U+0065U+0301`). Поэтому функция `positionUTF8()` может вернуть неожиданный результат:

Запрос для символа `é`, который представлен одной кодовой точкой `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!');
```

Result:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

Запрос для символа `é`, который представлен двумя кодовыми точками `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!');
```

Результат:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## positionCaseInsensitiveUTF8 {#positioncaseinsensitiveutf8}

Такая же, как и [positionUTF8](#positionutf8), но работает без учета регистра. Возвращает позицию (в кодовых точках Unicode) найденной подстроки в строке, начиная с 1.

Работает при допущении, что строка содержит набор кодовых точек, представляющий текст в кодировке UTF-8. Если допущение не выполнено — то возвращает неопределенный результат (не кидает исключение). Если символ может быть представлен с помощью двух кодовых точек, он будет представлен двумя и так далее.

**Синтаксис**

``` sql
positionCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [Строка](../syntax.md#syntax-string-literal).
-   `needle` — подстрока, которую необходимо найти. [Строка](../syntax.md#syntax-string-literal).
-   `start_pos` — опциональный параметр, позиция символа в строке, с которого начинается поиск. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Начальная позиция в байтах (начиная с 1), если подстрока найдена.
-   0, если подстрока не найдена.

Тип: `Integer`.

**Пример**

Запрос:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир');
```

Результат:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## multiSearchAllPositions {#multisearchallpositions}

The same as [position](#position) but returns `Array` of positions (in bytes) of the found corresponding substrings in the string. Positions are indexed starting from 1.

The search is performed on sequences of bytes without respect to string encoding and collation.

-   For case-insensitive ASCII search, use the function `multiSearchAllPositionsCaseInsensitive`.
-   For search in UTF-8, use the function [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   For case-insensitive UTF-8 search, use the function multiSearchAllPositionsCaseInsensitiveUTF8.

**Syntax**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../syntax.md#syntax-string-literal).

**Returned values**

-   Array of starting positions in bytes (counting from 1), if the corresponding substring was found and 0 if not found.

**Example**

Query:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);
```

Result:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8 {#multiSearchAllPositionsUTF8}

Смотрите `multiSearchAllPositions`.

## multiSearchFirstPosition(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>\]) {#multisearchfirstpositionhaystack-needle1-needle2-needlen}

Так же, как и `position`, только возвращает оффсет первого вхождения любого из needles.

Для поиска без учета регистра и/или в кодировке UTF-8 используйте функции `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstIndex(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

Возвращает индекс `i` (нумерация с единицы) первой найденной строки needle<sub>i</sub> в строке `haystack` и 0 иначе.

Для поиска без учета регистра и/или в кодировке UTF-8 используйте функции `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>\]) {#function-multisearchany}

Возвращает 1, если хотя бы одна подстрока needle<sub>i</sub> нашлась в строке `haystack` и 0 иначе.

Для поиска без учета регистра и/или в кодировке UTF-8 используйте функции `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

:::note Примечание
Во всех функциях `multiSearch*` количество needles должно быть меньше 2<sup>8</sup> из-за особенностей реализации.
:::

## match(haystack, pattern) {#matchhaystack-pattern}

Проверка строки на соответствие регулярному выражению pattern. Регулярное выражение **re2**. Синтаксис регулярных выражений **re2** является более ограниченным по сравнению с регулярными выражениями **Perl** ([подробнее](https://github.com/google/re2/wiki/Syntax)).
Возвращает 0 (если не соответствует) или 1 (если соответствует).

Обратите внимание, что для экранирования в регулярном выражении, используется символ `\` (обратный слеш). Этот же символ используется для экранирования в строковых литералах. Поэтому, чтобы экранировать символ в регулярном выражении, необходимо написать в строковом литерале \\ (два обратных слеша).

Регулярное выражение работает со строкой как с набором байт. Регулярное выражение не может содержать нулевые байты.
Для шаблонов на поиск подстроки в строке, лучше используйте LIKE или position, так как они работают существенно быстрее.

## multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

То же, что и `match`, но возвращает ноль, если ни одно регулярное выражение не подошло и один, если хотя бы одно. Используется библиотека [hyperscan](https://github.com/intel/hyperscan) для соответствия регулярных выражений. Для шаблонов на поиск многих подстрок в строке, лучше используйте `multiSearchAny`, так как она работает существенно быстрее.

:::note Примечание
Длина любой строки из `haystack` должна быть меньше 2<sup>32</sup> байт, иначе бросается исключение. Это ограничение связано с ограничением hyperscan API.
:::
## multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

То же, что и `multiMatchAny`, только возвращает любой индекс подходящего регулярного выражения.

## multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

То же, что и `multiMatchAny`, только возвращает массив всех индексов всех подходящих регулярных выражений в любом порядке.

## multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

То же, что и `multiMatchAny`, но возвращает 1 если любой шаблон соответствует haystack в пределах константного [редакционного расстояния](https://en.wikipedia.org/wiki/Edit_distance). Эта функция основана на экспериментальной библиотеке [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) и может быть медленной для некоторых частных случаев. Производительность зависит от значения редакционного расстояния и используемых шаблонов, но всегда медленнее по сравнению с non-fuzzy вариантами.

## multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

То же, что и `multiFuzzyMatchAny`, только возвращает любой индекс подходящего регулярного выражения в пределах константного редакционного расстояния.

## multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

То же, что и `multiFuzzyMatchAny`, только возвращает массив всех индексов всех подходящих регулярных выражений в любом порядке в пределах константного редакционного расстояния.

:::note Примечание
`multiFuzzyMatch*` функции не поддерживают UTF-8 закодированные регулярные выражения, и такие выражения рассматриваются как байтовые из-за ограничения hyperscan.
:::

:::note Примечание
Чтобы выключить все функции, использующие hyperscan, используйте настройку `SET allow_hyperscan = 0;`.
:::
## extract(haystack, pattern) {#extracthaystack-pattern}

Извлечение фрагмента строки по регулярному выражению. Если haystack не соответствует регулярному выражению pattern, то возвращается пустая строка. Если регулярное выражение не содержит subpattern-ов, то вынимается фрагмент, который подпадает под всё регулярное выражение. Иначе вынимается фрагмент, который подпадает под первый subpattern.

## extractAll(haystack, pattern) {#extractallhaystack-pattern}

Извлечение всех фрагментов строки по регулярному выражению. Если haystack не соответствует регулярному выражению pattern, то возвращается пустая строка. Возвращается массив строк, состоящий из всех соответствий регулярному выражению. В остальном, поведение аналогично функции extract (по прежнему, вынимается первый subpattern, или всё выражение, если subpattern-а нет).

## extractAllGroupsHorizontal {#extractallgroups-horizontal}

Разбирает строку `haystack` на фрагменты, соответствующие группам регулярного выражения `pattern`. Возвращает массив массивов, где первый массив содержит все фрагменты, соответствующие первой группе регулярного выражения, второй массив - соответствующие второй группе, и т.д.

:::note Замечание
Функция `extractAllGroupsHorizontal` работает медленнее, чем функция [extractAllGroupsVertical](#extractallgroups-vertical).
:::

**Синтаксис**

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**Аргументы**

-   `haystack` — строка для разбора. Тип: [String](../../sql-reference/data-types/string.md).
-   `pattern` — регулярное выражение, построенное по синтаксическим правилам [re2](https://github.com/google/re2/wiki/Syntax). Выражение должно содержать группы, заключенные в круглые скобки. Если выражение не содержит групп, генерируется исключение. Тип: [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Тип: [Array](../../sql-reference/data-types/array.md).

Если в строке `haystack` нет групп, соответствующих регулярному выражению `pattern`, возвращается массив пустых массивов.

**Пример**

Запрос:

``` sql
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Результат:

``` text
┌─extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','def','ghi'],['111','222','333']]                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   Функция [extractAllGroupsVertical](#extractallgroups-vertical)

## extractAllGroupsVertical {#extractallgroups-vertical}

Разбирает строку `haystack` на фрагменты, соответствующие группам регулярного выражения `pattern`. Возвращает массив массивов, где каждый массив содержит по одному фрагменту, соответствующему каждой группе регулярного выражения. Фрагменты группируются в массивы в соответствии с порядком появления в исходной строке.

**Синтаксис**

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**Аргументы**

-   `haystack` — строка для разбора. Тип: [String](../../sql-reference/data-types/string.md).
-   `pattern` — регулярное выражение, построенное по синтаксическим правилам [re2](https://github.com/google/re2/wiki/Syntax). Выражение должно содержать группы, заключенные в круглые скобки. Если выражение не содержит групп, генерируется исключение. Тип: [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Тип: [Array](../../sql-reference/data-types/array.md).

Если в строке `haystack` нет групп, соответствующих регулярному выражению `pattern`, возвращается пустой массив.

**Пример**

Запрос:

``` sql
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Результат:

``` text
┌─extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','111'],['def','222'],['ghi','333']]                                            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   Функция [extractAllGroupsHorizontal](#extractallgroups-horizontal)

## like(haystack, pattern), оператор haystack LIKE pattern {#function-like}

Проверка строки на соответствие простому регулярному выражению.
Регулярное выражение может содержать метасимволы `%` и `_`.

`%` обозначает любое количество любых байт (в том числе, нулевое количество символов).

`_` обозначает один любой байт.

Для экранирования метасимволов, используется символ `\` (обратный слеш). Смотрите замечание об экранировании в описании функции match.

Для регулярных выражений вида `%needle%` действует более оптимальный код, который работает также быстро, как функция `position`.
Для остальных регулярных выражений, код аналогичен функции match.

## notLike(haystack, pattern), оператор haystack NOT LIKE pattern {#function-notlike}

То же, что like, но с отрицанием.

## ilike {#ilike}

Нечувствительный к регистру вариант функции [like](https://clickhouse.com/docs/ru/sql-reference/functions/string-search-functions/#function-like). Вы можете использовать оператор `ILIKE` вместо функции `ilike`.

**Синтаксис**

``` sql
ilike(haystack, pattern)
```

**Аргументы**

-   `haystack` — входная строка. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `pattern` — если `pattern` не содержит процента или нижнего подчеркивания, тогда `pattern` представляет саму строку. Нижнее подчеркивание (`_`) в `pattern` обозначает любой отдельный символ. Знак процента (`%`) соответствует последовательности из любого количества символов: от нуля и более.

Некоторые примеры `pattern`:

``` text
'abc' ILIKE 'abc'    true
'abc' ILIKE 'a%'     true
'abc' ILIKE '_b_'    true
'abc' ILIKE 'c'      false
```

**Возвращаемые значения**

-   Правда, если строка соответствует `pattern`.
-   Ложь, если строка не соответствует `pattern`.

**Пример**

Входная таблица:

``` text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Запрос:

``` sql
SELECT * FROM Months WHERE ilike(name, '%j%');
```

Результат:

``` text
┌─id─┬─name────┬─days─┐
│  1 │ January │   31 │
└────┴─────────┴──────┘
```

**Смотрите также**



## ngramDistance(haystack, needle) {#ngramdistancehaystack-needle}

Вычисление 4-граммного расстояния между `haystack` и `needle`: считается симметрическая разность между двумя мультимножествами 4-грамм и нормализуется на сумму их мощностей. Возвращает число float от 0 до 1 – чем ближе к нулю, тем больше строки похожи друг на друга. Если константный `needle` или `haystack` больше чем 32КБ, кидается исключение. Если некоторые строки из неконстантного `haystack` или `needle` больше 32КБ, расстояние всегда равно единице.

Для поиска без учета регистра и/или в формате UTF-8 используйте функции `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramSearch(haystack, needle) {#ngramsearchhaystack-needle}

То же, что и `ngramDistance`, но вычисляет несимметричную разность между `needle` и `haystack` – количество n-грамм из `needle` минус количество общих n-грамм, нормированное на количество n-грамм из `needle`. Чем ближе результат к единице, тем вероятнее, что `needle` внутри `haystack`. Может быть использовано для приближенного поиска.

Для поиска без учета регистра и/или в формате UTF-8 используйте функции `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

:::note Примечание
Для случая UTF-8 мы используем триграммное расстояние. Вычисление n-граммного расстояния не совсем честное. Мы используем 2-х байтные хэши для хэширования n-грамм, а затем вычисляем (не)симметрическую разность между хэш таблицами – могут возникнуть коллизии. В формате UTF-8 без учета регистра мы не используем честную функцию `tolower` – мы обнуляем 5-й бит (нумерация с нуля) каждого байта кодовой точки, а также первый бит нулевого байта, если байтов больше 1 – это работает для латиницы и почти для всех кириллических букв.
:::

## countMatches(haystack, pattern) {#countmatcheshaystack-pattern}

Возвращает количество совпадений, найденных в строке `haystack`, для регулярного выражения `pattern`.

**Синтаксис**

``` sql
countMatches(haystack, pattern)
```

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `pattern` — регулярное выражение, построенное по синтаксическим правилам [re2](https://github.com/google/re2/wiki/Syntax). [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Количество совпадений.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT countMatches('foobar.com', 'o+');
```

Результат:

``` text
┌─countMatches('foobar.com', 'o+')─┐
│                                2 │
└──────────────────────────────────┘
```

Запрос:

``` sql
SELECT countMatches('aaaa', 'aa');
```

Результат:

``` text
┌─countMatches('aaaa', 'aa')────┐
│                             2 │
└───────────────────────────────┘
```

## countSubstrings {#countSubstrings}

Возвращает количество вхождений подстроки.

Для поиска без учета регистра, используйте функции [countSubstringsCaseInsensitive](../../sql-reference/functions/string-search-functions.md#countSubstringsCaseInsensitive) или [countSubstringsCaseInsensitiveUTF8](../../sql-reference/functions/string-search-functions.md#countSubstringsCaseInsensitiveUTF8)

**Синтаксис**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, в которой ведется поиск. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — искомая подстрока. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` — позиция первого символа в строке, с которого начнется поиск. Необязательный параметр. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Число вхождений.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT countSubstrings('foobar.com', '.');
```

Результат:

``` text
┌─countSubstrings('foobar.com', '.')─┐
│                                  1 │
└────────────────────────────────────┘
```

Запрос:

``` sql
SELECT countSubstrings('aaaa', 'aa');
```

Результат:

``` text
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
```

Запрос:

```sql
SELECT countSubstrings('abc___abc', 'abc', 4);
```

Результат:

``` text
┌─countSubstrings('abc___abc', 'abc', 4)─┐
│                                      1 │
└────────────────────────────────────────┘
```

## countSubstringsCaseInsensitive {#countSubstringsCaseInsensitive}

Возвращает количество вхождений подстроки без учета регистра.

**Синтаксис**

``` sql
countSubstringsCaseInsensitive(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, в которой ведется поиск. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — искомая подстрока. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` — позиция первого символа в строке, с которого начнется поиск. Необязательный параметр. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Число вхождений.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
select countSubstringsCaseInsensitive('aba', 'B');
```

Результат:

``` text
┌─countSubstringsCaseInsensitive('aba', 'B')─┐
│                                          1 │
└────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT countSubstringsCaseInsensitive('foobar.com', 'CoM');
```

Результат:

``` text
┌─countSubstringsCaseInsensitive('foobar.com', 'CoM')─┐
│                                                   1 │
└─────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT countSubstringsCaseInsensitive('abC___abC', 'aBc', 2);
```

Результат:

``` text
┌─countSubstringsCaseInsensitive('abC___abC', 'aBc', 2)─┐
│                                                     1 │
└───────────────────────────────────────────────────────┘
```

## countSubstringsCaseInsensitiveUTF8 {#countSubstringsCaseInsensitiveUTF8}

Возвращает количество вхождений подстроки в `UTF-8` без учета регистра.

**Синтаксис**

``` sql
SELECT countSubstringsCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**Аргументы**

-   `haystack` — строка, в которой ведется поиск. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — искомая подстрока. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` — позиция первого символа в строке, с которого начнется поиск. Необязательный параметр. [UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Число вхождений.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT countSubstringsCaseInsensitiveUTF8('абв', 'A');
```

Результат:

``` text
┌─countSubstringsCaseInsensitiveUTF8('абв', 'A')─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Запрос:

```sql
SELECT countSubstringsCaseInsensitiveUTF8('аБв__АбВ__абв', 'Абв');
```

Результат:

``` text
┌─countSubstringsCaseInsensitiveUTF8('аБв__АбВ__абв', 'Абв')─┐
│                                                          3 │
└────────────────────────────────────────────────────────────┘
```

## hasSubsequence(haystack, needle) {#hasSubsequence}

Возвращает 1 если needle является подпоследовательностью haystack, иначе 0.


**Синтаксис**

``` sql
hasSubsequence(haystack, needle)
```

**Аргументы**

-   `haystack` — строка, по которой выполняется поиск. [Строка](../syntax.md#syntax-string-literal).
-   `needle` — подпоследовательность, которую необходимо найти. [Строка](../syntax.md#syntax-string-literal).

**Возвращаемые значения**

-   1, если 
-   0, если подстрока не найдена.

Тип: `UInt8`.

**Примеры**

Запрос:

``` sql
SELECT hasSubsequence('garbage', 'arg') ;
```

Результат:

``` text
┌─hasSubsequence('garbage', 'arg')─┐
│                                1 │
└──────────────────────────────────┘
```


## hasSubsequenceCaseInsensitive

Такая же, как и [hasSubsequence](#hasSubsequence), но работает без учета регистра.

## hasSubsequenceUTF8

Такая же, как и [hasSubsequence](#hasSubsequence) при допущении что `haystack` и `needle` содержат набор кодовых точек, представляющий текст в кодировке UTF-8.

## hasSubsequenceCaseInsensitiveUTF8

Такая же, как и [hasSubsequenceUTF8](#hasSubsequenceUTF8), но работает без учета регистра.
