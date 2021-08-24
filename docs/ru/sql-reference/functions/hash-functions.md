---
toc_priority: 50
toc_title: "Функции хэширования"
---

# Функции хэширования {#funktsii-kheshirovaniia}

Функции хэширования могут использоваться для детерминированного псевдослучайного разбрасывания элементов.

Simhash – это хеш-функция, которая для близких значений возвращает близкий хеш.

## halfMD5 {#hash-functions-halfmd5}

[Интерпретирует](../../sql-reference/functions/hash-functions.md#type_conversion_functions-reinterpretAsString) все входные параметры как строки и вычисляет хэш [MD5](https://ru.wikipedia.org/wiki/MD5) для каждой из них. Затем объединяет хэши, берет первые 8 байт хэша результирующей строки и интерпретирует их как значение типа `UInt64` с big-endian порядком байтов.

``` sql
halfMD5(par1, ...)
```

Функция относительно медленная (5 миллионов коротких строк в секунду на ядро процессора).
По возможности, используйте функцию [sipHash64](#hash_functions-siphash64) вместо неё.

**Аргументы**

Функция принимает переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type;
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

Вычисляет MD5 от строки и возвращает полученный набор байт в виде FixedString(16).
Если вам не нужен конкретно MD5, а нужен неплохой криптографический 128-битный хэш, то используйте вместо этого функцию sipHash128.
Если вы хотите получить такой же результат, как выдаёт утилита md5sum, напишите lower(hex(MD5(s))).

## sipHash64 {#hash_functions-siphash64}

Генерирует 64-х битное значение [SipHash](https://131002.net/siphash/).

``` sql
sipHash64(par1,...)
```

Это криптографическая хэш-функция. Она работает по крайней мере в три раза быстрее, чем функция [MD5](#hash_functions-md5).

Функция [интерпретирует](../../sql-reference/functions/hash-functions.md#type_conversion_functions-reinterpretAsString) все входные параметры как строки и вычисляет хэш MD5 для каждой из них. Затем комбинирует хэши по следующему алгоритму.

1.  После хэширования всех входных параметров функция получает массив хэшей.
2.  Функция принимает первый и второй элементы и вычисляет хэш для массива из них.
3.  Затем функция принимает хэш-значение, вычисленное на предыдущем шаге, и третий элемент исходного хэш-массива, и вычисляет хэш для массива из них.
4.  Предыдущий шаг повторяется для всех остальных элементов исходного хэш-массива.

**Аргументы**

Функция принимает переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash_functions-siphash128}

Вычисляет SipHash от строки.
Принимает аргумент типа String. Возвращает FixedString(16).
Отличается от sipHash64 тем, что финальный xor-folding состояния делается только до 128 бит.

## cityHash64 {#cityhash64}

Генерирует 64-х битное значение [CityHash](https://github.com/google/cityhash).

``` sql
cityHash64(par1,...)
```

Это не криптографическая хэш-функция. Она использует CityHash алгоритм для строковых параметров и зависящую от реализации быструю некриптографическую хэш-функцию для параметров с другими типами данных. Функция использует комбинатор CityHash для получения конечных результатов.

**Аргументы**

Функция принимает переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Примеры**

Пример вызова:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

А вот так вы можете вычислить чексумму всей таблицы с точностью до порядка строк:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

Вычисляет 32-битный хэш-код от целого числа любого типа.
Это сравнительно быстрая не криптографическая хэш-функция среднего качества для чисел.

## intHash64 {#inthash64}

Вычисляет 64-битный хэш-код от целого числа любого типа.
Работает быстрее, чем intHash32. Качество среднее.

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

Вычисляет SHA-1, SHA-224, SHA-256 от строки и возвращает полученный набор байт в виде FixedString(20), FixedString(28), FixedString(32).
Функция работает достаточно медленно (SHA-1 - примерно 5 миллионов коротких строк в секунду на одном процессорном ядре, SHA-224 и SHA-256 - примерно 2.2 миллионов).
Рекомендуется использовать эти функции лишь в тех случаях, когда вам нужна конкретная хэш-функция и вы не можете её выбрать.
Даже в этих случаях, рекомендуется применять функцию оффлайн - заранее вычисляя значения при вставке в таблицу, вместо того, чтобы применять её при SELECT-ах.

## URLHash(url\[, N\]) {#urlhashurl-n}

Быстрая не криптографическая хэш-функция неплохого качества для строки, полученной из URL путём некоторой нормализации.
`URLHash(s)` - вычислить хэш от строки без одного завершающего символа `/`, `?` или `#` на конце, если там такой есть.
`URLHash(s, N)` - вычислить хэш от строки до N-го уровня в иерархии URL, без одного завершающего символа `/`, `?` или `#` на конце, если там такой есть.
Уровни аналогичные URLHierarchy. Функция специфична для Яндекс.Метрики.

## farmFingerprint64 {#farmfingerprint64}

## farmHash64 {#farmhash64}

Создает 64-битное значение [FarmHash](https://github.com/google/farmhash), независимое от платформы (архитектуры сервера), что важно, если значения сохраняются или используются для разбиения данных на группы.

``` sql
farmFingerprint64(par1, ...)
farmHash64(par1, ...)
```

Эти функции используют методы `Fingerprint64` и `Hash64` из всех [доступных методов](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Аргументы**

Функция принимает переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Вычисляет [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) от строки. `JavaHash` не отличается ни скоростью, ни качеством, поэтому эту функцию следует считать устаревшей. Используйте эту функцию, если вам необходимо получить значение хэша по такому же алгоритму.

``` sql
SELECT javaHash('')
```

**Возвращаемое значение**

Хэш-значение типа `Int32`.

Тип: `javaHash`.

**Пример**

Запрос:

``` sql
SELECT javaHash('Hello, world!');
```

Результат:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

Вычисляет [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) от строки, при допущении, что строка представлена в кодировке `UTF-16LE`.

**Синтаксис**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**Аргументы**

-   `stringUtf16le` — строка в `UTF-16LE`.

**Возвращаемое значение**

Хэш-значение типа `Int32`.

Тип: `javaHash`.

**Пример**

Верный запрос для строки кодированной в `UTF-16LE`.

Запрос:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'));
```

Результат:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

Вычисляет `HiveHash` от строки.

``` sql
SELECT hiveHash('')
```

`HiveHash` — это результат [JavaHash](#hash_functions-javahash) с обнулённым битом знака числа. Функция используется в [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) вплоть до версии 3.0.

**Возвращаемое значение**

Хэш-значение типа `Int32`.

Тип: `hiveHash`.

**Пример**

Запрос:

``` sql
SELECT hiveHash('Hello, world!');
```

Результат:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64 {#metrohash64}

Генерирует 64-х битное значение [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/).

``` sql
metroHash64(par1, ...)
```

**Аргументы**

Функция принимает переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

Вычисляет JumpConsistentHash от значения типа UInt64.
Имеет два параметра: ключ типа UInt64 и количество бакетов. Возвращает значение типа Int32.
Дополнительные сведения смотрите по ссылке: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32, murmurHash2_64 {#murmurhash2-32-murmurhash2-64}

Генерирует значение [MurmurHash2](https://github.com/aappleby/smhasher).

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Аргументы**

Обе функции принимают переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

-   Функция `murmurHash2_32` возвращает значение типа [UInt32](../../sql-reference/functions/hash-functions.md).
-   Функция `murmurHash2_64` возвращает значение типа [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash {#gccmurmurhash}

Вычисляет 64-битное значение [MurmurHash2](https://github.com/aappleby/smhasher), используя те же hash seed, что и [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191).

**Синтаксис**

``` sql
gccMurmurHash(par1, ...);
```

**Аргументы**

-   `par1, ...` — переменное число параметров. Каждый параметр может быть любого из [поддерживаемых типов данных](../../sql-reference/data-types/index.md).

**Возвращаемое значение**

-   Вычисленный хэш-код.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

Результат:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## murmurHash3_32, murmurHash3_64 {#murmurhash3-32-murmurhash3-64}

Генерирует значение [MurmurHash3](https://github.com/aappleby/smhasher).

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Аргументы**

Обе функции принимают переменное число входных параметров. Аргументы могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

-   Функция `murmurHash3_32` возвращает значение типа [UInt32](../../sql-reference/functions/hash-functions.md).
-   Функция `murmurHash3_64` возвращает значение типа [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3_128 {#murmurhash3-128}

Генерирует значение [MurmurHash3](https://github.com/aappleby/smhasher).

``` sql
murmurHash3_128( expr )
```

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Хэш-значение типа [FixedString(16)](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT hex(murmurHash3_128('example_string')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;
```

``` text
┌─MurmurHash3──────────────────────┬─type───┐
│ 368A1A311CB7342253354B548E7E7E71 │ String │
└──────────────────────────────────┴────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32-xxhash64}

Вычисляет `xxHash` от строки. Предлагается в двух вариантах: 32 и 64 бита.

``` sql
SELECT xxHash32('')

OR

SELECT xxHash64('')
```

**Возвращаемое значение**

Хэш-значение типа `Uint32` или `Uint64`.

Тип: `xxHash`.

**Пример**

Запрос:

``` sql
SELECT xxHash32('Hello, world!');
```

Результат:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**Смотрите также**

-   [xxHash](http://cyan4973.github.io/xxHash/).

## ngramSimHash {#ngramsimhash}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммовый `simhash`. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
ngramSimHash(string[, ngramsize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT ngramSimHash('ClickHouse') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 1627567969 │
└────────────┘
```

## ngramSimHashCaseInsensitive {#ngramsimhashcaseinsensitive}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммовый `simhash`. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
ngramSimHashCaseInsensitive(string[, ngramsize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT ngramSimHashCaseInsensitive('ClickHouse') AS Hash;
```

Результат:

``` text
┌──────Hash─┐
│ 562180645 │
└───────────┘
```

## ngramSimHashUTF8 {#ngramsimhashutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммовый `simhash`. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
ngramSimHashUTF8(string[, ngramsize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT ngramSimHashUTF8('ClickHouse') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 1628157797 │
└────────────┘
```

## ngramSimHashCaseInsensitiveUTF8 {#ngramsimhashcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммовый `simhash`. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
ngramSimHashCaseInsensitiveUTF8(string[, ngramsize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT ngramSimHashCaseInsensitiveUTF8('ClickHouse') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 1636742693 │
└────────────┘
```

## wordShingleSimHash {#wordshinglesimhash}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и возвращает шингловый `simhash`. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
wordShingleSimHash(string[, shinglesize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT wordShingleSimHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitive {#wordshinglesimhashcaseinsensitive}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и возвращает шингловый `simhash`. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
wordShingleSimHashCaseInsensitive(string[, shinglesize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT wordShingleSimHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wordShingleSimHashUTF8 {#wordshinglesimhashutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и возвращает шингловый `simhash`. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
wordShingleSimHashUTF8(string[, shinglesize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT wordShingleSimHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitiveUTF8 {#wordshinglesimhashcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и возвращает шингловый `simhash`. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). Чем меньше [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между результатом вычисления `simhash` двух строк, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
wordShingleSimHashCaseInsensitiveUTF8(string[, shinglesize])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Значение хеш-функции от строки.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT wordShingleSimHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Результат:

``` text
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## ngramMinHash {#ngramminhash}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и вычисляет хеш для каждой n-граммы. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
ngramMinHash(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT ngramMinHash('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,9054248444481805918) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitive {#ngramminhashcaseinsensitive}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и вычисляет хеш для каждой n-граммы. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
ngramMinHashCaseInsensitive(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashCaseInsensitive('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────┐
│ (2106263556442004574,13203602793651726206) │
└────────────────────────────────────────────┘
```

## ngramMinHashUTF8 {#ngramminhashutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и вычисляет хеш для каждой n-граммы. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**
``` sql
ngramMinHashUTF8(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashUTF8('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,6742163577938632877) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitiveUTF8 {#ngramminhashcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и вычисляет хеш для каждой n-граммы. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
ngramMinHashCaseInsensitiveUTF8(string [, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple───────────────────────────────────────┐
│ (12493625717655877135,13203602793651726206) │
└─────────────────────────────────────────────┘
```

## ngramMinHashArg {#ngramminhasharg}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммы с минимальным и максимальным хешами, вычисленными функцией [ngramMinHash](#ngramminhash) с теми же входными данными. Функция регистрозависимая.

**Синтаксис**

``` sql
ngramMinHashArg(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` n-грамм.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashArg('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('Hou','lic','ick','ous','ckH','Cli')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitive {#ngramminhashargcaseinsensitive}

Выделяет из ASCII строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммы с минимальным и максимальным хешами, вычисленными функцией [ngramMinHashCaseInsensitive](#ngramminhashcaseinsensitive) с теми же входными данными. Функция регистро**не**зависимая.

**Синтаксис**

``` sql
ngramMinHashArgCaseInsensitive(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` n-грамм.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashArgCaseInsensitive('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','kHo','use','Cli'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgUTF8 {#ngramminhashargutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммы с минимальным и максимальным хешами, вычисленными функцией [ngramMinHashUTF8](#ngramminhashutf8) с теми же входными данными. Функция регистрозависимая.

**Синтаксис**

``` sql
ngramMinHashArgUTF8(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` n-грамм.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashArgUTF8('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('kHo','Hou','lic','ick','ous','ckH')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitiveUTF8 {#ngramminhashargcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (n-граммы) размером `ngramsize` символов и возвращает n-граммы с минимальным и максимальным хешами, вычисленными функцией [ngramMinHashCaseInsensitiveUTF8](#ngramminhashcaseinsensitiveutf8) с теми же входными данными. Функция регистро**не**зависимая.

**Синтаксис**

``` sql
ngramMinHashArgCaseInsensitiveUTF8(string[, ngramsize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — размер n-грамм. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` n-грамм.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT ngramMinHashArgCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ckH','ous','ick','lic','kHo','use'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHash {#wordshingleminhash}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и вычисляет хеш для каждого шингла. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
wordShingleMinHash(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitive {#wordshingleminhashcaseinsensitive}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и вычисляет хеш для каждого шингла. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
wordShingleMinHashCaseInsensitive(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashUTF8 {#wordshingleminhashutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и вычисляет хеш для каждого шингла. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистрозависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
wordShingleMinHashUTF8(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitiveUTF8 {#wordshingleminhashcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и вычисляет хеш для каждого шингла. Использует `hashnum` минимальных хешей, чтобы вычислить минимальный хеш, и `hashnum` максимальных хешей, чтобы вычислить максимальный хеш. Возвращает кортеж из этих хешей. Функция регистро**не**зависимая.

Может быть использована для проверки двух строк на схожесть вместе с функцией [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). Если для двух строк минимальные или максимальные хеши одинаковы, мы считаем, что эти строки совпадают.

**Синтаксис**

``` sql
wordShingleMinHashCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж с двумя хешами — минимальным и максимальным.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashArg {#wordshingleminhasharg}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и возвращает шинглы с минимальным и максимальным хешами, вычисленными функцией [wordshingleMinHash](#wordshingleminhash) с теми же входными данными. Функция регистрозависимая.

**Синтаксис**

``` sql
wordShingleMinHashArg(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` шинглов.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashArg('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitive {#wordshingleminhashargcaseinsensitive}

Выделяет из ASCII строки отрезки (шинглы) из `shinglesize` слов и возвращает шинглы с минимальным и максимальным хешами, вычисленными функцией [wordShingleMinHashCaseInsensitive](#wordshingleminhashcaseinsensitive) с теми же входными данными. Функция регистро**не**зависимая.

**Синтаксис**

``` sql
wordShingleMinHashArgCaseInsensitive(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` шинглов.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashArgCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgUTF8 {#wordshingleminhashargutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и возвращает шинглы с минимальным и максимальным хешами, вычисленными функцией [wordShingleMinHashUTF8](#wordshingleminhashutf8) с теми же входными данными. Функция регистрозависимая.

**Синтаксис**

``` sql
wordShingleMinHashArgUTF8(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` шинглов.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashArgUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Результат:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitiveUTF8 {#wordshingleminhashargcaseinsensitiveutf8}

Выделяет из UTF-8 строки отрезки (шинглы) из `shinglesize` слов и возвращает шинглы с минимальным и максимальным хешами, вычисленными функцией [wordShingleMinHashCaseInsensitiveUTF8](#wordshingleminhashcaseinsensitiveutf8) с теми же входными данными. Функция регистро**не**зависимая.

**Синтаксис**

``` sql
wordShingleMinHashArgCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Аргументы**

-   `string` — строка. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — размер словесных шинглов. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — количество минимальных и максимальных хешей, которое используется при вычислении результата. Необязательный. Возможные значения: любое число от `1` до `25`. Значение по умолчанию: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Кортеж из двух кортежей, каждый из которых состоит из `hashnum` шинглов.

Тип: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Пример**

Запрос:

``` sql
SELECT wordShingleMinHashArgCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Результат:

``` text
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```
