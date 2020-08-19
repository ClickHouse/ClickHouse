# Функции хэширования {#funktsii-kheshirovaniia}

Функции хэширования могут использоваться для детерминированного псевдослучайного разбрасывания элементов.

## halfMD5 {#hash-functions-halfmd5}

[Интерпретирует](../../sql-reference/functions/hash-functions.md#type_conversion_functions-reinterpretAsString) все входные параметры как строки и вычисляет хэш [MD5](https://ru.wikipedia.org/wiki/MD5) для каждой из них. Затем объединяет хэши, берет первые 8 байт хэша результирующей строки и интерпретирует их как значение типа `UInt64` с big-endian порядком байтов.

``` sql
halfMD5(par1, ...)
```

Функция относительно медленная (5 миллионов коротких строк в секунду на ядро процессора).
По возможности, используйте функцию [sipHash64](#hash_functions-siphash64) вместо неё.

**Параметры**

Функция принимает переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
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

**Параметры**

Функция принимает переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
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

**Параметры**

Функция принимает переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Примеры**

Пример вызова:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
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

## farmHash64 {#farmhash64}

Генерирует 64-х битное значение [FarmHash](https://github.com/google/farmhash).

``` sql
farmHash64(par1, ...)
```

Из всех [доступных методов](https://github.com/google/farmhash/blob/master/src/farmhash.h) функция использует `Hash64`.

**Параметры**

Функция принимает переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Вычисляет [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) от строки. `JavaHash` не отличается ни скоростью, ни качеством, поэтому эту функцию следует считать устаревшей. Используйте эту функцию, если вам необходимо получить значение хэша по такому же алгоритму.

``` sql
SELECT javaHash('');
```

**Возвращаемое значение**

Хэш-значение типа `Int32`.

Тип: `javaHash`.

**Пример**

Запрос:

``` sql
SELECT javaHash('Hello, world!');
```

Ответ:

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

**Параметры**

-   `stringUtf16le` — строка в `UTF-16LE`.

**Возвращаемое значение**

Хэш-значение типа `Int32`.

Тип: `javaHash`.

**Пример**

Верный запрос для строки кодированной в `UTF-16LE`.

Запрос:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

Ответ:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

Вычисляет `HiveHash` от строки.

``` sql
SELECT hiveHash('');
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

Ответ:

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

**Параметры**

Функция принимает переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Значение хэша с типом данных [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
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

## murmurHash2\_32, murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

Генерирует значение [MurmurHash2](https://github.com/aappleby/smhasher).

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Параметры**

Обе функции принимают переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

-   Функция `murmurHash2_32` возвращает значение типа [UInt32](../../sql-reference/functions/hash-functions.md).
-   Функция `murmurHash2_64` возвращает значение типа [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
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

**Параметры**

-   `par1, ...` — Переменное число параметров. Каждый параметр может быть любого из [поддерживаемых типов данных](../../sql-reference/data-types/index.md).

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

## murmurHash3\_32, murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

Генерирует значение [MurmurHash3](https://github.com/aappleby/smhasher).

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Параметры**

Обе функции принимают переменное число входных параметров. Параметры могут быть любого [поддерживаемого типа данных](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

-   Функция `murmurHash3_32` возвращает значение типа [UInt32](../../sql-reference/functions/hash-functions.md).
-   Функция `murmurHash3_64` возвращает значение типа [UInt64](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3\_128 {#murmurhash3-128}

Генерирует значение [MurmurHash3](https://github.com/aappleby/smhasher).

``` sql
murmurHash3_128( expr )
```

**Параметры**

-   `expr` — [выражение](../syntax.md#syntax-expressions) возвращающее значение типа[String](../../sql-reference/functions/hash-functions.md).

**Возвращаемое значение**

Хэш-значение типа [FixedString(16)](../../sql-reference/functions/hash-functions.md).

**Пример**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32-xxhash64}

Вычисляет `xxHash` от строки. Предлагается в двух вариантах: 32 и 64 бита.

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**Возвращаемое значение**

Хэш-значение типа `Uint32` или `Uint64`.

Тип: `xxHash`.

**Пример**

Запрос:

``` sql
SELECT xxHash32('Hello, world!');
```

Ответ:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**Смотрите также**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/hash_functions/) <!--hide-->
