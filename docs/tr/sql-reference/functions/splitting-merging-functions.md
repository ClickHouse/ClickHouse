---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "Dizeleri ve dizileri b\xF6lme ve birle\u015Ftirme"
---

# Dizeleri ve dizileri bölme ve birleştirme işlevleri {#functions-for-splitting-and-merging-strings-and-arrays}

## splitByChar (ayırıcı, s) {#splitbycharseparator-s}

Bir dizeyi belirtilen bir karakterle ayrılmış alt dizelere böler. Sabit bir dize kullanır `separator` tam olarak bir karakterden oluşan.
Seçili alt dizelerin bir dizisini döndürür. Ayırıcı dizenin başında veya sonunda oluşursa veya ardışık birden çok ayırıcı varsa, boş alt dizeler seçilebilir.

**Sözdizimi**

``` sql
splitByChar(<separator>, <s>)
```

**Parametre**

-   `separator` — The separator which should contain exactly one character. [Dize](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer (ler)**

Seçili alt dizelerin bir dizisini döndürür. Boş alt dizeler şu durumlarda seçilebilir:

-   Dizenin başında veya sonunda bir ayırıcı oluşur;
-   Birden fazla ardışık ayırıcı vardır;
-   Orijinal dize `s` boş.

Tür: [Dizi](../../sql-reference/data-types/array.md) -den [Dize](../../sql-reference/data-types/string.md).

**Örnek**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString (ayırıcı, s) {#splitbystringseparator-s}

Bir dizeyi bir dizeyle ayrılmış alt dizelere böler. Sabit bir dize kullanır `separator` ayırıcı olarak birden fazla karakter. Eğer dize `separator` boş olduğunu, bu bölünmüş dize `s` tek karakter dizisine.

**Sözdizimi**

``` sql
splitByString(<separator>, <s>)
```

**Parametre**

-   `separator` — The separator. [Dize](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer (ler)**

Seçili alt dizelerin bir dizisini döndürür. Boş alt dizeler şu durumlarda seçilebilir:

Tür: [Dizi](../../sql-reference/data-types/array.md) -den [Dize](../../sql-reference/data-types/string.md).

-   Boş olmayan bir ayırıcı dizenin başında veya sonunda oluşur;
-   Birden fazla ardışık boş olmayan ayırıcı vardır;
-   Orijinal dize `s` ayırıcı boş değilken boş.

**Örnek**

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

## arrayStringConcat(arr \[, ayırıcı\]) {#arraystringconcatarr-separator}

Dizide listelenen dizeleri ayırıcı ile birleştirir.'separator' isteğe bağlı bir parametredir: varsayılan olarak boş bir dizeye ayarlanmış sabit bir dize.
Dizeyi döndürür.

## alphaTokens (s) {#alphatokenss}

A-z ve A-Z aralıklarından ardışık baytların alt dizelerini seçer.

**Örnek**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
