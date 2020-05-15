---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Arama Dizeleri \u0130\xE7in"
---

# Dizeleri aramak için işlevler {#functions-for-searching-strings}

Arama, tüm bu işlevlerde varsayılan olarak büyük / küçük harf duyarlıdır. Büyük / küçük harf duyarlı arama için ayrı Varyantlar vardır.

## pozisyon (Samanlık, iğne), bulun (Samanlık, iğne) {#position}

1'den başlayarak dizedeki bulunan alt dizenin konumunu (bayt cinsinden) döndürür.

Dize, tek baytlık kodlanmış bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışır. Bu varsayım karşılanmazsa ve bir karakter tek bir bayt kullanılarak temsil edilemezse, işlev bir istisna atmaz ve beklenmeyen bir sonuç döndürür. Karakter iki bayt kullanılarak temsil edilebilirse, iki bayt vb. kullanır.

Büyük / küçük harf duyarsız arama için işlevi kullanın [positionCaseİnsensitive](#positioncaseinsensitive).

**Sözdizimi**

``` sql
position(haystack, needle)
```

Takma ad: `locate(haystack, needle)`.

**Parametre**

-   `haystack` — string, in which substring will to be searched. [Dize](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Dize](../syntax.md#syntax-string-literal).

**Döndürülen değerler**

-   Alt dize bulunursa, bayt cinsinden başlangıç pozisyonu (1'den sayma).
-   0, alt dize bulunamadı.

Tür: `Integer`.

**Örnekler**

İfade “Hello, world!” tek baytla kodlanmış bir metni temsil eden bir bayt kümesi içerir. İşlev beklenen bazı sonuçları döndürür:

Sorgu:

``` sql
SELECT position('Hello, world!', '!')
```

Sonuç:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

Rusça'daki aynı ifade, tek bir bayt kullanılarak temsil edilemeyen karakterler içerir. İşlev beklenmedik bir sonuç verir (kullanım [positionUTF8](#positionutf8) çok bayt kodlu metin için işlev):

Sorgu:

``` sql
SELECT position('Привет, мир!', '!')
```

Sonuç:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## positionCaseİnsensitive {#positioncaseinsensitive}

Olarak aynı [konum](#position) 1'den başlayarak dizedeki bulunan alt dizenin konumunu (bayt cinsinden) döndürür. Büyük / küçük harf duyarlı bir arama için işlevi kullanın.

Dize, tek baytlık kodlanmış bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışır. Bu varsayım karşılanmazsa ve bir karakter tek bir bayt kullanılarak temsil edilemezse, işlev bir istisna atmaz ve beklenmeyen bir sonuç döndürür. Karakter iki bayt kullanılarak temsil edilebilirse, iki bayt vb. kullanır.

**Sözdizimi**

``` sql
positionCaseInsensitive(haystack, needle)
```

**Parametre**

-   `haystack` — string, in which substring will to be searched. [Dize](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Dize](../syntax.md#syntax-string-literal).

**Döndürülen değerler**

-   Alt dize bulunursa, bayt cinsinden başlangıç pozisyonu (1'den sayma).
-   0, alt dize bulunamadı.

Tür: `Integer`.

**Örnek**

Sorgu:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

Sonuç:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8 {#positionutf8}

1'den başlayarak dizedeki bulunan alt dizenin konumunu (Unicode noktalarında) döndürür.

Dizenin UTF-8 kodlanmış bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışır. Bu varsayım karşılanmazsa, işlev bir istisna atmaz ve beklenmeyen bir sonuç döndürür. Karakter iki Unicode noktası kullanılarak temsil edilebilirse, iki vb. kullanır.

Büyük / küçük harf duyarsız arama için işlevi kullanın [positionCaseİnsensitiveUTF8](#positioncaseinsensitiveutf8).

**Sözdizimi**

``` sql
positionUTF8(haystack, needle)
```

**Parametre**

-   `haystack` — string, in which substring will to be searched. [Dize](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Dize](../syntax.md#syntax-string-literal).

**Döndürülen değerler**

-   Unicode noktalarında başlangıç pozisyonu (1'den sayma), eğer alt dize bulundu.
-   0, alt dize bulunamadı.

Tür: `Integer`.

**Örnekler**

İfade “Hello, world!” rusça'da, tek noktalı kodlanmış bir metni temsil eden bir dizi Unicode noktası bulunur. İşlev beklenen bazı sonuçları döndürür:

Sorgu:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

Sonuç:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

İfade “Salut, étudiante!” karakter nerede `é` bir nokta kullanılarak temsil edilebilir (`U+00E9`) veya iki puan (`U+0065U+0301`) fonksiyon bazı beklenmedik sonuç iade edilebilir:

Mektup için sorgu `é` bir Unicode noktasını temsil eden `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Sonuç:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

Mektup için sorgu `é`, iki Unicode noktası temsil edilen `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Sonuç:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## positionCaseİnsensitiveUTF8 {#positioncaseinsensitiveutf8}

Olarak aynı [positionUTF8](#positionutf8) ama büyük küçük harf duyarlı. 1'den başlayarak dizedeki bulunan alt dizenin konumunu (Unicode noktalarında) döndürür.

Dizenin UTF-8 kodlanmış bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışır. Bu varsayım karşılanmazsa, işlev bir istisna atmaz ve beklenmeyen bir sonuç döndürür. Karakter iki Unicode noktası kullanılarak temsil edilebilirse, iki vb. kullanır.

**Sözdizimi**

``` sql
positionCaseInsensitiveUTF8(haystack, needle)
```

**Parametre**

-   `haystack` — string, in which substring will to be searched. [Dize](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Dize](../syntax.md#syntax-string-literal).

**Döndürülen değer**

-   Unicode noktalarında başlangıç pozisyonu (1'den sayma), eğer alt dize bulundu.
-   0, alt dize bulunamadı.

Tür: `Integer`.

**Örnek**

Sorgu:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

Sonuç:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## multiSearchAllPositions {#multisearchallpositions}

Olarak aynı [konum](string-search-functions.md#position) ama döner `Array` dizede bulunan karşılık gelen alt dizelerin konumlarının (bayt cinsinden). Pozisyonlar 1'den başlayarak endekslenir.

Arama, dize kodlaması ve harmanlama ile ilgili olmayan bayt dizileri üzerinde gerçekleştirilir.

-   Büyük / küçük harf duyarlı ASCII arama için işlevi kullanın `multiSearchAllPositionsCaseInsensitive`.
-   UTF-8'de arama yapmak için işlevi kullanın [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   Büyük / küçük harf duyarlı UTF-8 arama için multisearchallpositionscaseınsensitiveutf8 işlevini kullanın.

**Sözdizimi**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**Parametre**

-   `haystack` — string, in which substring will to be searched. [Dize](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Dize](../syntax.md#syntax-string-literal).

**Döndürülen değerler**

-   Bayt cinsinden başlangıç pozisyonları dizisi (1'den sayma), karşılık gelen alt dize bulunursa ve 0 bulunmazsa.

**Örnek**

Sorgu:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

Sonuç:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8 {#multiSearchAllPositionsUTF8}

Görmek `multiSearchAllPositions`.

## multiSearchFirstPosition (Samanlık, \[iğne<sub>1</sub>, iğne<sub>2</sub>, …, needle<sub>ve</sub>\]) {#multisearchfirstposition}

Olarak aynı `position` ancak dizenin en soldaki ofsetini döndürür `haystack` bu bazı iğnelerle eşleşti.

Büyük/küçük harfe duyarsız arama veya / VE UTF-8 biçiminde kullanım işlevleri için `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstİndex (Samanlık, \[iğne<sub>1</sub>, iğne<sub>2</sub>, …, needle<sub>ve</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

Dizini döndürür `i` en soldaki bulunan iğnenin (1'den başlayarak)<sub>ben</sub> diz inede `haystack` ve 0 aksi takdirde.

Büyük/küçük harfe duyarsız arama veya / VE UTF-8 biçiminde kullanım işlevleri için `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny (Samanlık, \[iğne<sub>1</sub>, iğne<sub>2</sub>, …, needle<sub>ve</sub>\]) {#function-multisearchany}

Döner 1, Eğer en az bir dize iğne<sub>ben</sub> dize ile eşleşir `haystack` ve 0 aksi takdirde.

Büyük/küçük harfe duyarsız arama veya / VE UTF-8 biçiminde kullanım işlevleri için `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "Not"
    Tamamı `multiSearch*` fonksiyonlar iğne sayısı 2'den az olmalıdır<sup>8</sup> uygulama şartname nedeniyle.

## maç (Samanlık, desen) {#matchhaystack-pattern}

Dize eşleşip eşleşmediğini denetler `pattern` düzenli ifade. Bir `re2` düzenli ifade. Bu [sözdizimi](https://github.com/google/re2/wiki/Syntax) of the `re2` düzenli ifadeler, Perl düzenli ifadelerin sözdiziminden daha sınırlıdır.

Eşleşmezse 0 veya eşleşirse 1 değerini döndürür.

Ters eğik çizgi sembolünün (`\`) normal ifadede kaçmak için kullanılır. Aynı sembol, dize değişmezlerinde kaçmak için kullanılır. Bu nedenle, normal bir ifadede sembolden kaçmak için, bir dize literalinde iki ters eğik çizgi (\\) yazmanız gerekir.

Normal ifade, bir bayt kümesiymiş gibi dizeyle çalışır. Normal ifade boş bayt içeremez.
Bir dizedeki alt dizeleri aramak için desenler için, LİKE veya ‘position’, çok daha hızlı çalıştıkları için.

## multiMatchAny (Samanlık, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

Olarak aynı `match`, ancak normal ifadelerin hiçbiri eşleşmezse 0 ve desenlerden herhangi biri eşleşirse 1 değerini döndürür. Kullanır [hyperscan](https://github.com/intel/hyperscan) kitaplık. Bir dizede alt dizeleri aramak için desenler için, kullanmak daha iyidir `multiSearchAny` çok daha hızlı çalıştığı için.

!!! note "Not"
    Herhangi birinin uzunluğu `haystack` dize 2'den az olmalıdır<sup>32</sup> bayt aksi takdirde özel durum atılır. Bu kısıtlama, hyperscan API nedeniyle gerçekleşir.

## multimatchanyındex (haystack, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

Olarak aynı `multiMatchAny`, ancak Samanlık eşleşen herhangi bir dizin döndürür.

## multiMatchAllİndices (haystack, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

Olarak aynı `multiMatchAny`, ancak herhangi bir sırada Samanlık eşleşen tüm indicies dizisini döndürür.

## multiFuzzyMatchAny (Samanlık, mesafe, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

Olarak aynı `multiMatchAny`, ancak herhangi bir desen samanlıkta bir sabitle eşleşirse 1 döndürür [mesafeyi Düzenle](https://en.wikipedia.org/wiki/Edit_distance). Bu fonksiyon aynı zamanda deneysel bir moddadır ve son derece yavaş olabilir. Daha fazla bilgi için bkz. [hyperscan belgeleri](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multifuzzymatchanyındex (Samanlık, mesafe, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

Olarak aynı `multiFuzzyMatchAny`, ancak sabit bir düzenleme mesafesi içinde Samanlık eşleşen herhangi bir dizin döndürür.

## multiFuzzyMatchAllİndices (Samanlık, mesafe, \[desen<sub>1</sub>, desen<sub>2</sub>, …, pattern<sub>ve</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

Olarak aynı `multiFuzzyMatchAny`, ancak sabit bir düzenleme mesafesi içinde saman yığını ile eşleşen herhangi bir sırada tüm dizinlerin dizisini döndürür.

!!! note "Not"
    `multiFuzzyMatch*` işlevler UTF-8 normal ifadeleri desteklemez ve bu tür ifadeler hyperscan kısıtlaması nedeniyle bayt olarak kabul edilir.

!!! note "Not"
    Hyperscan kullanan tüm işlevleri kapatmak için, ayarı kullanın `SET allow_hyperscan = 0;`.

## özü (Samanlık, desen) {#extracthaystack-pattern}

Normal ifade kullanarak bir dize parçasını ayıklar. Eğer ‘haystack’ eşleşmiyor ‘pattern’ regex, boş bir dize döndürülür. Regex alt desenler içermiyorsa, tüm regex ile eşleşen parçayı alır. Aksi takdirde, ilk alt desenle eşleşen parçayı alır.

## extractAll(Samanlık, desen) {#extractallhaystack-pattern}

Normal bir ifade kullanarak bir dizenin tüm parçalarını ayıklar. Eğer ‘haystack’ eşleşmiyor ‘pattern’ regex, boş bir dize döndürülür. Regex için tüm eşleşmelerden oluşan bir dizi dizeyi döndürür. Genel olarak, davranış ile aynıdır ‘extract’ işlev (bir alt desen yoksa ilk alt deseni veya tüm ifadeyi alır).

## gibi (Samanlık, desen), Samanlık gibi desen operatörü {#function-like}

Bir dizenin basit bir normal ifadeyle eşleşip eşleşmediğini denetler.
Normal ifade metasymbols içerebilir `%` ve `_`.

`%` herhangi bir bayt miktarını (sıfır karakter dahil) gösterir.

`_` herhangi bir bayt gösterir.

Ters eğik çizgi kullanın (`\`) metasimbollerden kaçmak için. Açıklamasında kaçan nota bakın ‘match’ İşlev.

Gibi düzenli ifadeler için `%needle%`, kod daha optimal ve hızlı olarak çalışır `position` İşlev.
Diğer normal ifadeler için kod, ‘match’ İşlev.

## notLike (Samanlık, desen), Samanlık desen operatörü gibi değil {#function-notlike}

Aynı şey ‘like’ ama negatif.

## ngramDistance(Samanlık, iğne) {#ngramdistancehaystack-needle}

Arasındaki 4 gram distancelık mesaf theeyi hesaplar `haystack` ve `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` veya `haystack` 32kb'den fazla, bir istisna atar. Sabit olmayan bazı `haystack` veya `needle` dizeler 32kb'den daha fazladır, mesafe her zaman birdir.

Büyük/küçük harf duyarsız arama veya / VE UTF-8 formatında kullanım işlevleri için `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramsearch(Samanlık, iğne) {#ngramsearchhaystack-needle}

Aynı olarak `ngramDistance` ama arasındaki simetrik olmayan farkı hesaplar `needle` ve `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` n-büyükanne. Daha yakın, daha `needle` is in the `haystack`. Bulanık dize arama için yararlı olabilir.

Büyük/küçük harf duyarsız arama veya / VE UTF-8 formatında kullanım işlevleri için `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "Not"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->
