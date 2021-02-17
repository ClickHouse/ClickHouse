---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_title: "Json ile \xE7al\u0131\u015Fma"
---

# Json ile çalışmak için fonksiyonlar {#functions-for-working-with-json}

Üye Olarak.Metrica, JSON kullanıcılar tarafından oturum parametreleri olarak iletilir. Bu JSON ile çalışmak için bazı özel fonksiyonlar var. (Çoğu durumda, JSONs ek olarak önceden işlenir ve elde edilen değerler işlenmiş biçimlerinde ayrı sütunlara konur .) Tüm bu işlevler, JSON'UN ne olabileceğine dair güçlü varsayımlara dayanır, ancak işi yapmak için mümkün olduğunca az şey yapmaya çalışırlar.

Aşağıdaki varsayımlar yapılır:

1.  Alan adı (işlev bağımsız değişkeni) sabit olmalıdır.
2.  Alan adı bir şekilde json'da kanonik olarak kodlanmıştır. Mesela: `visitParamHas('{"abc":"def"}', 'abc') = 1`, ama `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Alanlar, herhangi bir yuvalama düzeyinde, ayrım gözetmeksizin aranır. Birden çok eşleşen alan varsa, ilk olay kullanılır.
4.  JSON, dize değişmezleri dışında boşluk karakterlerine sahip değildir.

## visitParamHas (params, isim) {#visitparamhasparams-name}

İle bir alan olup olmadığını denetler ‘name’ ad.

## visitParamExtractUİnt (params, isim) {#visitparamextractuintparams-name}

Uint64 adlı alanın değerinden ayrıştırır ‘name’. Bu bir dize alanı ise, dizenin başlangıcından itibaren bir sayıyı ayrıştırmaya çalışır. Alan yoksa veya varsa ancak bir sayı içermiyorsa, 0 döndürür.

## visitParamExtractİnt (params, isim) {#visitparamextractintparams-name}

Int64 için olduğu gibi.

## visitParamExtractFloat (params, isim) {#visitparamextractfloatparams-name}

Float64 için olduğu gibi.

## visitParamExtractBool (params, isim) {#visitparamextractboolparams-name}

True/false değerini ayrıştırır. Sonuç Uİnt8.

## visitParamExtractRaw (params, isim) {#visitparamextractrawparams-name}

Ayırıcılar da dahil olmak üzere bir alanın değerini döndürür.

Örnekler:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString (params, isim) {#visitparamextractstringparams-name}

Dizeyi çift tırnak içinde ayrıştırır. Değeri unescaped. Unescaping başarısız olursa, boş bir dize döndürür.

Örnekler:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

Şu anda biçimdeki kod noktaları için destek yok `\uXXXX\uYYYY` bu temel çok dilli düzlemden değildir(UTF-8 yerine CESU-8'e dönüştürülürler).

Aşağıdaki işlevler dayanmaktadır [simdjson](https://github.com/lemire/simdjson) daha karmaşık json ayrıştırma gereksinimleri için tasarlanmıştır. Yukarıda belirtilen varsayım 2 hala geçerlidir.

## ısvalidjson(json) {#isvalidjsonjson}

Dize geçirilen kontroller geçerli bir json'dur.

Örnekler:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices_or_keys\]…) {#jsonhasjson-indices-or-keys}

Değer JSON belgesinde varsa, `1` iade edilecektir.

Değer yoksa, `0` iade edilecektir.

Örnekler:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` sıfır veya daha fazla argüman listesi her biri dize veya tamsayı olabilir.

-   String = nesne üyesine anahtarla erişin.
-   Pozitif tamsayı = n-inci üyesine / anahtarına baştan erişin.
-   Negatif tamsayı = sondan n-inci üye/anahtara erişin.

Elemanın minimum Endeksi 1'dir. Böylece 0 öğesi mevcut değildir.

Hem json dizilerine hem de JSON nesnelerine erişmek için tamsayılar kullanabilirsiniz.

Bu yüzden, örneğin :

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices_or_keys\]…) {#jsonlengthjson-indices-or-keys}

Bir json dizisinin veya bir JSON nesnesinin uzunluğunu döndürür.

Değer yoksa veya yanlış bir türe sahipse, `0` iade edilecektir.

Örnekler:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices_or_keys\]…) {#jsontypejson-indices-or-keys}

Bir JSON değerinin türünü döndürür.

Değer yoksa, `Null` iade edilecektir.

Örnekler:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices_or_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices_or_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices_or_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices_or_keys\]…) {#jsonextractbooljson-indices-or-keys}

Bir JSON ayrıştırır ve bir değer ayıklayın. Bu işlevler benzer `visitParam` işlevler.

Değer yoksa veya yanlış bir türe sahipse, `0` iade edilecektir.

Örnekler:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices_or_keys\]…) {#jsonextractstringjson-indices-or-keys}

Bir json ayrıştırır ve bir dize ayıklayın. Bu işlev benzer `visitParamExtractString` işlevler.

Değer yoksa veya yanlış bir tür varsa, boş bir dize döndürülür.

Değeri unescaped. Unescaping başarısız olursa, boş bir dize döndürür.

Örnekler:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices_or_keys…\], Return_type) {#jsonextractjson-indices-or-keys-return-type}

Bir Json ayrıştırır ve verilen ClickHouse veri türünün bir değerini çıkarır.

Bu, önceki bir genellemedir `JSONExtract<type>` işlevler.
Bu demektir
`JSONExtract(..., 'String')` tam olarak aynı döndürür `JSONExtractString()`,
`JSONExtract(..., 'Float64')` tam olarak aynı döndürür `JSONExtractFloat()`.

Örnekler:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices_or_keys…\], Value_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Anahtar değer çiftlerini, değerlerin verilen ClickHouse veri türünde olduğu bir JSON'DAN ayrıştırır.

Örnek:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)]
```

## JSONExtractRaw(json\[, indices_or_keys\]…) {#jsonextractrawjson-indices-or-keys}

Json'un bir bölümünü ayrıştırılmamış dize olarak döndürür.

Bölüm yoksa veya yanlış bir türe sahipse, boş bir dize döndürülür.

Örnek:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices_or_keys…\]) {#jsonextractarrayrawjson-indices-or-keys}

Her biri ayrıştırılmamış dize olarak temsil edilen json dizisinin öğeleriyle bir dizi döndürür.

Bölüm yoksa veya dizi değilse, boş bir dizi döndürülür.

Örnek:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

## JSONExtractKeysAndValuesRaw {#json-extract-keys-and-values-raw}

Bir json nesnesinden ham verileri ayıklar.

**Sözdizimi**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Parametre**

-   `json` — [Dize](../data-types/string.md) geçerli JSON ile.
-   `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [dize](../data-types/string.md) anahtar veya bir tarafından alan almak için [tamsayı](../data-types/int-uint.md) N-inci alanını almak için (1'den endeksli, negatif tamsayılar sondan sayılır). Ayarlanmazsa, tüm JSON üst düzey nesne olarak ayrıştırılır. İsteğe bağlı parametre.

**Döndürülen değerler**

-   İle dizi `('key', 'value')` Demetler. Her iki tuple üyeleri dizeleri vardır.
-   İstenen nesne yoksa veya giriş json geçersiz ise boş dizi.

Tür: [Dizi](../data-types/array.md)([Demet](../data-types/tuple.md)([Dize](../data-types/string.md), [Dize](../data-types/string.md)).

**Örnekler**

Sorgu:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')
```

Sonuç:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Sorgu:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')
```

Sonuç:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Sorgu:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')
```

Sonuç:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
