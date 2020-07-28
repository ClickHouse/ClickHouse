---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 56
toc_title: "JSON ile \xE7al\u0131\u015Fmak."
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

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

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

## JSONLength(json\[, indices\_or\_keys\]…) {#jsonlengthjson-indices-or-keys}

Bir json dizisinin veya bir JSON nesnesinin uzunluğunu döndürür.

Değer yoksa veya yanlış bir türe sahipse, `0` iade edilecektir.

Örnekler:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

Bir JSON değerinin türünü döndürür.

Değer yoksa, `Null` iade edilecektir.

Örnekler:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices\_or\_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices\_or\_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices\_or\_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices\_or\_keys\]…) {#jsonextractbooljson-indices-or-keys}

Bir JSON ayrıştırır ve bir değer ayıklayın. Bu işlevler benzer `visitParam` işlevler.

Değer yoksa veya yanlış bir türe sahipse, `0` iade edilecektir.

Örnekler:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

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

## JSONExtract(json\[, indices\_or\_keys…\], return\_type) {#jsonextractjson-indices-or-keys-return-type}

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

## JSONExtractKeysAndValues(json\[, indices\_or\_keys…\], value\_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Değerlerin verilen ClickHouse veri türünde olduğu bir json'dan anahtar değer çiftlerini ayrıştırın.

Örnek:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

Json'un bir bölümünü döndürür.

Bölüm yoksa veya yanlış bir türe sahipse, boş bir dize döndürülür.

Örnek:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys\]…) {#jsonextractarrayrawjson-indices-or-keys}

Her biri ayrıştırılmamış dize olarak temsil edilen json dizisinin öğeleriyle bir dizi döndürür.

Bölüm yoksa veya dizi değilse, boş bir dizi döndürülür.

Örnek:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
