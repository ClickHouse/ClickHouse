---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 21
toc_title: "Giri\u015F ve \xE7\u0131k\u0131\u015F bi\xE7imleri"
---

# Giriş ve çıkış verileri için biçimler {#formats}

ClickHouse kabul ve çeşitli biçimlerde veri dönebilirsiniz. Giriş için desteklenen bir biçim, sağlanan verileri ayrıştırmak için kullanılabilir `INSERT`s, gerçekleştirmek için `SELECT`s dosya, URL veya HDFS gibi bir dosya destekli tablodan veya harici bir sözlük okumak için. Çıktı için desteklenen bir biçim düzenlemek için kullanılabilir
sonuçları bir `SELECT` ve gerçekleştirmek için `INSERT`s dosya destekli bir tabloya.

Desteklenen formatlar şunlardır:

| Biçimli                                                         | Girdi | Çıktı |
|-----------------------------------------------------------------|-------|-------|
| [TabSeparated](#tabseparated)                                   | ✔     | ✔     |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗     | ✔     |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔     | ✔     |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔     | ✔     |
| [Şablon](#format-template)                                      | ✔     | ✔     |
| [TemplateİgnoreSpaces](#templateignorespaces)                   | ✔     | ✗     |
| [CSV](#csv)                                                     | ✔     | ✔     |
| [CSVWithNames](#csvwithnames)                                   | ✔     | ✔     |
| [CustomSeparated](#format-customseparated)                      | ✔     | ✔     |
| [Değerler](#data-format-values)                                 | ✔     | ✔     |
| [Dikey](#vertical)                                              | ✗     | ✔     |
| [VerticalRaw](#verticalraw)                                     | ✗     | ✔     |
| [JSON](#json)                                                   | ✗     | ✔     |
| [JSONCompact](#jsoncompact)                                     | ✗     | ✔     |
| [JSONEachRow](#jsoneachrow)                                     | ✔     | ✔     |
| [TSKV](#tskv)                                                   | ✔     | ✔     |
| [Çok](#pretty)                                                  | ✗     | ✔     |
| [PrettyCompact](#prettycompact)                                 | ✗     | ✔     |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗     | ✔     |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗     | ✔     |
| [PrettySpace](#prettyspace)                                     | ✗     | ✔     |
| [Protobuf](#protobuf)                                           | ✔     | ✔     |
| [Avro](#data-format-avro)                                       | ✔     | ✔     |
| [AvroConfluent](#data-format-avro-confluent)                    | ✔     | ✗     |
| [Parke](#data-format-parquet)                                   | ✔     | ✔     |
| [ORC](#data-format-orc)                                         | ✔     | ✗     |
| [RowBinary](#rowbinary)                                         | ✔     | ✔     |
| [Rowbinarywithnames ve türleri](#rowbinarywithnamesandtypes)    | ✔     | ✔     |
| [Yerli](#native)                                                | ✔     | ✔     |
| [Boş](#null)                                                    | ✗     | ✔     |
| [XML](#xml)                                                     | ✗     | ✔     |
| [CapnProto](#capnproto)                                         | ✔     | ✗     |

ClickHouse ayarları ile bazı biçim işleme parametrelerini kontrol edebilirsiniz. Daha fazla bilgi için okuyun [Ayarlar](../operations/settings/settings.md) bölme.

## TabSeparated {#tabseparated}

Sekmede ayrı format, veri satır ile yazılır. Her satır sekmelerle ayrılmış değerler içerir. Her değer, satırdaki son değer dışında bir sekme tarafından takip edilir ve ardından bir satır beslemesi gelir. Kesinlikle Unix hat beslemeleri her yerde kabul edilir. Son satır ayrıca sonunda bir satır beslemesi içermelidir. Değerler metin biçiminde, tırnak işaretleri olmadan yazılır ve özel karakterler kaçtı.

Bu biçim adı altında da kullanılabilir `TSV`.

Bu `TabSeparated` format, özel programlar ve komut dosyaları kullanarak verileri işlemek için uygundur. Varsayılan olarak HTTP arabiriminde ve komut satırı istemcisinin toplu iş modunda kullanılır. Bu format aynı zamanda farklı Dbms'ler arasında veri aktarımı sağlar. Örneğin, Mysql'den bir dökümü alabilir ve Clickhouse'a yükleyebilirsiniz veya tam tersi.

Bu `TabSeparated` biçim, toplam değerleri (TOPLAMLARLA birlikte kullanıldığında) ve aşırı değerleri (ne zaman ‘extremes’ 1 olarak ayarlanır). Bu durumlarda, toplam değerler ve aşırılıklar ana veriden sonra çıkar. Ana sonuç, toplam değerler ve aşırılıklar birbirinden boş bir çizgi ile ayrılır. Örnek:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

### Veri Biçimlendirme {#data-formatting}

Tamsayı sayılar ondalık biçimde yazılır. Sayılar ekstra içerebilir “+” başlangıçtaki karakter(ayrıştırma sırasında göz ardı edilir ve biçimlendirme sırasında kaydedilmez). Negatif olmayan sayılar negatif işareti içeremez. Okurken, boş bir dizeyi sıfır olarak veya (imzalı türler için) sıfır olarak sadece eksi işaretinden oluşan bir dizeyi ayrıştırmasına izin verilir. Karşılık gelen veri türüne uymayan sayılar, hata iletisi olmadan farklı bir sayı olarak ayrıştırılabilir.

Kayan noktalı sayılar ondalık biçimde yazılır. Nokta ondalık ayırıcı olarak kullanılır. Üstel girişler olduğu gibi desteklenir ‘inf’, ‘+inf’, ‘-inf’, ve ‘nan’. Kayan noktalı sayıların bir girişi, ondalık nokta ile başlayabilir veya sona erebilir.
Biçimlendirme sırasında kayan noktalı sayılarda doğruluk kaybolabilir.
Ayrıştırma sırasında, en yakın makine temsil edilebilir numarayı okumak kesinlikle gerekli değildir.

Tarihler YYYY-AA-DD biçiminde yazılır ve aynı biçimde ayrıştırılır, ancak ayırıcı olarak herhangi bir karakterle ayrıştırılır.
Tarihleri ile saatleri biçiminde yazılır `YYYY-MM-DD hh:mm:ss` ve aynı biçimde ayrıştırılır, ancak ayırıcı olarak herhangi bir karakterle.
Bu, tüm sistem saat diliminde istemci veya sunucu başlatıldığında (hangi veri biçimleri bağlı olarak) oluşur. Saatli tarihler için gün ışığından yararlanma saati belirtilmedi. Bu nedenle, bir dökümü gün ışığından yararlanma saati sırasında kez varsa, dökümü tümden verileri eşleşmiyor ve ayrıştırma iki kez birini seçecektir.
Bir okuma işlemi sırasında, yanlış tarih ve Tarih saatleriyle doğal taşma veya null tarih ve saat, hata iletisi olmadan ayrıştırılabilir.

Bir istisna olarak, tam 10 ondalık basamaktan oluşuyorsa, tarihlerin zamanlarla ayrıştırılması Unix zaman damgası biçiminde de desteklenir. Sonuç, saat dilimine bağlı değildir. Yyyy-MM-DD ss:mm:ss ve nnnnnnnnnn biçimleri otomatik olarak ayırt edilir.

Dizeler ters eğik çizgiden kaçan özel karakterlerle çıktılanır. Çıkış için aşağıdaki çıkış dizileri kullanılır: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Ayrıştırma da dizileri destekler `\a`, `\v`, ve `\xHH` (hxex kaçış dizileri) ve herhangi `\c` diz ,iler, nerede `c` herhangi bir karakter (bu diziler dönüştürülür `c`). Böylece, veri okuma, bir satır beslemesinin şu şekilde yazılabileceği formatları destekler `\n` veya `\`, veya bir çizgi besleme olarak. Örneğin, dize `Hello world` boşluk yerine kelimeler arasında bir çizgi beslemesi ile aşağıdaki varyasyonlardan herhangi birinde ayrıştırılabilir:

``` text
Hello\nworld

Hello\
world
```

MySQL, sekmeyle ayrılmış dökümleri yazarken kullandığı için ikinci varyant desteklenir.

Verileri TabSeparated biçiminde geçirirken kaçmanız gereken minimum karakter kümesi: sekme, satır besleme (LF) ve ters eğik çizgi.

Sadece küçük bir sembol seti kaçtı. Terminalinizin çıktıda mahvedeceği bir dize değerine kolayca rastlayabilirsiniz.

Diziler köşeli parantez içinde virgülle ayrılmış değerlerin bir listesi olarak yazılır. Dizideki sayı öğeleri normal olarak biçimlendirilir. `Date` ve `DateTime` türleri tek tırnak yazılır. Diz .eler yukarıdaki gibi aynı kural quoteslarla tek tırnak içinde yazılır.

[NULL](../sql-reference/syntax.md) olarak format islanır `\N`.

Her eleman [İçiçe](../sql-reference/data-types/nested-data-structures/nested.md) yapılar dizi olarak temsil edilir.

Mesela:

``` sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` text
1  [1]    ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

Farklıdır `TabSeparated` satırların kaçmadan yazıldığını biçimlendirin.
Bu biçim yalnızca bir sorgu sonucu çıktısı için uygundur, ancak ayrıştırma için değil (bir tabloya eklemek için veri alma).

Bu biçim adı altında da kullanılabilir `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Bu farklıdır `TabSeparated` sütun adlarının ilk satırda yazıldığını biçimlendirin.
Ayrıştırma sırasında, ilk satır tamamen göz ardı edilir. Sütun adlarını konumlarını belirlemek veya doğruluğunu kontrol etmek için kullanamazsınız.
(Başlık satırı ayrıştırma desteği gelecekte eklenebilir .)

Bu biçim adı altında da kullanılabilir `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Bu farklıdır `TabSeparated` sütun türleri ikinci satırda iken, sütun adlarının ilk satıra yazıldığını biçimlendirin.
Ayrıştırma sırasında, birinci ve ikinci satırlar tamamen göz ardı edilir.

Bu biçim adı altında da kullanılabilir `TSVWithNamesAndTypes`.

## Şablon {#format-template}

Bu biçim, belirli bir kaçış kuralına sahip değerler için yer tutucularla özel bir biçim dizesinin belirtilmesine izin verir.

Ayarları kullanır `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` kullanırken `JSON` kaçmak, daha fazla görmek)

Ayar `format_template_row` aşağıdaki sözdizimine sahip satırlar için Biçim dizesi içeren dosya yolunu belirtir:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

nerede `delimiter_i` değerler arasında bir sınırlayıcı mı (`$` sembol olarak kaçabilir `$$`),
`column_i` değerleri seçilecek veya eklenecek bir sütunun adı veya dizini (boşsa, sütun atlanır),
`serializeAs_i` sütun değerleri için kaçan bir kuraldır. Aşağıdaki kaçış kuralları desteklenir:

-   `CSV`, `JSON`, `XML` (aynı is theimlerin biçim formatslerine benzer şekilde)
-   `Escaped` (aynı şekilde `TSV`)
-   `Quoted` (aynı şekilde `Values`)
-   `Raw` (kaç withoutmadan, benzer şekilde `TSVRaw`)
-   `None` (kaçan kural yok, daha fazla görün)

Kaçan bir kural atlanırsa, o zaman `None` kullanılacaktır. `XML` ve `Raw` sadece çıkış için uygundur.

Yani, aşağıdaki biçim dizesi için:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

değerleri `SearchPhrase`, `c` ve `price` olarak kaçır ,ılan sütunlar `Quoted`, `Escaped` ve `JSON` (select için) yazdırılacak veya (ınsert için) arasında beklenecektir `Search phrase:`, `, count:`, `, ad price: $` ve `;` sırasıyla sınırlayıcılar. Mesela:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

Bu `format_template_rows_between_delimiter` ayar, sonuncusu hariç her satırdan sonra yazdırılan (veya beklenen) satırlar arasındaki sınırlayıcıyı belirtir (`\n` varsayılan olarak)

Ayar `format_template_resultset` resultset için bir biçim dizesi içeren dosya yolunu belirtir. Resultset için Biçim dizesi, satır için bir biçim dizesi ile aynı sözdizimine sahiptir ve bir önek, sonek ve bazı ek bilgileri yazdırmanın bir yolunu belirtmeyi sağlar. Sütun adları yerine aşağıdaki yer tutucuları içerir:

-   `data` veri içeren satırlar mı `format_template_row` biçim, ayrılmış `format_template_rows_between_delimiter`. Bu yer tutucu, biçim dizesindeki ilk yer tutucu olmalıdır.
-   `totals` toplam değerleri olan satır `format_template_row` biçim (TOPLAMLARLA birlikte kullanıldığında)
-   `min` satır içinde minimum değerlere sahip mi `format_template_row` biçim (1 olarak ayarlandığında)
-   `max` maksimum değerleri olan satır `format_template_row` biçim (1 olarak ayarlandığında)
-   `rows` çıktı satırlarının toplam sayısıdır
-   `rows_before_limit` minimum satır sayısı sınırı olmadan olurdu. Yalnızca sorgu sınırı içeriyorsa çıktı. Sorgu GROUP BY içeriyorsa, ROWS\_BEFORE\_LİMİT\_AT\_LEAST SINIRSIZDI olurdu satır tam sayısıdır.
-   `time` istek yürütme süresi saniyeler içinde mi
-   `rows_read` satır sayısı okun thedu mu
-   `bytes_read` bayt sayısı (sıkıştırılmamış) okundu mu

Tutucu `data`, `totals`, `min` ve `max` kaç rulema kuralı belirtilm (em (elidir (veya `None` açıkça belirtilen) olmalıdır. Kalan yer tutucuları belirtilen kaçan herhangi bir kural olabilir.
Eğer... `format_template_resultset` ayar boş bir dizedir, `${data}` varsayılan değer olarak kullanılır.
Insert sorguları biçimi için önek veya sonek varsa bazı sütunları veya bazı alanları atlamaya izin verir (örneğe bakın).

Örnek seç:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Sonuç:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

Örnek Ekle:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` ve `Sign` yer tutucular içinde tablodaki sütun adları vardır. Sonra değerler `Useless field` satır ve sonra `\nTotal rows:` sonek olarak göz ardı edilecektir.
Giriş verisindeki tüm sınırlayıcılar, belirtilen biçim dizelerindeki sınırlayıcılara kesinlikle eşit olmalıdır.

## TemplateİgnoreSpaces {#templateignorespaces}

Bu format sadece giriş için uygundur.
Benzer `Template`, ancak giriş akışındaki sınırlayıcılar ve değerler arasındaki boşluk karakterlerini atlar. Ancak, biçim dizeleri boşluk karakterleri içeriyorsa, bu karakterler giriş akışında beklenir. Ayrıca boş yer tutucuları belirtmek için izin verir (`${}` veya `${:None}`) aralarındaki boşlukları görmezden gelmek için bazı sınırlayıcıları ayrı parçalara ayırmak. Bu tür yer tutucular yalnızca boşluk karakterlerini atlamak için kullanılır.
Okumak mümkün `JSON` bu biçimi kullanarak, sütun değerleri tüm satırlarda aynı sıraya sahipse. Örneğin, aşağıdaki istek çıktı biçim örneğinden veri eklemek için kullanılabilir [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

TabSeparated benzer, ancak name=value biçiminde bir değer çıkarır. Adlar tabseparated biçiminde olduğu gibi kaçtı ve = simgesi de kaçtı.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) olarak format islanır `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

Çok sayıda küçük sütun olduğunda, bu biçim etkisizdir ve genellikle kullanmak için hiçbir neden yoktur. Bununla birlikte, verimlilik açısından Jsoneachrow'dan daha kötü değildir.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

Ayrıştırma, ek alanın varlığına izin verir `tskv` eşit işareti veya bir değer olmadan. Bu alan yoksayılır.

## CSV {#csv}

Virgülle ayrılmış değerler biçimi ([RFC](https://tools.ietf.org/html/rfc4180)).

Biçimlendirme yaparken, satırlar çift tırnak içine alınır. Bir dizenin içindeki çift alıntı, bir satırda iki çift tırnak olarak çıktılanır. Karakterlerden kaçmak için başka kural yoktur. Tarih ve Tarih-Saat çift tırnak içine alınır. Sayılar tırnak işaretleri olmadan çıktı. Değerler, bir sınırlayıcı karakterle ayrılır; `,` varsayılan olarak. Sınırlayıcı karakteri ayarında tanımlanır [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). Satırlar Unıx satır besleme (LF) kullanılarak ayrılır. Diziler CSV'DE aşağıdaki gibi serileştirilir: ilk olarak, dizi TabSeparated biçiminde olduğu gibi bir dizeye serileştirilir ve daha sonra ortaya çıkan dize çift tırnak içinde CSV'YE çıkarılır. CSV biçimindeki Tuples ayrı sütunlar olarak serileştirilir(yani, tuple'daki yuvalanmaları kaybolur).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\* Varsayılan olarak, sınırlayıcı `,`. Görmek [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) daha fazla bilgi için ayarlama.

Ayrıştırma yaparken, tüm değerler tırnak işaretleri ile veya tırnak işaretleri olmadan ayrıştırılabilir. Hem çift hem de tek tırnak desteklenmektedir. Satırlar tırnak işaretleri olmadan da düzenlenebilir. Bu durumda, sınırlayıcı karaktere veya satır beslemesine (CR veya LF) ayrıştırılır. RFC'Yİ ihlal ederken, satırları tırnak işaretleri olmadan ayrıştırırken, önde gelen ve sondaki boşluklar ve sekmeler göz ardı edilir. Hat beslemesi için Unix (LF), Windows (CR LF) ve Mac OS Classic (CR LF) türleri desteklenir.

Boş unquoted giriş değerleri, ilgili sütunlar için varsayılan değerlerle değiştirilir
[ınput\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
etkindir.

`NULL` olarak format islanır `\N` veya `NULL` veya boş bir unquoted dize (bkz. ayarlar [ınput\_format\_csv\_unquoted\_null\_literal\_as\_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) ve [ınput\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

CSV biçimi, toplamların ve aşırılıkların çıktısını aynı şekilde destekler `TabSeparated`.

## CSVWithNames {#csvwithnames}

Ayrıca, başlık satırını benzer şekilde yazdırır `TabSeparatedWithNames`.

## CustomSeparated {#format-customseparated}

Benzer [Şablon](#format-template), ancak tüm sütunları yazdırır veya okur ve ayardan kaçan kuralı kullanır `format_custom_escaping_rule` ve ayarlardan sınırlayıcılar `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` ve `format_custom_result_after_delimiter`, biçim dizelerinden değil.
Ayrıca var `CustomSeparatedIgnoreSpaces` biçim, benzer olan `TemplateIgnoreSpaces`.

## JSON {#json}

Verileri json formatında çıkarır. Veri tablolarının yanı sıra, bazı ek bilgilerle birlikte sütun adlarını ve türlerini de çıkarır: çıktı satırlarının toplam sayısı ve bir sınır yoksa çıktı olabilecek satır sayısı. Örnek:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Json JavaScript ile uyumludur. Bunu sağlamak için, bazı karakterler ek olarak kaçar: eğik çizgi `/` olarak kaç İsar `\/`; alternatif Satır sonları `U+2028` ve `U+2029`, hangi bazı tarayıcılar kırmak, olarak kaçtı `\uXXXX`. ASCII denetim karakterleri kaçtı: backspace, form besleme, satır besleme, satır başı ve yatay sekme ile değiştirilir `\b`, `\f`, `\n`, `\r`, `\t` , 00-1f aralığında kalan baytların yanı sıra `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output\_format\_json\_quote\_64bit\_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) 0'a.

`rows` – The total number of output rows.

`rows_before_limit_at_least` Minimum satır sayısı sınırı olmadan olurdu. Yalnızca sorgu sınırı içeriyorsa çıktı.
Sorgu GROUP BY içeriyorsa, ROWS\_BEFORE\_LİMİT\_AT\_LEAST SINIRSIZDI olurdu satır tam sayısıdır.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

Bu biçim yalnızca bir sorgu sonucu çıktısı için uygundur, ancak ayrıştırma için değil (bir tabloya eklemek için veri alma).

ClickHouse destekler [NULL](../sql-reference/syntax.md) olarak görüntülenen `null` JSON çıkışında.

Ayrıca bakınız [JSONEachRow](#jsoneachrow) biçimli.

## JSONCompact {#jsoncompact}

Yalnızca veri satırlarında json'dan farklıdır, nesnelerde değil, dizilerde çıktıdır.

Örnek:

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Bu biçim yalnızca bir sorgu sonucu çıktısı için uygundur, ancak ayrıştırma için değil (bir tabloya eklemek için veri alma).
Ayrıca bakınız `JSONEachRow` biçimli.

## JSONEachRow {#jsoneachrow}

Bu biçimi kullanırken, ClickHouse satırları ayrılmış, yeni satırla ayrılmış JSON nesneleri olarak çıkarır, ancak bir bütün olarak veriler geçerli JSON değildir.

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

Verileri eklerken, her satır için ayrı bir JSON nesnesi sağlamanız gerekir.

### Veri Ekleme {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse sağlar:

-   Nesnedeki herhangi bir anahtar-değer çiftleri sırası.
-   Bazı değerleri atlama.

ClickHouse, nesnelerden sonra öğeler ve virgüller arasındaki boşlukları yok sayar. Tüm nesneleri bir satırda geçirebilirsiniz. Onları Satır sonları ile ayırmak zorunda değilsiniz.

**İhmal edilen değerler işleme**

ClickHouse, karşılık gelen değerler için varsayılan değerlerle atlanmış değerleri değiştirir [veri türleri](../sql-reference/data-types/index.md).

Eğer `DEFAULT expr` belirtilen, ClickHouse bağlı olarak farklı ikame kuralları kullanır [ınput\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) ayar.

Aşağıdaki tabloyu düşünün:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   Eğer `input_format_defaults_for_omitted_fields = 0`, sonra varsayılan değer için `x` ve `a` eşitlikler `0` (varsayılan değer olarak `UInt32` veri türü).
-   Eğer `input_format_defaults_for_omitted_fields = 1`, sonra varsayılan değer için `x` eşitlikler `0`, ancak varsayılan değer `a` eşitlikler `x * 2`.

!!! note "Uyarıcı"
    İle veri ek whenlerken `insert_sample_with_metadata = 1`, ClickHouse, ekleme ile karşılaştırıldığında daha fazla hesaplama kaynağı tüketir `insert_sample_with_metadata = 0`.

### Veri Seçme {#selecting-data}

Düşünün `UserActivity` örnek olarak tablo:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Sorgu `SELECT * FROM UserActivity FORMAT JSONEachRow` dönüşler:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Aksine [JSON](#json) biçimi, geçersiz UTF-8 dizilerinin hiçbir ikame yoktur. Değerleri için olduğu gibi aynı şekilde kaçtı `JSON`.

!!! note "Not"
    Herhangi bir bayt kümesi dizelerde çıktı olabilir. Kullan... `JSONEachRow` tablodaki verilerin herhangi bir bilgi kaybetmeden JSON olarak biçimlendirilebileceğinden eminseniz biçimlendirin.

### İç içe yapıların kullanımı {#jsoneachrow-nested}

İle bir tablo varsa [İçiçe](../sql-reference/data-types/nested-data-structures/nested.md) veri türü sütunları, aynı yapıya sahip json verilerini ekleyebilirsiniz. İle bu özelliği etkinleştirin [ınput\_format\_ımport\_nested\_json](../operations/settings/settings.md#settings-input_format_import_nested_json) ayar.

Örneğin, aşağıdaki tabloyu göz önünde bulundurun:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Gibi görmek `Nested` veri türü açıklaması, ClickHouse, iç içe geçmiş yapının her bileşenini ayrı bir sütun olarak ele alır (`n.s` ve `n.i` sof )ram )ız için). Verileri aşağıdaki şekilde ekleyebilirsiniz:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Hiyerarşik bir json nesnesi olarak veri eklemek için [input\_format\_import\_nested\_json = 1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Bu ayar olmadan, ClickHouse bir özel durum atar.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Yerli {#native}

En verimli biçim. Veriler ikili formatta bloklar tarafından yazılır ve okunur. Her blok için satır sayısı, sütun sayısı, sütun adları ve türleri ve bu bloktaki sütunların parçaları birbiri ardına kaydedilir. Başka bir deyişle, bu format “columnar” – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

Bu biçimi, yalnızca ClickHouse DBMS tarafından okunabilen dökümleri hızlı bir şekilde oluşturmak için kullanabilirsiniz. Bu formatla kendiniz çalışmak mantıklı değil.

## Boş {#null}

Hiçbir şey çıktı. Ancak, sorgu işlenir ve komut satırı istemcisini kullanırken, veriler istemciye iletilir. Bu, performans testi de dahil olmak üzere testler için kullanılır.
Açıkçası, bu format yalnızca ayrıştırma için değil, çıktı için uygundur.

## Çok {#pretty}

Verileri Unicode-art tabloları olarak çıkarır, ayrıca TERMİNALDEKİ renkleri ayarlamak için ANSI-escape dizileri kullanır.
Tablonun tam bir ızgarası çizilir ve her satır terminalde iki satır kaplar.
Her sonuç bloğu ayrı bir tablo olarak çıktı. Bu, blokların arabelleğe alma sonuçları olmadan çıkabilmesi için gereklidir (tüm değerlerin görünür genişliğini önceden hesaplamak için arabelleğe alma gerekli olacaktır).

[NULL](../sql-reference/syntax.md) olarak çıktı `ᴺᵁᴸᴸ`.

Örnek (gösterilen [PrettyCompact](#prettycompact) biçimli):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Satırlar güzel\* biçimlerinde kaçmaz. Örnek için gösterilir [PrettyCompact](#prettycompact) biçimli:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Terminale çok fazla veri boşaltmaktan kaçınmak için yalnızca ilk 10.000 satır yazdırılır. Satır sayısı 10.000'den büyük veya eşitse, ileti “Showed first 10 000” bas .ılmıştır.
Bu biçim yalnızca bir sorgu sonucu çıktısı için uygundur, ancak ayrıştırma için değil (bir tabloya eklemek için veri alma).

Güzel biçim, toplam değerleri (TOPLAMLARLA birlikte kullanıldığında) ve aşırılıkları (ne zaman ‘extremes’ 1 olarak ayarlanır). Bu durumlarda, toplam değerler ve aşırı değerler ana veriden sonra ayrı tablolarda çıktılanır. Örnek (gösterilen [PrettyCompact](#prettycompact) biçimli):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyCompact {#prettycompact}

Farklıdır [Çok](#pretty) bu ızgara satırlar arasında çizilir ve sonuç daha kompakttır.
Bu biçim, etkileşimli modda komut satırı istemcisinde varsayılan olarak kullanılır.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Farklıdır [PrettyCompact](#prettycompact) bu kadar 10.000 satır arabelleğe alınır, daha sonra bloklarla değil, tek bir tablo olarak çıktılanır.

## PrettyNoEscapes {#prettynoescapes}

ANSİSİ-ESC .ape diz .ilerinin kullanıl .madığı güzel differsden farklıdır. Bu, bu formatı bir tarayıcıda görüntülemek ve aynı zamanda ‘watch’ komut satırı yardımcı programı.

Örnek:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

Tarayıcıda görüntülemek için HTTP arayüzünü kullanabilirsiniz.

### PrettyCompactNoEscapes {#prettycompactnoescapes}

Önceki ayar ile aynı.

### PrettySpaceNoEscapes {#prettyspacenoescapes}

Önceki ayar ile aynı.

## PrettySpace {#prettyspace}

Farklıdır [PrettyCompact](#prettycompact) bu boşluk (boşluk karakterleri) ızgara yerine kullanılır.

## RowBinary {#rowbinary}

Biçimleri ve ikili biçimde satır verileri ayrıştırır. Satırlar ve değerler, ayırıcılar olmadan ardışık olarak listelenir.
Bu biçim, satır tabanlı olduğundan yerel biçimden daha az etkilidir.

Tamsayılar sabit uzunlukta küçük endian temsilini kullanır. Örneğin, uint64 8 bayt kullanır.
DateTime, Unix zaman damgasını değer olarak içeren Uİnt32 olarak temsil edilir.
Tarih değeri olarak 1970-01-01 yılından bu yana gün sayısını içeren bir uint16 nesnesi olarak temsil edilir.
Dize varint uzunluğu (imzasız) olarak temsil edilir [LEB128](https://en.wikipedia.org/wiki/LEB128)), ardından dizenin baytları.
FixedString sadece bir bayt dizisi olarak temsil edilir.

Dizi varint uzunluğu (imzasız) olarak temsil edilir [LEB128](https://en.wikipedia.org/wiki/LEB128)), ardından dizinin ardışık elemanları.

İçin [NULL](../sql-reference/syntax.md#null-literal) destek, 1 veya 0 içeren ek bir bayt her önce eklenir [Nullable](../sql-reference/data-types/nullable.md) değer. 1 ise, o zaman değer `NULL` ve bu bayt ayrı bir değer olarak yorumlanır. 0 ise, bayttan sonraki değer değil `NULL`.

## Rowbinarywithnames ve türleri {#rowbinarywithnamesandtypes}

Benzer [RowBinary](#rowbinary), ancak eklenen Başlık ile:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)- kodlanmış sütun sayısı (N)
-   N `String`s sütun adlarını belirtme
-   N `String`s sütun türlerini belirleme

## Değerler {#data-format-values}

Her satırı parantez içinde yazdırır. Satırlar virgülle ayrılır. Son satırdan sonra virgül yok. Parantez içindeki değerler de virgülle ayrılır. Sayılar tırnak işaretleri olmadan ondalık biçimde çıktıdır. Diziler köşeli parantez içinde çıktı. Kat tırnak içinde çıkış dizelerle, tarihleri ve tarihleri. Kaçan kurallar ve ayrıştırma benzer [TabSeparated](#tabseparated) biçimli. Biçimlendirme sırasında fazladan boşluk eklenmez, ancak ayrıştırma sırasında izin verilir ve atlanır (izin verilmeyen dizi değerleri içindeki boşluklar hariç). [NULL](../sql-reference/syntax.md) olarak temsil edilir `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

Bu, kullanılan formattır `INSERT INTO t VALUES ...`, ancak sorgu sonuçlarını biçimlendirmek için de kullanabilirsiniz.

Ayrıca bakınız: [ınput\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) ve [ınput\_format\_values\_deduce\_templates\_of\_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) ayarlar.

## Dikey {#vertical}

Her değeri belirtilen sütun adıyla ayrı bir satıra yazdırır. Bu biçim, her satır çok sayıda sütundan oluşuyorsa, yalnızca bir veya birkaç satır yazdırmak için uygundur.

[NULL](../sql-reference/syntax.md) olarak çıktı `ᴺᵁᴸᴸ`.

Örnek:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Satırlar dikey biçimde kaçmadı:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Bu biçim yalnızca bir sorgu sonucu çıktısı için uygundur, ancak ayrıştırma için değil (bir tabloya eklemek için veri alma).

## VerticalRaw {#verticalraw}

Benzer [Dikey](#vertical), ama kaçan engelli ile. Bu biçim, yalnızca ayrıştırma (veri alma ve tabloya ekleme) için değil, sorgu sonuçlarının çıktısı için uygundur.

## XML {#xml}

XML biçimi, ayrıştırma için değil, yalnızca çıktı için uygundur. Örnek:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>yandex</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

Sütun adı kabul edilebilir bir biçime sahip değilse, sadece ‘field’ eleman adı olarak kullanılır. Genel olarak, XML yapısı JSON yapısını izler.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

Dize değerlerinde, karakterler `<` ve `&` olarak kaç arear `<` ve `&`.

Diziler olarak çıktı `<array><elem>Hello</elem><elem>World</elem>...</array>`ve tuples olarak `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap'n Proto, Protokol Tamponlarına ve tasarrufuna benzer, ancak JSON veya MessagePack gibi olmayan bir ikili mesaj biçimidir.

Cap'n Proto mesajları kesinlikle yazılır ve kendi kendini tanımlamaz, yani harici bir şema açıklamasına ihtiyaç duyarlar. Şema anında uygulanır ve her sorgu için önbelleğe alınır.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

Nerede `schema.capnp` bu gibi görünüyor:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Serializasyon etkilidir ve genellikle sistem yükünü arttırmaz.

Ayrıca bakınız [Biçim Şeması](#formatschema).

## Protobuf {#protobuf}

Protobuf-bir [Protokol Tamp Buffonları](https://developers.google.com/protocol-buffers/) biçimli.

Bu biçim, bir dış biçim şeması gerektirir. Şema sorgular arasında önbelleğe alınır.
ClickHouse hem destekler `proto2` ve `proto3` sözdizimiler. Tekrarlanan / isteğe bağlı / gerekli alanlar desteklenir.

Kullanım örnekleri:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

dosya nerede `schemafile.proto` bu gibi görünüyor:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

İletişim kuralı arabellekleri' ileti türü Tablo sütunları ve alanları arasındaki yazışmaları bulmak için clickhouse adlarını karşılaştırır.
Bu karşılaştırma büyük / küçük harf duyarsız ve karakterler `_` (alt çizgi) ve `.` (nokta) eşit olarak kabul edilir.
Bir sütun türleri ve protokol arabellekleri ileti alanı farklıysa, gerekli dönüştürme uygulanır.

İç içe geçmiş mesajlar desteklenir. Örneğin, alan için `z` aşağıdaki ileti türünde

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse adlı bir sütun bulmaya çalışır `x.y.z` (veya `x_y_z` veya `X.y_Z` ve benzeri).
İç içe mesajlar giriş veya çıkış a için uygundur [iç içe veri yapıları](../sql-reference/data-types/nested-data-structures/nested.md).

Böyle bir protobuf şemasında tanımlanan varsayılan değerler

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

uygulan ;mamaktadır; [tablo varsayılanları](../sql-reference/statements/create.md#create-default-values) bunların yerine kullanılır.

ClickHouse girişleri ve çıkışları protobuf mesajları `length-delimited` biçimli.
Bu, her mesajın uzunluğunu bir olarak yazmadan önce anlamına gelir [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
Ayrıca bakınız [popüler dillerde uzunlukla ayrılmış protobuf mesajları nasıl okunur / yazılır](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Apache Avro](http://avro.apache.org/) Apache'nin Hadoop projesi kapsamında geliştirilen satır odaklı veri serileştirme çerçevesidir.

ClickHouse Avro biçimi okuma ve yazma destekler [Avro veri dosyaları](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Veri Türleri Eşleştirme {#data_types-matching}

Aşağıdaki tablo, desteklenen veri türlerini ve Clickhouse'la nasıl eşleştiğini gösterir [veri türleri](../sql-reference/data-types/index.md) içinde `INSERT` ve `SELECT` sorgular.

| Avro veri türü `INSERT`                     | ClickHouse veri türü                                                                                              | Avro veri türü `SELECT`      |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8/16/32)](../sql-reference/data-types/int-uint.md), [Uİnt(8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [Uİnt64](../sql-reference/data-types/int-uint.md)               | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                   | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                   | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [Dize](../sql-reference/data-types/string.md)                                                                     | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                                                      | `fixed(N)`                   |
| `enum`                                      | [Enum (8/16)](../sql-reference/data-types/enum.md)                                                                | `enum`                       |
| `array(T)`                                  | [Dizi(T)](../sql-reference/data-types/array.md)                                                                   | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Null (T)](../sql-reference/data-types/date.md)                                                                   | `union(null, T)`             |
| `null`                                      | [Null (Hiçbir Şey)](../sql-reference/data-types/special-data-types/nothing.md)                                    | `null`                       |
| `int (date)` \*                             | [Tarihli](../sql-reference/data-types/date.md)                                                                    | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64 (3)](../sql-reference/data-types/datetime.md)                                                         | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64 (6)](../sql-reference/data-types/datetime.md)                                                         | `long (timestamp-micros)` \* |

\* [Avro mantıksal türleri](http://avro.apache.org/docs/current/spec.html#Logical+Types)

Desteklenmeyen Avro veri türleri: `record` (non-root), `map`

Desteklenmeyen Avro mantıksal veri türleri: `uuid`, `time-millis`, `time-micros`, `duration`

### Veri Ekleme {#inserting-data-1}

Bir Avro dosyasından ClickHouse tablosuna veri eklemek için:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

Giriş Avro dosyasının kök şeması olmalıdır `record` tür.

ClickHouse tablo sütunları ve Avro şema alanları arasındaki yazışmaları bulmak için adlarını karşılaştırır. Bu karşılaştırma büyük / küçük harf duyarlıdır.
Kullanılmayan alanlar atlanır.

ClickHouse tablo sütunlarının veri türleri, eklenen Avro verilerinin karşılık gelen alanlarından farklı olabilir. Veri eklerken, ClickHouse veri türlerini yukarıdaki tabloya göre yorumlar ve sonra [döküm](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) karşılık gelen sütun türüne veri.

### Veri Seçme {#selecting-data-1}

ClickHouse tablosundan bir Avro dosyasına veri seçmek için:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Sütun adları gerekir:

-   ile başla `[A-Za-z_]`
-   daha sonra sadece içerir `[A-Za-z0-9_]`

Çıkış Avro dosya sıkıştırma ve senkronizasyon aralığı ile yapılandırılabilir [output\_format\_avro\_codec](../operations/settings/settings.md#settings-output_format_avro_codec) ve [output\_format\_avro\_sync\_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) sırasıyla.

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent yaygın olarak kullanılan tek nesne Avro mesajları çözme destekler [Kafka](https://kafka.apache.org/) ve [Confluent Şema Kayıt](https://docs.confluent.io/current/schema-registry/index.html).

Her Avro iletisi, şema Kayıt defterinin yardımıyla gerçek şemaya çözülebilen bir şema kimliği gömer.

Şemalar çözüldükten sonra önbelleğe alınır.

Şema kayıt defteri URL'si ile yapılandırılır [format\_avro\_schema\_registry\_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### Veri Türleri Eşleştirme {#data_types-matching-1}

Aynı olarak [Avro](#data-format-avro)

### Kullanma {#usage}

Şema çözünürlüğünü hızlı bir şekilde doğrulamak için şunları kullanabilirsiniz [kafkasat](https://github.com/edenhill/kafkacat) ile [clickhouse-yerel](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

Kullanmak `AvroConfluent` ile [Kafka](../engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "Uyarıcı"
    Ayar `format_avro_schema_registry_url` yapılandırılmış olması gerekiyor `users.xml` yeniden başlattıktan sonra değerini korumak için.

## Parke {#data-format-parquet}

[Apache Parke](http://parquet.apache.org/) hadoop ekosisteminde yaygın bir sütunlu depolama biçimidir. ClickHouse, bu format için okuma ve yazma işlemlerini destekler.

### Veri Türleri Eşleştirme {#data_types-matching-2}

Aşağıdaki tablo, desteklenen veri türlerini ve Clickhouse'la nasıl eşleştiğini gösterir [veri türleri](../sql-reference/data-types/index.md) içinde `INSERT` ve `SELECT` sorgular.

| Parke veri türü (`INSERT`) | ClickHouse veri türü                                      | Parke veri türü (`SELECT`) |
|----------------------------|-----------------------------------------------------------|----------------------------|
| `UINT8`, `BOOL`            | [Uİnt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                    |
| `INT8`                     | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                     |
| `UINT16`                   | [Uınt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                   |
| `INT16`                    | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                    |
| `UINT32`                   | [Uİnt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                   |
| `INT32`                    | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                    |
| `UINT64`                   | [Uİnt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                   |
| `INT64`                    | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`      | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                    |
| `DOUBLE`                   | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                   |
| `DATE32`                   | [Tarihli](../sql-reference/data-types/date.md)            | `UINT16`                   |
| `DATE64`, `TIMESTAMP`      | [DateTime](../sql-reference/data-types/datetime.md)       | `UINT32`                   |
| `STRING`, `BINARY`         | [Dize](../sql-reference/data-types/string.md)             | `STRING`                   |
| —                          | [FixedString](../sql-reference/data-types/fixedstring.md) | `STRING`                   |
| `DECIMAL`                  | [Ondalık](../sql-reference/data-types/decimal.md)         | `DECIMAL`                  |

ClickHouse yapılandırılabilir hassas destekler `Decimal` tür. Bu `INSERT` sorgu parke davranır `DECIMAL` ClickHouse olarak yazın `Decimal128` tür.

Desteklen datameyen veri türleri: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse tablo sütunlarının veri türleri, eklenen parke verilerinin ilgili alanlarından farklı olabilir. Veri eklerken, ClickHouse veri türlerini yukarıdaki tabloya göre yorumlar ve sonra [döküm](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) ClickHouse tablo sütunu için ayarlanmış olan bu veri türüne ait veriler.

### Veri ekleme ve seçme {#inserting-and-selecting-data}

Bir dosyadan parke verilerini ClickHouse tablosuna aşağıdaki komutla ekleyebilirsiniz:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

Bir ClickHouse tablosundan veri seçin ve aşağıdaki komutla parke formatında bazı dosyaya kaydedebilirsiniz:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Hadoop ile veri alışverişi yapmak için şunları kullanabilirsiniz [HDFS tablo motoru](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORCC](https://orc.apache.org/) hadoop ekosisteminde yaygın bir sütunlu depolama biçimidir. Bu formatta yalnızca Clickhouse'a veri ekleyebilirsiniz.

### Veri Türleri Eşleştirme {#data_types-matching-3}

Aşağıdaki tablo, desteklenen veri türlerini ve Clickhouse'la nasıl eşleştiğini gösterir [veri türleri](../sql-reference/data-types/index.md) içinde `INSERT` sorgular.

| Orc veri türü (`INSERT`) | ClickHouse veri türü                                |
|--------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`          | [Uİnt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                   | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`                 | [Uınt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                  | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`                 | [Uİnt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                  | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`                 | [Uİnt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                  | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`    | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`                 | [Float64](../sql-reference/data-types/float.md)     |
| `DATE32`                 | [Tarihli](../sql-reference/data-types/date.md)      |
| `DATE64`, `TIMESTAMP`    | [DateTime](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`       | [Dize](../sql-reference/data-types/string.md)       |
| `DECIMAL`                | [Ondalık](../sql-reference/data-types/decimal.md)   |

ClickHouse yapılandırılabilir hassas destekler `Decimal` tür. Bu `INSERT` sorgu Orc davranır `DECIMAL` ClickHouse olarak yazın `Decimal128` tür.

Desteklenmeyen Orc veri türleri: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse tablo sütunlarının veri türlerinin karşılık gelen ORC veri alanları ile eşleşmesi gerekmez. Veri eklerken, ClickHouse veri türlerini yukarıdaki tabloya göre yorumlar ve sonra [döküm](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) veri türü için veri kümesi ClickHouse tablo sütun.

### Veri Ekleme {#inserting-data-2}

Bir dosyadan Orc verilerini ClickHouse tablosuna aşağıdaki komutla ekleyebilirsiniz:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Hadoop ile veri alışverişi yapmak için şunları kullanabilirsiniz [HDFS tablo motoru](../engines/table-engines/integrations/hdfs.md).

## Biçim Şeması {#formatschema}

Biçim şemasını içeren dosya adı, ayar tarafından ayarlanır `format_schema`.
Biçim onelerinden biri kullanıldığında bu ayarı ayarlamak gerekir `Cap'n Proto` ve `Protobuf`.
Biçim şeması, bir dosya adının ve bu dosyadaki bir ileti türünün adının birleşimidir ve iki nokta üst üste ile sınırlandırılmıştır,
e.g. `schemafile.proto:MessageType`.
Dosya, format için standart uzantıya sahipse (örneğin, `.proto` için `Protobuf`),
ihmal edilebilir ve bu durumda, biçim şeması şöyle görünür `schemafile:MessageType`.

Eğer giriş veya çıkış veri üzerinden [müşteri](../interfaces/cli.md) in the [interaktif mod](../interfaces/cli.md#cli_usage) biçim şe themasında belirtilen dosya adı
mutlak bir yol veya istemci üzerinde geçerli dizine göre bir yol içerebilir.
Eğer istemci kullanıyorsanız [Toplu Modu](../interfaces/cli.md#cli_usage), şemanın yolu güvenlik nedeniyle göreceli olmalıdır.

Eğer giriş veya çıkış veri üzerinden [HTTP arayüzü](../interfaces/http.md) biçim şemasında belirtilen dosya adı
belirtilen dizinde bulunmalıdır [format\_schema\_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
sunucu yapılandırmasında.

## Atlama Hataları {#skippingerrors}

Gibi bazı format suchlar `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` ve `Protobuf` ayrıştırma hatası oluşursa kırık satırı atlayabilir ve bir sonraki satırın başından ayrıştırmaya devam edebilir. Görmek [ınput\_format\_allow\_errors\_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) ve
[ınput\_format\_allow\_errors\_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) ayarlar.
Sınırlamalar:
- Ayrıştırma hatası durumunda `JSONEachRow` yeni satıra (veya EOF) kadar tüm verileri atlar, bu nedenle satırlar AŞAĞIDAKİLERLE sınırlandırılmalıdır `\n` hataları doğru saymak için.
- `Template` ve `CustomSeparated` bir sonraki satırın başlangıcını bulmak için son sütundan sonra sınırlayıcı ve satırlar arasındaki sınırlayıcıyı kullanın, Bu nedenle hataları atlamak yalnızca en az biri boş değilse çalışır.

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
