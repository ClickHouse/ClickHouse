---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Ayarlar {#settings}

## distributed\_product\_mode {#distributed-product-mode}

Davranışını değiştirir [dağıtılmış alt sorgular](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Kısıtlama:

-   Yalnızca ın ve JOIN alt sorguları için uygulanır.
-   Yalnızca FROM bölümü birden fazla parça içeren dağıtılmış bir tablo kullanıyorsa.
-   Alt sorgu birden fazla parça içeren dağıtılmış bir tablo ile ilgiliyse.
-   Bir tablo için kullanılmaz-değerli [uzak](../../sql-reference/table-functions/remote.md) İşlev.

Olası değerler:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” özel).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` ile sorgu `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable\_optimize\_predicate\_expression {#enable-optimize-predicate-expression}

Yüklemi pushdown açar `SELECT` sorgular.

Yüklemi pushdown, dağıtılmış sorgular için ağ trafiğini önemli ölçüde azaltabilir.

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 1.

Kullanma

Aşağıdaki sorguları düşünün:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

Eğer `enable_optimize_predicate_expression = 1`, daha sonra bu sorguların yürütme süresi eşittir çünkü ClickHouse geçerlidir `WHERE` işlerken alt sorguya.

Eğer `enable_optimize_predicate_expression = 0`, daha sonra ikinci sorgunun yürütme süresi çok daha uzundur, çünkü `WHERE` yan tümcesi alt sorgu tamamlandıktan sonra tüm veriler için geçerlidir.

## fallback\_to\_stale\_replicas\_for\_distributed\_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Güncelleştirilmiş veriler mevcut değilse, bir sorgu için güncel olmayan bir yineleme zorlar. Görmek [Çoğalma](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse, tablonun eski kopyalarından en alakalı olanı seçer.

Yaparken kullanılır `SELECT` çoğaltılmış tablolara işaret eden dağıtılmış bir tablodan.

Varsayılan olarak, 1 (etkin).

## force\_index\_by\_date {#settings-force_index_by_date}

Dizin tarihe göre kullanılamıyorsa, sorgu yürütülmesini devre dışı bırakır.

MergeTree ailesindeki tablolarla çalışır.

Eğer `force_index_by_date=1`, ClickHouse sorgunun veri aralıklarını kısıtlamak için kullanılabilecek bir tarih anahtarı koşulu olup olmadığını denetler. Uygun bir koşul yoksa, bir istisna atar. Ancak, koşul okumak için veri miktarını azaltır olup olmadığını denetlemez. Örneğin, durum `Date != ' 2000-01-01 '` tablodaki tüm verilerle eşleştiğinde bile kabul edilebilir (yani, sorguyu çalıştırmak tam bir tarama gerektirir). MergeTree tablolarındaki veri aralıkları hakkında daha fazla bilgi için bkz. [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force\_primary\_key {#force-primary-key}

Birincil anahtar tarafından dizin oluşturma mümkün değilse, sorgu yürütülmesini devre dışı bırakır.

MergeTree ailesindeki tablolarla çalışır.

Eğer `force_primary_key=1`, ClickHouse, sorgunun veri aralıklarını kısıtlamak için kullanılabilecek bir birincil anahtar koşulu olup olmadığını denetler. Uygun bir koşul yoksa, bir istisna atar. Ancak, koşul okumak için veri miktarını azaltır olup olmadığını denetlemez. MergeTree tablolarındaki veri aralıkları hakkında daha fazla bilgi için bkz. [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## format\_schema {#format-schema}

Bu parametre, aşağıdaki gibi bir şema tanımı gerektiren biçimler kullanırken kullanışlıdır [Cap'n Proto](https://capnproto.org/) veya [Protobuf](https://developers.google.com/protocol-buffers/). Değer biçime bağlıdır.

## fsync\_metadata {#fsync-metadata}

Etkinleştirir veya devre dışı bırakır [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) yazarken `.sql` eğe. Varsayılan olarak etkin.

Sunucu, sürekli olarak oluşturulan ve yok edilen milyonlarca küçük tabloya sahipse, onu devre dışı bırakmak mantıklıdır.

## enable\_http\_compression {#settings-enable_http_compression}

Bir HTTP isteğine yanıt olarak veri sıkıştırmasını etkinleştirir veya devre dışı bırakır.

Daha fazla bilgi için, okuyun [HTTP arayüzü açıklaması](../../interfaces/http.md).

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

## http\_zlib\_compression\_level {#settings-http_zlib_compression_level}

Eğer bir HTTP isteğine yanıt veri sıkıştırma düzeyini ayarlar [enable\_http\_compression = 1](#settings-enable_http_compression).

Olası değerler: 1'den 9'a kadar olan sayılar.

Varsayılan değer: 3.

## http\_native\_compression\_disable\_checksumming\_on\_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

İstemciden HTTP POST verilerini açarken sağlama toplamı doğrulamasını etkinleştirir veya devre dışı bırakır. Sadece ClickHouse yerel sıkıştırma formatı için kullanılır (ile kullanılmaz `gzip` veya `deflate`).

Daha fazla bilgi için, okuyun [HTTP arayüzü açıklaması](../../interfaces/http.md).

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

## send\_progress\_in\_http\_headers {#settings-send_progress_in_http_headers}

Etkinleştirir veya devre dışı bırakır `X-ClickHouse-Progress` HTTP yanıt başlıkları `clickhouse-server` yanıtlar.

Daha fazla bilgi için, okuyun [HTTP arayüzü açıklaması](../../interfaces/http.md).

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

## max\_http\_get\_redirects {#setting-max_http_get_redirects}

Maksimum http get yönlendirme atlama sayısını sınırlar [URL](../../engines/table-engines/special/url.md)- motor masaları. Ayarı tablolar iki tür tarafından oluşturulan bu geçerlidir: [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) sorgu ve [url](../../sql-reference/table-functions/url.md) tablo işlevi.

Olası değerler:

-   Herhangi bir pozitif tamsayı şerbetçiotu sayısı.
-   0 — No hops allowed.

Varsayılan değer: 0.

## ınput\_format\_allow\_errors\_num {#settings-input_format_allow_errors_num}

Metin biçimlerinden (CSV, TSV, vb.) okurken kabul edilebilir hataların maksimum sayısını ayarlar.).

Varsayılan değer 0'dır.

Her zaman ile eşleştirmek `input_format_allow_errors_ratio`.

Satırları okurken bir hata oluştu, ancak hata sayacı hala daha az `input_format_allow_errors_num`, ClickHouse satırı yok sayar ve bir sonrakine geçer.

Eğer her ikisi de `input_format_allow_errors_num` ve `input_format_allow_errors_ratio` aşıldı, ClickHouse bir istisna atar.

## ınput\_format\_allow\_errors\_ratio {#settings-input_format_allow_errors_ratio}

Metin biçimlerinden (CSV, TSV, vb.) okurken izin verilen maksimum hata yüzdesini ayarlar.).
Hataların yüzdesi 0 ile 1 arasında kayan nokta sayısı olarak ayarlanır.

Varsayılan değer 0'dır.

Her zaman ile eşleştirmek `input_format_allow_errors_num`.

Satırları okurken bir hata oluştu, ancak hata sayacı hala daha az `input_format_allow_errors_ratio`, ClickHouse satırı yok sayar ve bir sonrakine geçer.

Eğer her ikisi de `input_format_allow_errors_num` ve `input_format_allow_errors_ratio` aşıldı, ClickHouse bir istisna atar.

## ınput\_format\_values\_interpret\_expressions {#settings-input_format_values_interpret_expressions}

Hızlı akış ayrıştırıcısı verileri ayrıştıramazsa, tam SQL ayrıştırıcısını etkinleştirir veya devre dışı bırakır. Bu ayar yalnızca için kullanılır [Değerler](../../interfaces/formats.md#data-format-values) veri ekleme sırasında biçimlendirin. Sözdizimi ayrıştırma hakkında daha fazla bilgi için bkz: [Sözdizimi](../../sql-reference/syntax.md) bölme.

Olası değerler:

-   0 — Disabled.

    Bu durumda, biçimlendirilmiş veri sağlamanız gerekir. Görmek [Biçimliler](../../interfaces/formats.md) bölme.

-   1 — Enabled.

    Bu durumda, bir SQL ifadesini bir değer olarak kullanabilirsiniz, ancak veri ekleme bu şekilde çok daha yavaştır. Yalnızca biçimlendirilmiş veri eklerseniz, ClickHouse ayar değeri 0 gibi davranır.

Varsayılan değer: 1.

Kullanım örneği

Ekle [DateTime](../../sql-reference/data-types/datetime.md) farklı ayarlarla değer yazın.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

Son sorgu Aşağıdakilere eşdeğerdir:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## ınput\_format\_values\_deduce\_templates\_of\_expressions {#settings-input_format_values_deduce_templates_of_expressions}

SQL deyimleri için şablon kesintisini etkinleştirir veya devre dışı bırakır [Değerler](../../interfaces/formats.md#data-format-values) biçimli. Bu ayrıştırma ve ifadeleri yorumlama sağlar `Values` ardışık satırlardaki ifadeler aynı yapıya sahipse çok daha hızlı. ClickHouse, bir ifadenin şablonunu çıkarmaya, bu şablonu kullanarak aşağıdaki satırları ayrıştırmaya ve ifadeyi başarılı bir şekilde ayrıştırılmış satırların bir yığınında değerlendirmeye çalışır.

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 1.

Aşağıdaki sorgu için:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   Eğer `input_format_values_interpret_expressions=1` ve `format_values_deduce_templates_of_expressions=0`, ifadeler her satır için ayrı ayrı yorumlanır (bu çok sayıda satır için çok yavaştır).
-   Eğer `input_format_values_interpret_expressions=0` ve `format_values_deduce_templates_of_expressions=1`, birinci, ikinci ve üçüncü satırlardaki ifadeler şablon kullanılarak ayrıştırılır `lower(String)` ve birlikte yorumlanır, ileri satırdaki ifade başka bir şablonla ayrıştırılır (`upper(String)`).
-   Eğer `input_format_values_interpret_expressions=1` ve `format_values_deduce_templates_of_expressions=1`, önceki durumda olduğu gibi aynı, ama aynı zamanda şablon anlamak mümkün değilse ayrı ayrı ifadeleri yorumlama geri dönüş sağlar.

## ınput\_format\_values\_accurate\_types\_of\_literals {#settings-input-format-values-accurate-types-of-literals}

Bu ayar yalnızca şu durumlarda kullanılır `input_format_values_deduce_templates_of_expressions = 1`. Bu, bazı sütunların ifadelerinin aynı yapıya sahip olması, ancak farklı türlerde sayısal değişmezler içermesi olabilir, örneğin

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Olası değerler:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` veya `Int64` yerine `UInt64` için `42`), ancak taşma ve hassasiyet sorunlarına neden olabilir.

-   1 — Enabled.

    Bu durumda, ClickHouse gerçek literal türünü denetler ve karşılık gelen türde bir ifade şablonu kullanır. Bazı durumlarda, ifade değerlendirmesini önemli ölçüde yavaşlatabilir `Values`.

Varsayılan değer: 1.

## ınput\_format\_defaults\_for\_omitted\_fields {#session_settings-input_format_defaults_for_omitted_fields}

Yaparken `INSERT` sorgular, atlanmış giriş sütun değerlerini ilgili sütunların varsayılan değerleriyle değiştirin. Bu seçenek yalnızca aşağıdakiler için geçerlidir [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) ve [TabSeparated](../../interfaces/formats.md#tabseparated) biçimliler.

!!! note "Not"
    Bu seçenek etkinleştirildiğinde, genişletilmiş tablo meta verileri sunucudan istemciye gönderilir. Sunucuda ek bilgi işlem kaynakları tüketir ve performansı azaltabilir.

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 1.

## ınput\_format\_tsv\_empty\_as\_default {#settings-input-format-tsv-empty-as-default}

Etkinleştirildiğinde, TSV'DEKİ boş giriş alanlarını varsayılan değerlerle değiştirin. Karmaşık varsayılan ifadeler için `input_format_defaults_for_omitted_fields` de etkin olmalıdır.

Varsayılan olarak devre dışı.

## ınput\_format\_null\_as\_default {#settings-input-format-null-as-default}

Giriş verileri içeriyorsa, varsayılan değerleri kullanarak etkinleştirir veya devre dışı bırakır `NULL`, ancak ilgili sütunun veri türü değil `Nullable(T)` (Metin Giriş biçimleri için).

## ınput\_format\_skip\_unknown\_fields {#settings-input-format-skip-unknown-fields}

Etkinleştirir veya ek veri ekleme atlama devre dışı bırakır.

Veri yazarken, giriş verileri hedef tabloda bulunmayan sütunlar içeriyorsa, ClickHouse bir özel durum atar. Atlama etkinleştirilirse, ClickHouse ek veri eklemez ve bir istisna atmaz.

Desteklenen formatlar:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

## ınput\_format\_ımport\_nested\_json {#settings-input_format_import_nested_json}

Json verilerinin iç içe nesnelerle eklenmesini etkinleştirir veya devre dışı bırakır.

Desteklenen formatlar:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

Ayrıca bakınız:

-   [İç içe yapıların kullanımı](../../interfaces/formats.md#jsoneachrow-nested) ile... `JSONEachRow` biçimli.

## ınput\_format\_with\_names\_use\_header {#settings-input-format-with-names-use-header}

Veri eklerken sütun sırasını denetlemeyi etkinleştirir veya devre dışı bırakır.

Ekleme performansını artırmak için, giriş verilerinin sütun sırasının hedef tablodaki ile aynı olduğundan eminseniz, bu denetimi devre dışı bırakmanızı öneririz.

Desteklenen formatlar:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 1.

## date\_time\_input\_format {#settings-date_time_input_format}

Tarih ve saat metin gösterimi bir ayrıştırıcı seçme sağlar.

Ayar için geçerli değildir [tarih ve saat fonksiyonları](../../sql-reference/functions/date-time-functions.md).

Olası değerler:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse temel ayrıştırmak `YYYY-MM-DD HH:MM:SS` format ve tüm [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) tarih ve saat biçimleri. Mesela, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse sadece temel ayrıştırmak `YYYY-MM-DD HH:MM:SS` biçimli. Mesela, `'2019-08-20 10:18:56'`.

Varsayılan değer: `'basic'`.

Ayrıca bakınız:

-   [DateTime veri türü.](../../sql-reference/data-types/datetime.md)
-   [Tarihler ve saatler ile çalışmak için fonksiyonlar.](../../sql-reference/functions/date-time-functions.md)

## join\_default\_strictness {#settings-join_default_strictness}

Ayarlar varsayılan strictness için [Maddeleri KATILIN ](../../sql-reference/statements/select/join.md#select-join).

Olası değerler:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Kartezyen ürün](https://en.wikipedia.org/wiki/Cartesian_product) eşleşen satırlardan. Bu normaldir `JOIN` standart SQL'DEN davranış.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` ve `ALL` aynı.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` veya `ANY` sorguda belirtilmezse, ClickHouse bir özel durum atar.

Varsayılan değer: `ALL`.

## join\_any\_take\_last\_row {#settings-join_any_take_last_row}

İle birleştirme işlemlerinin davranışını değiştirir `ANY` katılık.

!!! warning "Dikkat"
    Bu ayar yalnızca aşağıdakiler için geçerlidir `JOIN` ile işlemler [Katmak](../../engines/table-engines/special/join.md) motor tabloları.

Olası değerler:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

Varsayılan değer: 0.

Ayrıca bakınız:

-   [Jo](../../sql-reference/statements/select/join.md#select-join)
-   [Jo tablein table engine](../../engines/table-engines/special/join.md)
-   [join\_default\_strictness](#settings-join_default_strictness)

## join\_use\_nulls {#join_use_nulls}

Türünü ayarlar [JOIN](../../sql-reference/statements/select/join.md) davranış. Tabloları birleştirirken boş hücreler görünebilir. ClickHouse bu ayara göre onları farklı şekilde doldurur.

Olası değerler:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` standart SQL ile aynı şekilde davranır. Karşılık gelen alanın türü dönüştürülür [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) ve boş hücreler ile doldurulur [NULL](../../sql-reference/syntax.md).

Varsayılan değer: 0.

## max\_block\_size {#setting-max_block_size}

Clickhouse'da, veriler bloklarla (sütun parçaları kümeleri) işlenir. Tek bir blok için dahili işlem döngüleri yeterince verimlidir, ancak her blokta gözle görülür harcamalar vardır. Bu `max_block_size` ayar, blokun boyutunun (satır sayımında) tablolardan yükleneceği bir öneridir. Blok boyutu çok küçük olmamalı, böylece her bloktaki harcamalar hala fark edilebilir, ancak çok büyük olmamalı, böylece ilk blok hızla işlendikten sonra tamamlanan limitli sorgu çok büyük olmamalıdır. Amaç, birden çok iş parçacığında çok sayıda sütun ayıklarken çok fazla bellek tüketmekten kaçınmak ve en azından bazı önbellek konumlarını korumaktır.

Varsayılan değer: 65,536.

Blok boyutu `max_block_size` her zaman tablodan yüklenmez. Daha az verinin alınması gerektiği açıksa, daha küçük bir blok işlenir.

## preferred\_block\_size\_bytes {#preferred-block-size-bytes}

Olarak aynı amaç için kullanılır `max_block_size`, ancak önerilen blok boyutunu bayt cinsinden, bloktaki satır sayısına uyarlayarak ayarlar.
Ancak, blok boyutu daha fazla olamaz `max_block_size` satırlar.
Varsayılan olarak: 1.000.000. Sadece MergeTree motorlarından okurken çalışır.

## merge\_tree\_mın\_rows\_for\_concurrent\_read {#setting-merge-tree-min-rows-for-concurrent-read}

Bir dosyadan okunacak satır sayısı ise [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tablo aşıyor `merge_tree_min_rows_for_concurrent_read` daha sonra ClickHouse, bu dosyadan birkaç iş parçacığı üzerinde eşzamanlı bir okuma gerçekleştirmeye çalışır.

Olası değerler:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 163840.

## merge\_tree\_min\_bytes\_for\_concurrent\_read {#setting-merge-tree-min-bytes-for-concurrent-read}

Eğer bir dosyadan okunacak bayt sayısı [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)- motor tablosu `merge_tree_min_bytes_for_concurrent_read`, daha sonra ClickHouse, bu dosyadan aynı anda birkaç iş parçacığında okumaya çalışır.

Olası değer:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 251658240.

## merge\_tree\_mın\_rows\_for\_seek {#setting-merge-tree-min-rows-for-seek}

Bir dosyada okunacak iki veri bloğu arasındaki mesafe daha az ise `merge_tree_min_rows_for_seek` satırlar, daha sonra ClickHouse dosyayı aramaz, ancak verileri sırayla okur.

Olası değerler:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 0.

## merge\_tree\_min\_bytes\_for\_seek {#setting-merge-tree-min-bytes-for-seek}

Bir dosyada okunacak iki veri bloğu arasındaki mesafe daha az ise `merge_tree_min_bytes_for_seek` bayt, daha sonra ClickHouse sırayla böylece ekstra arama kaçınarak, her iki blok içeren bir dosya aralığını okur.

Olası değerler:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 0.

## merge\_tree\_coarse\_index\_granularity {#setting-merge-tree-coarse-index-granularity}

Veri ararken, ClickHouse dizin dosyasındaki veri işaretlerini denetler. ClickHouse gerekli tuşların bazı aralıklarda olduğunu bulursa, bu aralığı `merge_tree_coarse_index_granularity` subranges ve gerekli anahtarları orada yinelemeli olarak arar.

Olası değerler:

-   Herhangi bir pozitif bile tamsayı.

Varsayılan değer: 8.

## merge\_tree\_max\_rows\_to\_use\_cache {#setting-merge-tree-max-rows-to-use-cache}

ClickHouse daha fazla okumak gerekiyorsa `merge_tree_max_rows_to_use_cache` bir sorgudaki satırlar, sıkıştırılmamış blokların önbelleğini kullanmaz.

Sıkıştırılmamış blokların önbelleği, sorgular için ayıklanan verileri depolar. ClickHouse, tekrarlanan küçük sorgulara verilen yanıtları hızlandırmak için bu önbelleği kullanır. Bu ayar, önbelleğin büyük miktarda veri okuyan sorgularla çöpe atmasını önler. Bu [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) sunucu ayarı, sıkıştırılmamış blokların önbelleğinin boyutunu tanımlar.

Olası değerler:

-   Herhangi bir pozitif tamsayı.

Default value: 128 ✕ 8192.

## merge\_tree\_max\_bytes\_to\_use\_cache {#setting-merge-tree-max-bytes-to-use-cache}

ClickHouse daha fazla okumak gerekiyorsa `merge_tree_max_bytes_to_use_cache` bir sorguda bayt, sıkıştırılmamış blokların önbelleğini kullanmaz.

Sıkıştırılmamış blokların önbelleği, sorgular için ayıklanan verileri depolar. ClickHouse, tekrarlanan küçük sorgulara verilen yanıtları hızlandırmak için bu önbelleği kullanır. Bu ayar, önbelleğin büyük miktarda veri okuyan sorgularla çöpe atmasını önler. Bu [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) sunucu ayarı, sıkıştırılmamış blokların önbelleğinin boyutunu tanımlar.

Olası değer:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 2013265920.

## min\_bytes\_to\_use\_direct\_io {#settings-min-bytes-to-use-direct-io}

Depolama diskine Doğrudan G/Ç erişimi kullanmak için gereken minimum veri hacmi.

ClickHouse, tablolardan veri okurken bu ayarı kullanır. Okunacak tüm verilerin toplam depolama hacmi aşarsa `min_bytes_to_use_direct_io` bayt, daha sonra ClickHouse ile depolama diskinden veri okur `O_DIRECT` seçenek.

Olası değerler:

-   0 — Direct I/O is disabled.
-   Pozitif tamsayı.

Varsayılan değer: 0.

## log\_queries {#settings-log-queries}

Sorgu günlüğü ayarlama.

Bu kurulum ile Clickhouse'a gönderilen sorgular, [query\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) sunucu yapılandırma parametresi.

Örnek:

``` text
log_queries=1
```

## log\_queries\_min\_type {#settings-log-queries-min-type}

`query_log` giriş yapmak için en az tür.

Olası değerler:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Varsayılan değer: `QUERY_START`.

Entiries gider hangi sınırlamak için kullanılabilir `query_log`, sadece hatalarda ilginç olduğunuzu söyleyin, o zaman kullanabilirsiniz `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log\_query\_threads {#settings-log-query-threads}

Sorgu iş parçacığı günlüğü ayarlama.

Bu kurulum ile ClickHouse tarafından çalıştırılan sorguların konuları, [query\_thread\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) sunucu yapılandırma parametresi.

Örnek:

``` text
log_query_threads=1
```

## max\_ınsert\_block\_size {#settings-max_insert_block_size}

Bir tabloya eklemek için oluşturulacak blokların boyutu.
Bu ayar yalnızca sunucu blokları oluşturduğu durumlarda geçerlidir.
Örneğin, HTTP arabirimi üzerinden bir ekleme için sunucu veri biçimini ayrıştırır ve belirtilen boyuttaki blokları oluşturur.
Ancak, clickhouse-client kullanırken, istemci verileri kendisi ayrıştırır ve ‘max\_insert\_block\_size’ sunucudaki ayar, eklenen blokların boyutunu etkilemez.
Veri SELECT sonra oluşturulan aynı blokları kullanarak eklendiğinden, INSERT SELECT kullanırken ayarı da bir amacı yoktur.

Varsayılan değer: 1.048,576.

Varsayılan biraz daha fazla `max_block_size`. Bunun nedeni, bazı tablo motorlarının (`*MergeTree`) oldukça büyük bir varlık olan eklenen her blok için diskte bir veri parçası oluşturun. Benzer bir şekilde, `*MergeTree` tablolar ekleme sırasında verileri sıralar ve yeterince büyük bir blok boyutu RAM'de daha fazla veriyi sıralamaya izin verir.

## min\_insert\_block\_size\_rows {#min-insert-block-size-rows}

Bir tabloya eklenebilen blok içindeki minimum satır sayısını ayarlar. `INSERT` sorgu. Daha küçük boyutlu bloklar daha büyük olanlara ezilir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Squashing disabled.

Varsayılan değer: 1048576.

## min\_insert\_block\_size\_bytes {#min-insert-block-size-bytes}

Bir tabloya eklenebilen blok içindeki minimum bayt sayısını ayarlar. `INSERT` sorgu. Daha küçük boyutlu bloklar daha büyük olanlara ezilir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Squashing disabled.

Varsayılan değer: 268435456.

## max\_replica\_delay\_for\_distributed\_queries {#settings-max_replica_delay_for_distributed_queries}

Dağıtılmış sorgular için gecikmeli yinelemeleri devre dışı bırakır. Görmek [Çoğalma](../../engines/table-engines/mergetree-family/replication.md).

Saati saniye olarak ayarlar. Bir çoğaltma ayarlanan değerden daha fazla kalıyorsa, Bu çoğaltma kullanılmaz.

Varsayılan değer: 300.

Yaparken kullanılır `SELECT` çoğaltılmış tablolara işaret eden dağıtılmış bir tablodan.

## max\_threads {#settings-max_threads}

Uzak sunuculardan veri almak için iş parçacıkları hariç olmak üzere sorgu işleme iş parçacıklarının maksimum sayısı (bkz. ‘max\_distributed\_connections’ parametre).

Bu parametre, paralel olarak sorgu işleme ardışık düzeninin aynı aşamalarını gerçekleştiren iş parçacıkları için geçerlidir.
Örneğin, bir tablodan okurken, ifadeleri işlevlerle değerlendirmek mümkün ise, en azından paralel olarak grup için where ve pre-aggregate ile filtreleyin ‘max\_threads’ konu sayısı, daha sonra ‘max\_threads’ kullanılır.

Varsayılan değer: fiziksel CPU çekirdeği sayısı.

Bir kerede bir sunucuda normal olarak birden az SELECT sorgusu çalıştırılırsa, bu parametreyi gerçek işlemci çekirdeği sayısından biraz daha küçük bir değere ayarlayın.

Bir sınır nedeniyle hızlı bir şekilde tamamlanan sorgular için, daha düşük bir ‘max\_threads’. Örneğin, gerekli sayıda giriş her blokta ve max\_threads = 8'de bulunuyorsa, sadece bir tane okumak için yeterli olsa da, 8 blok alınır.

Daha küçük `max_threads` değer, daha az bellek tüketilir.

## max\_ınsert\_threads {#settings-max-insert-threads}

Çalıştırılacak maksimum iş parçacığı sayısı `INSERT SELECT` sorgu.

Olası değerler:

-   0 (or 1) — `INSERT SELECT` paralel infaz yok.
-   Pozitif tamsayı. 1'den büyük.

Varsayılan değer: 0.

Paralellik `INSERT SELECT` etkisi vardır sadece eğer `SELECT` bölüm paralel olarak yürütülür, bkz [max\_threads](#settings-max_threads) ayar.
Daha yüksek değerler daha yüksek bellek kullanımına yol açacaktır.

## max\_compress\_block\_size {#max-compress-block-size}

Bir tabloya yazmak için sıkıştırmadan önce sıkıştırılmamış veri bloklarının en büyük boyutu. Varsayılan olarak, 1.048.576 (1 MiB). Boyut azaltılırsa, sıkıştırma oranı önemli ölçüde azalır, önbellek konumu nedeniyle sıkıştırma ve dekompresyon hızı biraz artar ve bellek tüketimi azalır. Bu ayarı değiştirmek için genellikle herhangi bir neden yoktur.

Sıkıştırma için blokları (bayttan oluşan bir bellek yığını) sorgu işleme için bloklarla (bir tablodan satır kümesi) karıştırmayın.

## min\_compress\_block\_size {#min-compress-block-size}

İçin [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)" Tablolar. Sorguları işlerken gecikmeyi azaltmak için, boyutu en az bir sonraki işareti yazarken bir blok sıkıştırılır ‘min\_compress\_block\_size’. Varsayılan olarak, 65.536.

Sıkıştırılmamış veriler daha az ise, bloğun gerçek boyutu ‘max\_compress\_block\_size’, bu değerden daha az değildir ve bir işaret için veri hacminden daha az değildir.

Bir örneğe bakalım. Varsaymak ‘index\_granularity’ tablo oluşturma sırasında 8192 olarak ayarlandı.

Bir uint32 tipi sütun yazıyoruz (değer başına 4 bayt). 8192 satır yazarken, toplam 32 KB veri olacaktır. Min\_compress\_block\_size = 65.536 olduğundan, her iki işaret için sıkıştırılmış bir blok oluşturulacaktır.

Dize türüne sahip bir URL sütunu yazıyoruz (değer başına ortalama 60 bayt boyutu). 8192 satır yazarken, ortalama 500 KB veri biraz daha az olacaktır. Bu 65,536'dan fazla olduğu için, her işaret için sıkıştırılmış bir blok oluşturulacaktır. Bu durumda, diskteki verileri tek bir işaret aralığında okurken, ekstra veriler sıkıştırılmaz.

Bu ayarı değiştirmek için genellikle herhangi bir neden yoktur.

## max\_query\_size {#settings-max_query_size}

SQL ayrıştırıcısı ile ayrıştırmak için RAM'e alınabilecek bir sorgunun en büyük kısmı.
INSERT sorgusu, bu kısıtlamaya dahil olmayan ayrı bir akış ayrıştırıcısı (o(1) RAM tüketir) tarafından işlenen INSERT için veri de içerir.

Varsayılan değer: 256 KiB.

## ınteractive\_delay {#interactive-delay}

İstek yürütülmesinin iptal edilip edilmediğini kontrol etmek ve ilerlemeyi göndermek için mikrosaniye cinsinden Aralık.

Varsayılan değer: 100.000 (iptal için denetler ve ilerleme saniyede on kez gönderir).

## connect\_timeout, receıve\_tımeout, send\_timeout {#connect-timeout-receive-timeout-send-timeout}

İstemci ile iletişim kurmak için kullanılan sokette saniye cinsinden zaman aşımları.

Varsayılan değer: 10, 300, 300.

## cancel\_http\_readonly\_queries\_on\_client\_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Varsayılan değer: 0

## poll\_interval {#poll-interval}

Belirtilen saniye sayısı için bir bekleme döngüsünde kilitleyin.

Varsayılan değer: 10.

## max\_distributed\_connections {#max-distributed-connections}

Tek bir dağıtılmış tabloya tek bir sorgunun dağıtılmış işlenmesi için uzak sunucularla eşzamanlı bağlantı sayısı. Kümedeki sunucu sayısından daha az bir değer ayarlamanızı öneririz.

Varsayılan değer: 1024.

Aşağıdaki parametreler yalnızca dağıtılmış tablolar oluştururken (ve bir sunucu başlatırken) kullanılır, bu nedenle bunları çalışma zamanında değiştirmek için hiçbir neden yoktur.

## distributed\_connections\_pool\_size {#distributed-connections-pool-size}

Tüm sorguların tek bir dağıtılmış tabloya dağıtılmış işlenmesi için uzak sunucularla eşzamanlı bağlantıların maksimum sayısı. Kümedeki sunucu sayısından daha az bir değer ayarlamanızı öneririz.

Varsayılan değer: 1024.

## connect\_timeout\_with\_failover\_ms {#connect-timeout-with-failover-ms}

Dağıtılmış bir tablo altyapısı için uzak bir sunucuya bağlanmak için milisaniye cinsinden zaman aşımı ‘shard’ ve ‘replica’ bölümler küme tanımında kullanılır.
Başarısız olursa, çeşitli yinelemelere bağlanmak için birkaç deneme yapılır.

Varsayılan değer: 50.

## connections\_with\_failover\_max\_tries {#connections-with-failover-max-tries}

Dağıtılmış tablo altyapısı için her yineleme ile bağlantı girişimi sayısı.

Varsayılan değer: 3.

## çıkmaz {#extremes}

Aşırı değerleri (bir sorgu sonucunun sütunlarındaki minimum ve maksimum değerler) saymak ister. 0 veya 1 kabul eder. Varsayılan olarak, 0 (devre dışı).
Daha fazla bilgi için bölüme bakın “Extreme values”.

## use\_uncompressed\_cache {#setting-use_uncompressed_cache}

Sıkıştırılmamış blokların önbelleğinin kullanılıp kullanılmayacağı. 0 veya 1 kabul eder. Varsayılan olarak, 0 (devre dışı).
Sıkıştırılmamış önbelleği (yalnızca mergetree ailesindeki tablolar için) kullanmak, çok sayıda kısa Sorgu ile çalışırken gecikmeyi önemli ölçüde azaltabilir ve verimi artırabilir. Sık sık kısa istek Gönderen kullanıcılar için bu ayarı etkinleştirin. Ayrıca dikkat [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

En azından biraz büyük bir veri hacmi (bir milyon satır veya daha fazla) okuyan sorgular için sıkıştırılmamış önbellek, gerçekten küçük sorgular için yer kazanmak için otomatik olarak devre dışı bırakılır. Bu tutmak anlamına gelir ‘use\_uncompressed\_cache’ ayar her zaman 1 olarak ayarlanır.

## replace\_running\_query {#replace-running-query}

HTTP arayüzünü kullanırken, ‘query\_id’ parametre geçirilebilir. Bu, sorgu tanımlayıcısı olarak hizmet veren herhangi bir dizedir.
Aynı kullanıcıdan aynı sorgu varsa ‘query\_id’ zaten şu anda var, davranış bağlıdır ‘replace\_running\_query’ parametre.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query\_id’ zaten çalışan) var.

`1` – Cancel the old query and start running the new one.

Üye.Metrica, segmentasyon koşulları için öneriler uygulamak için 1 olarak ayarlanmış bu parametreyi kullanır. Bir sonraki karakteri girdikten sonra, eski sorgu henüz tamamlanmamışsa, iptal edilmelidir.

## stream\_flush\_interval\_ms {#stream-flush-interval-ms}

Bir zaman aşımı durumunda akışlı tablolar için çalışır veya bir iş parçacığı oluşturduğunda [max\_ınsert\_block\_size](#settings-max_insert_block_size) satırlar.

Varsayılan değer 7500'dür.

Küçük değer, daha sık veri tablosuna temizlendi. Değeri çok düşük ayarlamak, düşük performansa yol açar.

## dengeleme {#settings-load_balancing}

Dağıtılmış sorgu işleme için kullanılan yinelemeler seçimi algoritmasını belirtir.

ClickHouse kopyaları seçme aşağıdaki algoritmaları destekler:

-   [Rastgele](#load_balancing-random) (varsayılan olarak)
-   [En yakın hostnamename](#load_balancing-nearest_hostname)
-   [Sıralı](#load_balancing-in_order)
-   [İlk veya rastgele](#load_balancing-first_or_random)

### Rastgele (varsayılan olarak) {#load_balancing-random}

``` sql
load_balancing = random
```

Her yineleme için hata sayısı sayılır. Sorgu, en az hata ile çoğaltmaya gönderilir ve bunlardan birkaçı varsa, bunlardan herhangi birine gönderilir.
Dezavantajları: sunucu yakınlık hesaba değil; kopyaları farklı veri varsa, aynı zamanda farklı veri alırsınız.

### En Yakın Hostnamename {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

Örneğin, example01-01-1 ve example01-01-2.yandex.ru bir pozisyonda farklıdır, örneği01-01-1 ve örneği01-02-2 iki yerde farklılık gösterir.
Bu yöntem ilkel görünebilir, ancak ağ topolojisi hakkında harici veri gerektirmez ve IPv6 adreslerimiz için karmaşık olan IP adreslerini karşılaştırmaz.

Bu nedenle, eşdeğer yinelemeler varsa, isme göre en yakın olanı tercih edilir.
Aynı sunucuya bir sorgu gönderirken, arızaların yokluğunda, dağıtılmış bir sorgunun da aynı sunuculara gideceğini varsayabiliriz. Bu nedenle, yinelemelere farklı veriler yerleştirilse bile, sorgu çoğunlukla aynı sonuçları döndürür.

### Sıralı {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Yapılandırmada belirtilen hataları aynı sayıda yinelemeler aynı sırayla erişilir.
Bu yöntem, tam olarak hangi kopyanın tercih edildiğini bildiğinizde uygundur.

### İlk veya rastgele {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

İlk kullanılamaz, bu algoritma kümesindeki ilk yineleme veya rasgele bir yineleme seçer. Çapraz çoğaltma topolojisi kurulumlarında etkilidir, ancak diğer yapılandırmalarda işe yaramaz.

Bu `first_or_random` algoritma sorunu çözer `in_order` algoritma. İle `in_order`, bir çoğaltma aşağı giderse, kalan yinelemeler normal trafik miktarını işlerken bir sonraki bir çift yük alır. Kullanırken `first_or_random` algoritma, yük hala mevcut olan kopyalar arasında eşit olarak dağıtılır.

## prefer\_localhost\_replica {#settings-prefer-localhost-replica}

Etkinleştirir / devre dışı bırakır tercih kullanarak localhost çoğaltma dağıtılmış sorguları işlerken.

Olası değerler:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [dengeleme](#settings-load_balancing) ayar.

Varsayılan değer: 1.

!!! warning "Uyarıcı"
    Kullanıyorsanız bu ayarı devre dışı bırakın [max\_parallel\_replicas](#settings-max_parallel_replicas).

## totals\_mode {#totals-mode}

MAX\_ROWS\_TO\_GROUP\_BY ve group\_by\_overflow\_mode = ‘any’ Bulunmak.
Bölümüne bakınız “WITH TOTALS modifier”.

## totals\_auto\_threshold {#totals-auto-threshold}

İçin eşik `totals_mode = 'auto'`.
Bölümüne bakınız “WITH TOTALS modifier”.

## max\_parallel\_replicas {#settings-max_parallel_replicas}

Bir sorgu yürütülürken her parça için en fazla yineleme sayısı.
Tutarlılık için (aynı veri bölünmesinin farklı bölümlerini elde etmek için), bu seçenek yalnızca örnekleme anahtarı ayarlandığında çalışır.
Çoğaltma gecikme denetlenmez.

## derlemek {#compile}

Sorguların derlenmesini etkinleştirin. Varsayılan olarak, 0 (devre dışı).

Derleme yalnızca sorgu işleme boru hattının bir parçası için kullanılır: toplamanın ilk aşaması için (GROUP BY).
Potansiyel hattın bu bölümü derlenmişse, sorgu, kısa döngüleri ve inlining toplu işlev çağrılarının dağıtımı nedeniyle daha hızlı çalışabilir. Birden çok basit toplama işlevine sahip sorgular için maksimum performans artışı (nadir durumlarda dört kata kadar daha hızlı) görülür. Tipik olarak, performans kazancı önemsİzdİr. Çok nadir durumlarda, sorgu yürütülmesini yavaşlatabilir.

## min\_count\_to\_compile {#min-count-to-compile}

Derleme çalıştırmadan önce derlenmiş bir kod yığını potansiyel olarak kaç kez kullanılır. Varsayılan olarak, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
Değer 1 veya daha fazla ise, derleme zaman uyumsuz olarak ayrı bir iş parçacığında oluşur. Sonuç, şu anda çalışmakta olan sorgular da dahil olmak üzere hazır olduğu anda kullanılacaktır.

Derlenmiş kod, sorguda kullanılan toplama işlevlerinin her farklı birleşimi ve GROUP BY yan tümcesindeki anahtarların türü için gereklidir.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output\_format\_json\_quote\_64bit\_integers {#session_settings-output_format_json_quote_64bit_integers}

Değer doğruysa, json\* Int64 ve Uİnt64 formatlarını kullanırken tamsayılar tırnak içinde görünür (çoğu JavaScript uygulamasıyla uyumluluk için); aksi takdirde, tamsayılar tırnak işaretleri olmadan çıktılanır.

## format\_csv\_delimiter {#settings-format_csv_delimiter}

Karakter CSV verilerinde bir sınırlayıcı olarak yorumlanır. Varsayılan olarak, sınırlayıcı `,`.

## ınput\_format\_csv\_unquoted\_null\_literal\_as\_null {#settings-input_format_csv_unquoted_null_literal_as_null}

CSV giriş biçimi sağlar veya unquoted ayrıştırma devre dışı bırakır için `NULL` literal olarak (eşanlamlı `\N`).

## output\_format\_csv\_crlf\_end\_of\_line {#settings-output-format-csv-crlf-end-of-line}

Unix stili (LF) yerine CSV'DE DOS/Windows stili çizgi ayırıcı (CRLF) kullanın.

## output\_format\_tsv\_crlf\_end\_of\_line {#settings-output-format-tsv-crlf-end-of-line}

Unıx stili (LF) yerine TSV'DE DOC/Windows stili çizgi ayırıcı (CRLF) kullanın.

## insert\_quorum {#settings-insert_quorum}

Çekirdek yazma sağlar.

-   Eğer `insert_quorum < 2`, çekirdek yazma devre dışı bırakılır.
-   Eğer `insert_quorum >= 2`, çekirdek yazma etkin.

Varsayılan değer: 0.

Nis writesap yazar

`INSERT` yalnızca ClickHouse verileri doğru bir şekilde yazmayı başardığında başarılı olur. `insert_quorum` sırasında kopya ofların `insert_quorum_timeout`. Herhangi bir nedenle başarılı yazma ile kopya sayısı ulaşmazsa `insert_quorum`, yazma başarısız olarak kabul edilir ve ClickHouse, verilerin zaten yazıldığı tüm kopyalardan eklenen bloğu siler.

Nisaptaki tüm kopyalar tutarlıdır, yani önceki tüm verileri içerir `INSERT` sorgular. Bu `INSERT` sıra doğrusallaştırılmıştır.

Yazılan verileri okurken `insert_quorum` olabilir kullanın [select\_sequential\_consistency](#settings-select_sequential_consistency) seçenek.

ClickHouse bir istisna oluşturur

-   Sorgu sırasında kullanılabilir yinelemelerin sayısı daha az ise `insert_quorum`.
-   Önceki blok henüz eklenmemiş olduğunda veri yazma girişiminde `insert_quorum` kopyaların. Bu durum, bir kullanıcı gerçekleştirmeye çalışırsa oluşabilir. `INSERT` ile bir öncekinden önce `insert_quorum` Tamam islanmıştır.

Ayrıca bakınız:

-   [ınsert\_quorum\_timeout](#settings-insert_quorum_timeout)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## ınsert\_quorum\_timeout {#settings-insert_quorum_timeout}

Çekirdek zaman aşımına saniyeler içinde yazın. Zaman aşımı geçti ve yazma henüz gerçekleşmedi, ClickHouse bir özel durum oluşturur ve istemci aynı bloğu aynı veya başka bir yineleme yazmak için sorguyu yinelemeniz gerekir.

Varsayılan değer: 60 saniye.

Ayrıca bakınız:

-   [insert\_quorum](#settings-insert_quorum)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## select\_sequential\_consistency {#settings-select_sequential_consistency}

İçin sıralı tutarlılığı etkinleştirir veya devre dışı bırakır `SELECT` sorgular:

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

Kullanma

Sıralı tutarlılık etkinleştirildiğinde, clickhouse istemci çalıştırmak sağlar `SELECT` yalnızca önceki tüm verileri içeren yinelemeler için sorgu `INSERT` ile yürütülen sorgular `insert_quorum`. Istemci kısmi bir yineleme başvurursa, ClickHouse bir özel durum oluşturur. SELECT sorgusu yinelemeler çekirdek için henüz yazılmamış verileri içermez.

Ayrıca bakınız:

-   [insert\_quorum](#settings-insert_quorum)
-   [ınsert\_quorum\_timeout](#settings-insert_quorum_timeout)

## ınsert\_deduplicate {#settings-insert-deduplicate}

Blok tekilleştirmesini etkinleştirir veya devre dışı bırakır `INSERT` (çoğaltılmış \* tablolar için).

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 1.

Varsayılan olarak, çoğaltılmış tablolara eklenen bloklar `INSERT` açıklama tekilleştirilmiştir (bkz [Veri Çoğaltma](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate\_blocks\_ın\_dependent\_materialized\_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Yinelenmiş\* tablolardan veri alan materialized görünümler için tekilleştirme denetimini etkinleştirir veya devre dışı bırakır.

Olası değerler:

      0 — Disabled.
      1 — Enabled.

Varsayılan değer: 0.

Kullanma

Varsayılan olarak, tekilleştirme materialized görünümler için gerçekleştirilmez, ancak kaynak tabloda, Yukarı akış yapılır.
Eklenen bir blok, kaynak tablodaki tekilleştirme nedeniyle atlanırsa, ekli materialized görünümlerine ekleme olmaz. Bu davranış, eklenen blokların materialized görünüm toplamasından sonra aynı olduğu, ancak kaynak tabloya farklı eklerden türetildiği durumlar için, yüksek oranda toplanmış verilerin materialized görünümlere eklenmesini sağlamak için vardır.
Aynı zamanda, bu davranış “breaks” `INSERT` idempotency. Eğer bir `INSERT` ana tabloya başarılı oldu ve `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` bu davranışı değiştirmeye izin verir. Yeniden denemede, somutlaştırılmış bir görünüm tekrar ekleme işlemini alacak ve tekilleştirme kontrolünü kendi başına gerçekleştirecektir,
kaynak tablo için onay sonucunu yoksayar ve ilk hata nedeniyle kaybedilen satırları ekler.

## max\_network\_bytes {#settings-max-network-bytes}

Alınan veya bir sorgu yürütülürken ağ üzerinden iletilen veri birimi (bayt cinsinden) sınırlar. Bu ayar, her bir sorgu için geçerlidir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Data volume control is disabled.

Varsayılan değer: 0.

## max\_network\_bandwidth {#settings-max-network-bandwidth}

Ağ üzerinden veri alışverişinin hızını saniyede bayt cinsinden sınırlar. Bu ayar her sorgu için geçerlidir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Bandwidth control is disabled.

Varsayılan değer: 0.

## max\_network\_bandwidth\_for\_user {#settings-max-network-bandwidth-for-user}

Ağ üzerinden veri alışverişinin hızını saniyede bayt cinsinden sınırlar. Bu ayar, tek bir kullanıcı tarafından gerçekleştirilen tüm aynı anda çalışan sorgular için geçerlidir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Control of the data speed is disabled.

Varsayılan değer: 0.

## max\_network\_bandwidth\_for\_all\_users {#settings-max-network-bandwidth-for-all-users}

Verilerin ağ üzerinden saniyede bayt olarak değiştirildiği hızı sınırlar. Bu ayar, sunucuda aynı anda çalışan tüm sorgular için geçerlidir.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Control of the data speed is disabled.

Varsayılan değer: 0.

## count\_distinct\_implementation {#settings-count_distinct_implementation}

Aşağıdakilerden hang theisinin `uniq*` işlevleri gerçekleştirmek için kullanılmalıdır [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) yapı.

Olası değerler:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

Varsayılan değer: `uniqExact`.

## skip\_unavailable\_shards {#settings-skip_unavailable_shards}

Etkinleştirir veya sessizce kullanılamaz kırıkları atlama devre dışı bırakır.

Tüm kopyaları kullanılamıyorsa, Shard kullanılamaz olarak kabul edilir. Aşağıdaki durumlarda bir yineleme kullanılamaz:

-   ClickHouse herhangi bir nedenle kopya bağlanamıyor.

    Bir kopyaya bağlanırken, ClickHouse birkaç deneme gerçekleştirir. Tüm bu girişimler başarısız olursa, çoğaltma kullanılamaz kabul edilir.

-   Çoğaltma DNS üzerinden çözülemez.

    Çoğaltmanın ana bilgisayar adı DNS aracılığıyla çözümlenemezse, aşağıdaki durumları gösterebilir:

    -   Çoğaltma ana bilgisayar DNS kaydı yok. Dinamik DNS'YE sahip sistemlerde oluşabilir, örneğin, [Kubernetes](https://kubernetes.io), burada düğümler kesinti sırasında çözülmez olabilir ve bu bir hata değildir.

    -   Yapılandırma hatası. ClickHouse yapılandırma dosyası yanlış bir ana bilgisayar adı içerir.

Olası değerler:

-   1 — skipping enabled.

    Bir parça kullanılamıyorsa, ClickHouse kısmi verilere dayalı bir sonuç döndürür ve düğüm kullanılabilirliği sorunlarını bildirmez.

-   0 — skipping disabled.

    Bir shard kullanılamıyorsa, ClickHouse bir özel durum atar.

Varsayılan değer: 0.

## optimize\_skip\_unused\_shards {#settings-optimize_skip_unused_shards}

Prewhere/WHERE (verilerin sharding anahtarı tarafından dağıtıldığını varsayar, aksi takdirde hiçbir şey yapmaz).

Varsayılan değer: 0

## force\_optimize\_skip\_unused\_shards {#settings-force_optimize_skip_unused_shards}

Sorgu yürütülmesini etkinleştirir veya devre dışı bırakır [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) etkin ve kullanılmayan kırıkları atlama mümkün değildir. Atlama mümkün değilse ve ayar etkinse özel durum atılır.

Olası değerler:

-   0-Devre Dışı (at notmayın)
-   1-sorgu yürütülmesini yalnızca tablonun sharding anahtarı varsa devre dışı bırakın
-   2-devre dışı sorgu yürütme ne olursa olsun sharding anahtar tablo için tanımlanır

Varsayılan değer: 0

## optimize\_throw\_if\_noop {#setting-optimize_throw_if_noop}

Bir özel durum atmayı etkinleştirir veya devre dışı bırakır. [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) sorgu birleştirme gerçekleştirmedi.

Varsayılan olarak, `OPTIMIZE` eğer hiç bir şey yapmamış olsa bile, başarılı bir şekilde verir. Bu ayar, bu durumları ayırt etmenizi ve bir özel durum iletisinde nedeni almanızı sağlar.

Olası değerler:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

Varsayılan değer: 0.

## distributed\_replica\_error\_half\_life {#settings-distributed_replica_error_half_life}

-   Türü: saniye
-   Varsayılan değer: 60 saniye

Dağıtılmış tablolardaki hataların ne kadar hızlı sıfırlandığını denetler. Bir yineleme bir süre için kullanılamıyorsa, 5 hataları biriktirir ve distributed\_replica\_error\_half\_lıfe 1 saniye olarak ayarlanır, sonra yineleme son hatadan sonra normal 3 saniye olarak kabul edilir.

Ayrıca bakınız:

-   [Masa motoru Dağıt Distributedıldı](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap](#settings-distributed_replica_error_cap)

## distributed\_replica\_error\_cap {#settings-distributed_replica_error_cap}

-   Tür: imzasız int
-   Varsayılan değer: 1000

Her yineleme hata sayısı çok fazla hata biriken tek bir yineleme engelleyerek, bu değerle kaplıdır.

Ayrıca bakınız:

-   [Masa motoru Dağıt Distributedıldı](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_half\_life](#settings-distributed_replica_error_half_life)

## distributed\_directory\_monitor\_sleep\_time\_ms {#distributed_directory_monitor_sleep_time_ms}

İçin taban aralığı [Dağılı](../../engines/table-engines/special/distributed.md) veri göndermek için tablo motoru. Gerçek Aralık, hatalar durumunda katlanarak büyür.

Olası değerler:

-   Milisaniye pozitif tamsayı sayısı.

Varsayılan değer: 100 milisaniye.

## distributed\_directory\_monitor\_max\_sleep\_time\_ms {#distributed_directory_monitor_max_sleep_time_ms}

İçin Maksimum Aralık [Dağılı](../../engines/table-engines/special/distributed.md) veri göndermek için tablo motoru. Sınırları içinde belirlenen Aralık üstel büyüme [distributed\_directory\_monitor\_sleep\_time\_ms](#distributed_directory_monitor_sleep_time_ms) ayar.

Olası değerler:

-   Milisaniye pozitif tamsayı sayısı.

Varsayılan değer: 30000 milisaniye (30 saniye).

## distributed\_directory\_monitor\_batch\_ınserts {#distributed_directory_monitor_batch_inserts}

Eklenen verilerin toplu olarak gönderilmesini etkinleştirir / devre dışı bırakır.

Toplu gönderme etkinleştirildiğinde, [Dağılı](../../engines/table-engines/special/distributed.md) table engine, eklenen verilerin birden çok dosyasını ayrı ayrı göndermek yerine tek bir işlemde göndermeye çalışır. Toplu gönderme, sunucu ve ağ kaynaklarını daha iyi kullanarak küme performansını artırır.

Olası değerler:

-   1 — Enabled.
-   0 — Disabled.

Varsayılan değer: 0.

## os\_thread\_priority {#setting-os-thread-priority}

Önceliği ayarlar ([güzel](https://en.wikipedia.org/wiki/Nice_(Unix))) sorguları yürüten iş parçacıkları için. İşletim sistemi Zamanlayıcısı, kullanılabilir her CPU çekirdeğinde çalışacak bir sonraki iş parçacığını seçerken bu önceliği dikkate alır.

!!! warning "Uyarıcı"
    Bu ayarı kullanmak için, `CAP_SYS_NICE` özellik. Bu `clickhouse-server` paket kurulum sırasında kurar. Bazı sanal ortamlar ayarlamanıza izin vermez `CAP_SYS_NICE` özellik. Bu durumda, `clickhouse-server` Başlangıçta bu konuda bir mesaj gösterir.

Olası değerler:

-   Aralıktaki değerleri ayarlayabilirsiniz `[-20, 19]`.

Daha düşük değerler daha yüksek öncelik anlamına gelir. Düşük olan iplikler `nice` öncelik değerleri, yüksek değerlere sahip iş parçacıklarından daha sık yürütülür. Yüksek değerler, uzun süren etkileşimli olmayan sorgular için tercih edilir, çünkü geldiklerinde kısa etkileşimli sorgular lehine kaynakları hızlı bir şekilde bırakmalarına izin verir.

Varsayılan değer: 0.

## query\_profiler\_real\_time\_period\_ns {#query_profiler_real_time_period_ns}

Gerçek bir saat zamanlayıcı için süreyi ayarlar [sorgu profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Gerçek saat zamanlayıcı duvar saati zaman sayar.

Olası değerler:

-   Nanosaniye cinsinden pozitif tam sayı.

    Önerilen değerler:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   Zamanlayıcıyı kapatmak için 0.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

Varsayılan değer: 1000000000 nanosaniye (saniyede bir kez).

Ayrıca bakınız:

-   Sistem tablosu [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## query\_profiler\_cpu\_time\_period\_ns {#query_profiler_cpu_time_period_ns}

Bir CPU saat süreölçerinin dönemini ayarlar. [sorgu profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Bu zamanlayıcı sadece CPU süresini sayar.

Olası değerler:

-   Nanosaniye pozitif tamsayı sayısı.

    Önerilen değerler:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   Zamanlayıcıyı kapatmak için 0.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

Varsayılan değer: 1000000000 nanosaniye.

Ayrıca bakınız:

-   Sistem tablosu [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## allow\_introspection\_functions {#settings-allow_introspection_functions}

Devre dışı bırakmayı etkinleştirir [ıntrospections fonksiyonları](../../sql-reference/functions/introspection.md) sorgu profilleme için.

Olası değerler:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

Varsayılan değer: 0.

**Ayrıca Bakınız**

-   [Örnekleme Sorgusu Profiler](../optimizing-performance/sampling-query-profiler.md)
-   Sistem tablosu [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## ınput\_format\_parallel\_parsing {#input-format-parallel-parsing}

-   Tipi: bool
-   Varsayılan değer: True

Veri biçimlerinin paralel ayrıştırma sırasını koruyarak etkinleştirin. Sadece TSV, TKSV, CSV ve JSONEachRow formatları için desteklenir.

## min\_chunk\_bytes\_for\_parallel\_parsing {#min-chunk-bytes-for-parallel-parsing}

-   Tür: imzasız int
-   Varsayılan değer: 1 MiB

Her iş parçacığının paralel olarak ayrıştırılacağı bayt cinsinden minimum yığın boyutu.

## output\_format\_avro\_codec {#settings-output_format_avro_codec}

Çıkış Avro dosyası için kullanılan sıkıştırma codec ayarlar.

Tipi: dize

Olası değerler:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [Çabuk](https://google.github.io/snappy/)

Varsayılan değer: `snappy` (varsa) veya `deflate`.

## output\_format\_avro\_sync\_interval {#settings-output_format_avro_sync_interval}

Çıkış Avro dosyası için senkronizasyon işaretçileri arasında minimum veri boyutunu (bayt cinsinden) ayarlar.

Tür: imzasız int

Olası değerler: 32 (32 bayt) - 1073741824 (1 GiB)

Varsayılan değer: 32768 (32 KiB)

## format\_avro\_schema\_registry\_url {#settings-format_avro_schema_registry_url}

Sets Confluent Schema Registry URL to use with [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) biçimli

Type: URL

Varsayılan değer: boş

## background\_pool\_size {#background_pool_size}

Tablo altyapılarında arka plan işlemlerini gerçekleştiren iş parçacıklarının sayısını ayarlar (örneğin, [MergeTree motoru](../../engines/table-engines/mergetree-family/index.md) Tablolar). Bu ayar ClickHouse sunucu başlangıcında uygulanır ve bir kullanıcı oturumunda değiştirilemez. Bu ayarı ayarlayarak, CPU ve disk yükünü yönetirsiniz. Daha küçük havuz boyutu daha az CPU ve disk kaynağı kullanır, ancak arka plan işlemleri daha yavaş ilerler ve bu da sorgu performansını etkileyebilir.

Olası değerler:

-   Herhangi bir pozitif tamsayı.

Varsayılan değer: 16.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
