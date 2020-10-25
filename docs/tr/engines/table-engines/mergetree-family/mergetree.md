---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: MergeTree
---

# MergeTree {#table_engines-mergetree}

Bu `MergeTree` bu ailenin motoru ve diğer motorları (`*MergeTree`) en sağlam ClickHouse masa motorlarıdır.

Motor inlarda `MergeTree` aile, bir tabloya çok büyük miktarda veri eklemek için tasarlanmıştır. Veriler hızlı bir şekilde tabloya kısmen yazılır, daha sonra parçaları arka planda birleştirmek için kurallar uygulanır. Bu yöntem, ınsert sırasında depolama alanındaki verileri sürekli olarak yeniden yazmaktan çok daha etkilidir.

Ana özellikler:

-   Birincil anahtara göre sıralanmış verileri saklar.

    Bu, verileri daha hızlı bulmanıza yardımcı olan küçük bir seyrek dizin oluşturmanıza olanak sağlar.

-   Bölümler eğer kullanılabilir [bölümleme anahtarı](custom-partitioning-key.md) belirt .ilmektedir.

    ClickHouse, aynı sonuçla aynı veriler üzerindeki genel işlemlerden daha etkili olan bölümlerle belirli işlemleri destekler. ClickHouse, bölümleme anahtarının sorguda belirtildiği bölüm verilerini de otomatik olarak keser. Bu da sorgu performansını artırır.

-   Veri çoğaltma desteği.

    The family of `ReplicatedMergeTree` tablolar veri çoğaltma sağlar. Daha fazla bilgi için, bkz. [Veri çoğaltma](replication.md).

-   Veri örnekleme desteği.

    Gerekirse, tabloda veri örnekleme yöntemini ayarlayabilirsiniz.

!!! info "Bilgin"
    Bu [Birleştirmek](../special/merge.md#merge) motor ait değil `*MergeTree` aile.

## Tablo oluşturma {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
```

Parametrelerin açıklaması için bkz. [Sorgu açıklaması oluştur](../../../sql-reference/statements/create.md).

!!! note "Not"
    `INDEX` deneysel bir özelliktir, bkz [Veri Atlama Dizinleri](#table_engine-mergetree-data_skipping-indexes).

### Sorgu Yan Tümceleri {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. Bu `MergeTree` motor parametreleri yok.

-   `PARTITION BY` — The [bölümleme anahtarı](custom-partitioning-key.md).

    Aylara göre bölümleme için `toYYYYMM(date_column)` ifade, nerede `date_column` türün tarihi olan bir sütun mu [Tarihli](../../../sql-reference/data-types/date.md). Burada bölüm isimleri var `"YYYYMM"` biçimli.

-   `ORDER BY` — The sorting key.

    Sütun veya keyfi ifadeler bir tuple. Örnek: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [sıralama anahtarından farklıdır](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    Varsayılan olarak, birincil anahtar sıralama anahtarıyla aynıdır (bu anahtar tarafından belirtilir). `ORDER BY` yan). Bu nedenle çoğu durumda ayrı bir belirtmek gereksizdir `PRIMARY KEY` yan.

-   `SAMPLE BY` — An expression for sampling.

    Bir örnekleme ifadesi kullanılırsa, birincil anahtar onu içermelidir. Örnek: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [diskler ve birimler arasında](#table_engine-mergetree-multiple-volumes).

    İfade bir olmalıdır `Date` veya `DateTime` sonuç olarak sütun. Örnek:
    `TTL date + INTERVAL 1 DAY`

    Kuralın türü `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` ifade tatmin edildiyse (geçerli zamana ulaşırsa) parça ile yapılacak bir eylemi belirtir: süresi dolmuş satırların kaldırılması, bir parçanın (bir parçadaki tüm satırlar için ifade tatmin edildiyse) belirtilen diske taşınması (`TO DISK 'xxx'`) veya hacim (`TO VOLUME 'xxx'`). Kuralın varsayılan türü kaldırma (`DELETE`). Birden fazla kural listesi belirtilebilir, ancak birden fazla olmamalıdır `DELETE` kural.

    Daha fazla ayrıntı için bkz. [Sütunlar ve tablolar için TTL](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [Veri Depolama](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [Veri Depolama](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` ayar. 19.11 sürümünden önce, sadece `index_granularity` granül boyutunu kısıtlamak için ayar. Bu `index_granularity_bytes` büyük satırlar (onlarca ve megabayt yüzlerce) ile tablolardan veri seçerken ayarı ClickHouse performansını artırır. Büyük satırlara sahip tablolarınız varsa, tabloların verimliliğini artırmak için bu ayarı etkinleştirebilirsiniz. `SELECT` sorgular.
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`, daha sonra ZooKeeper daha az veri depolar. Daha fazla bilgi için, bkz: [ayar açıklaması](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) içinde “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` bayt, ClickHouse okur ve doğrudan I/O arabirimi kullanarak depolama diskine veri yazar (`O_DIRECT` seçenek). Eğer `min_merge_bytes_to_use_direct_io = 0`, sonra doğrudan g / Ç devre dışı bırakılır. Varsayılan değer: `10 * 1024 * 1024 * 1024` baytlar.
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don't turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [Veri depolama için birden fazla blok cihazı kullanma](#table_engine-mergetree-multiple-volumes).

**Bölüm ayarı örneği**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

Örnekte, aylara göre bölümleme ayarladık.

Biz de kullanıcı kimliği ile karma olarak örnekleme için bir ifade ayarlayın. Bu, her biri için tablodaki verileri pseudorandomize etmenizi sağlar `CounterID` ve `EventDate`. Tanım yoularsanız bir [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) yan tümcesi verileri seçerken, ClickHouse kullanıcıların bir alt kümesi için eşit pseudorandom veri örneği döndürür.

Bu `index_granularity` 8192 varsayılan değer olduğundan ayarı atlanabilir.

<details markdown="1">

<summary>Bir tablo oluşturmak için kullanımdan kaldırılan yöntem</summary>

!!! attention "Dikkat"
    Bu yöntemi yeni projelerde kullanmayın. Mümkünse, eski projeleri yukarıda açıklanan yönteme geçin.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree () Parametreleri**

-   `date-column` — The name of a column of the [Tarihli](../../../sql-reference/data-types/date.md) tür. ClickHouse otomatik olarak bu sütuna göre ay bölümleri oluşturur. Bölüm adları `"YYYYMM"` biçimli.
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [Demet()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” bir dizinin. 8192 değeri çoğu görev için uygundur.

**Örnek**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

Bu `MergeTree` motor, Ana motor yapılandırma yöntemi için yukarıdaki örnekte olduğu gibi yapılandırılır.
</details>

## Veri Depolama {#mergetree-data-storage}

Bir tabloda birincil anahtar tarafından sıralanmış verileri bölümden oluşmaktadır.

Veri bir tabloya eklendiğinde, ayrı veri parçaları oluşturulur ve bunların her biri birincil anahtara göre lexicographically sıralanır. Örneğin, birincil anahtar `(CounterID, Date)`, parçadaki veriler şu şekilde sıralanır `CounterID` ve içinde her `CounterID` tarafından sipariş edilir `Date`.

Farklı bölümlere ait veriler farklı parçalara ayrılır. Arka planda, ClickHouse daha verimli depolama için veri parçalarını birleştirir. Farklı bölümlere ait parçalar birleştirilmez. Birleştirme mekanizması, aynı birincil anahtara sahip tüm satırların aynı veri bölümünde olacağını garanti etmez.

Her veri parçası mantıksal olarak granüllere ayrılmıştır. Bir granül, Clickhouse'un veri seçerken okuduğu en küçük bölünmez veri kümesidir. ClickHouse satırları veya değerleri bölmez, bu nedenle her granül her zaman bir tamsayı satır içerir. Bir granülün ilk satırı, satır için birincil anahtarın değeri ile işaretlenir. Her veri bölümü için ClickHouse işaretleri depolayan bir dizin dosyası oluşturur. Her sütun için, birincil anahtarda olsun ya da olmasın, ClickHouse aynı işaretleri de saklar. Bu işaretler, verileri doğrudan sütun dosyalarında bulmanızı sağlar.

Granül boyutu ile sınırlıdır `index_granularity` ve `index_granularity_bytes` tablo motorunun ayarları. Bir granüldeki satır sayısı `[1, index_granularity]` Aralık, satırların boyutuna bağlı olarak. Bir granülün boyutu aşabilir `index_granularity_bytes` tek bir satırın boyutu ayarın değerinden büyükse. Bu durumda, granülün boyutu satırın boyutuna eşittir.

## Sorgularda birincil anahtarlar ve dizinler {#primary-keys-and-indexes-in-queries}

Tak thee the `(CounterID, Date)` örnek olarak birincil anahtar. Bu durumda, sıralama ve dizin aşağıdaki gibi gösterilebilir:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

Veri sorgusu belirtirse:

-   `CounterID in ('a', 'h')`, sunucu işaretleri aralıklarında verileri okur `[0, 3)` ve `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`, sunucu işaretleri aralıklarında verileri okur `[1, 3)` ve `[7, 8)`.
-   `Date = 3`, sunucu işaretleri aralığında veri okur `[1, 10]`.

Yukarıdaki örnekler, her zaman bir dizin tam taramadan daha etkili olduğunu göstermektedir.

Seyrek bir dizin, ekstra verilerin okunmasına izin verir. Birincil anahtarın tek bir aralığını okurken, `index_granularity * 2` her veri bloğundaki ekstra satırlar okunabilir.

Seyrek dizinler, çok sayıda tablo satırı ile çalışmanıza izin verir, çünkü çoğu durumda, bu tür dizinler bilgisayarın RAM'İNE sığar.

ClickHouse benzersiz bir birincil anahtar gerektirmez. Aynı birincil anahtar ile birden çok satır ekleyebilirsiniz.

### Birincil anahtar seçme {#selecting-the-primary-key}

Birincil anahtardaki sütun sayısı açıkça sınırlı değildir. Veri yapısına bağlı olarak, birincil anahtara daha fazla veya daha az sütun ekleyebilirsiniz. Bu Mayıs:

-   Bir dizin performansını artırın.

    Birincil anahtar ise `(a, b)`, sonra başka bir sütun ekleyerek `c` aşağıdaki koşullar yerine getirilirse performansı artıracaktır:

    -   Sütun üzerinde bir koşulu olan sorgular var `c`.
    -   Uzun veri aralıkları (birkaç kat daha uzun `index_granularity`) için aynı değer withlerle `(a, b)` yaygındır. Başka bir deyişle, başka bir sütun eklerken oldukça uzun veri aralıklarını atlamanıza izin verir.

-   Veri sıkıştırmasını geliştirin.

    ClickHouse verileri birincil anahtarla sıralar, bu nedenle tutarlılık ne kadar yüksek olursa sıkıştırma o kadar iyi olur.

-   Veri parçalarını birleştirirken ek mantık sağlayın [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) ve [SummingMergeTree](summingmergetree.md) motorlar.

    Bu durumda belirtmek mantıklı *sıralama anahtarı* bu birincil anahtardan farklıdır.

Uzun bir birincil anahtar, ekleme performansını ve bellek tüketimini olumsuz yönde etkiler, ancak birincil anahtardaki ek sütunlar, ClickHouse performansını etkilemez `SELECT` sorgular.

### Sıralama anahtarından farklı bir birincil anahtar seçme {#choosing-a-primary-key-that-differs-from-the-sorting-key}

Sıralama anahtarından (veri bölümlerindeki satırları sıralamak için bir ifade) farklı bir birincil anahtar (her işaret için dizin dosyasında yazılan değerlere sahip bir ifade) belirtmek mümkündür. Bu durumda, birincil anahtar ifadesi tuple, sıralama anahtarı ifadesi tuple'ın bir öneki olmalıdır.

Bu özellik kullanırken yararlıdır [SummingMergeTree](summingmergetree.md) ve
[AggregatingMergeTree](aggregatingmergetree.md) masa motorları. Bu motorları kullanırken yaygın bir durumda, tablonun iki tür sütunu vardır: *boyutlar* ve *ölçümler*. Tipik sorgular, rasgele ölçü sütunlarının değerlerini toplar `GROUP BY` ve boyutlara göre filtreleme. Çünkü SummingMergeTree ve AggregatingMergeTree sıralama anahtarının aynı değere sahip satırları toplamak, tüm boyutları eklemek doğaldır. Sonuç olarak, anahtar ifadesi uzun bir sütun listesinden oluşur ve bu liste yeni eklenen boyutlarla sık sık güncelleştirilmelidir.

Bu durumda, birincil anahtarda verimli Aralık taramaları sağlayacak ve kalan boyut sütunlarını sıralama anahtarı kümesine ekleyecek yalnızca birkaç sütun bırakmak mantıklıdır.

[ALTER](../../../sql-reference/statements/alter.md) yeni bir sütun aynı anda tabloya ve sıralama anahtarı eklendiğinde, varolan veri parçaları değiştirilmesi gerekmez, çünkü sıralama anahtarının hafif bir işlemdir. Eski sıralama anahtarı yeni sıralama anahtarının bir öneki olduğundan ve yeni eklenen sütunda veri olmadığından, veriler tablo değişikliği anında hem eski hem de yeni sıralama anahtarlarına göre sıralanır.

### Sorgularda dizin ve bölümlerin kullanımı {#use-of-indexes-and-partitions-in-queries}

İçin `SELECT` sorgular, ClickHouse bir dizin kullanılabilir olup olmadığını analiz eder. Eğer bir dizin kullanılabilir `WHERE/PREWHERE` yan tümce, bir eşitlik veya eşitsizlik karşılaştırma işlemini temsil eden bir ifadeye (bağlantı öğelerinden biri olarak veya tamamen) sahiptir veya varsa `IN` veya `LIKE` sütun veya birincil anahtar veya bölümleme anahtar veya bu sütunların belirli kısmen tekrarlayan işlevleri veya bu ifadelerin mantıksal ilişkileri olan ifadeler üzerinde sabit bir önek ile.

Bu nedenle, birincil anahtarın bir veya daha fazla aralığındaki sorguları hızlı bir şekilde çalıştırmak mümkündür. Bu örnekte, belirli bir izleme etiketi, belirli bir etiket ve tarih aralığı, belirli bir etiket ve tarih için, tarih aralığına sahip birden çok etiket için vb. çalıştırıldığında sorgular hızlı olacaktır.

Aşağıdaki gibi yapılandırılmış motora bakalım:

      ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

Bu durumda, sorgularda:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse, uygun olmayan verileri kırpmak için birincil anahtar dizinini ve uygun olmayan tarih aralıklarındaki bölümleri kırpmak için aylık bölümleme anahtarını kullanır.

Yukarıdaki sorgular, dizinin karmaşık ifadeler için bile kullanıldığını göstermektedir. Tablodan okuma, dizini kullanarak tam taramadan daha yavaş olamayacak şekilde düzenlenmiştir.

Aşağıdaki örnekte, dizin kullanılamaz.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Clickhouse'un bir sorgu çalıştırırken dizini kullanıp kullanamayacağını kontrol etmek için ayarları kullanın [force\_index\_by\_date](../../../operations/settings/settings.md#settings-force_index_by_date) ve [force\_primary\_key](../../../operations/settings/settings.md).

Aylara göre bölümleme anahtarı, yalnızca uygun aralıktaki tarihleri içeren veri bloklarını okumanıza izin verir. Bu durumda, veri bloğu birçok tarih için veri içerebilir (bir aya kadar). Bir blok içinde veriler, ilk sütun olarak tarihi içermeyen birincil anahtara göre sıralanır. Bu nedenle, birincil anahtar önekini belirtmeyen yalnızca bir tarih koşulu ile bir sorgu kullanarak tek bir tarih için okunacak daha fazla veri neden olur.

### Kısmen monotonik birincil anahtarlar için Endeks kullanımı {#use-of-index-for-partially-monotonic-primary-keys}

Örneğin, Ayın günlerini düşünün. Onlar formu bir [monotonik dizisi](https://en.wikipedia.org/wiki/Monotonic_function) bir ay boyunca, ancak daha uzun süreler için monotonik değil. Bu kısmen monotonik bir dizidir. Bir kullanıcı kısmen monoton birincil anahtar ile tablo oluşturursa, ClickHouse her zamanki gibi seyrek bir dizin oluşturur. Bir kullanıcı bu tür bir tablodan veri seçtiğinde, ClickHouse sorgu koşullarını analiz eder. Kullanıcı, dizinin iki işareti arasında veri almak isterse ve bu işaretlerin her ikisi de bir ay içinde düşerse, ClickHouse bu özel durumda dizini kullanabilir, çünkü sorgu parametreleri ile dizin işaretleri arasındaki mesafeyi hesaplayabilir.

Sorgu parametresi aralığındaki birincil anahtarın değerleri monotonik bir sırayı temsil etmiyorsa, ClickHouse bir dizin kullanamaz. Bu durumda, ClickHouse Tam Tarama yöntemini kullanır.

ClickHouse bu mantığı yalnızca ay dizilerinin günleri için değil, kısmen monotonik bir diziyi temsil eden herhangi bir birincil anahtar için kullanır.

### Veri atlama indeksleri (deneysel) {#table_engine-mergetree-data_skipping-indexes}

Dizin bildirimi sütunlar bölümünde `CREATE` sorgu.

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

Tablolar için `*MergeTree` aile, veri atlama endeksleri belirtilebilir.

Bu endeksler, bloklarda belirtilen ifade hakkında bazı bilgileri toplar ve bunlardan oluşur `granularity_value` granüller (granül boyutu kullanılarak belirtilir `index_granularity` tablo motoru ayarı). Daha sonra bu agregalar `SELECT` büyük veri bloklarını atlayarak diskten okunacak veri miktarını azaltmak için sorgular `where` sorgu tatmin edilemez.

**Örnek**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Örneğin endeksleri aşağıdaki sorgularda diskten okunacak veri miktarını azaltmak için ClickHouse tarafından kullanılabilir:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### Mevcut Endeks türleri {#available-types-of-indices}

-   `minmax`

    Belirtilen ifad (eyi saklar (ifad (enin `tuple`, sonra her eleman için aşırı depolar `tuple`), birincil anahtar gibi veri bloklarını atlamak için saklanan bilgileri kullanır.

-   `set(max_rows)`

    Belirtilen ifadenin benzersiz değerlerini depolar (en fazla `max_rows` satırlar, `max_rows=0` Anlamına geliyor “no limits”). Kontrol etmek için değerleri kullanır `WHERE` ifade, bir veri bloğu üzerinde tatmin edilemez değildir.

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Mağazalar a [Blo filterom filtre](https://en.wikipedia.org/wiki/Bloom_filter) bu, bir veri bloğundaki tüm ngramları içerir. Sadece dizeleri ile çalışır. Optimizasyonu için kullanılabilir `equals`, `like` ve `in` ifadeler.

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Olarak aynı `ngrambf_v1`, ancak ngrams yerine simgeleri saklar. Belirteçler alfasayısal olmayan karakterlerle ayrılmış dizilerdir.

-   `bloom_filter([false_positive])` — Stores a [Blo filterom filtre](https://en.wikipedia.org/wiki/Bloom_filter) belirtilen sütunlar için.

    Opsiyonel `false_positive` parametre, filtreden yanlış pozitif yanıt alma olasılığıdır. Olası değerler: (0, 1). Varsayılan değer: 0.025.

    Desteklenen veri türleri: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    Aşağıdaki işlevleri kullanabilirsiniz: [eşitlikler](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [içinde](../../../sql-reference/functions/in-functions.md), [notİn](../../../sql-reference/functions/in-functions.md), [var](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### Fonksiyonları Destek {#functions-support}

Koşulları `WHERE` yan tümcesi, sütunlarla çalışan işlevlerin çağrılarını içerir. Sütun bir dizinin bir parçasıysa, ClickHouse işlevleri gerçekleştirirken bu dizini kullanmaya çalışır. ClickHouse, dizinleri kullanmak için farklı işlev alt kümelerini destekler.

Bu `set` dizin tüm fonksiyonları ile kullanılabilir. Diğer dizinler için işlev alt kümeleri aşağıdaki tabloda gösterilmiştir.

| Fonksiyon (operatör) / dizin                                                                               | birincil anahtar | minmax | ngrambf\_v1 | tokenbf\_v1 | bloom\_filter |
|------------------------------------------------------------------------------------------------------------|------------------|--------|-------------|-------------|---------------|
| [eşitlikler (=, ==)](../../../sql-reference/functions/comparison-functions.md#function-equals)             | ✔                | ✔      | ✔           | ✔           | ✔             |
| [notEquals(!=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals)         | ✔                | ✔      | ✔           | ✔           | ✔             |
| [hoşlanmak](../../../sql-reference/functions/string-search-functions.md#function-like)                     | ✔                | ✔      | ✔           | ✗           | ✗             |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                    | ✔                | ✔      | ✔           | ✗           | ✗             |
| [startsWith](../../../sql-reference/functions/string-functions.md#startswith)                              | ✔                | ✔      | ✔           | ✔           | ✗             |
| [endsWith](../../../sql-reference/functions/string-functions.md#endswith)                                  | ✗                | ✗      | ✔           | ✔           | ✗             |
| [multiSearchAny](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)      | ✗                | ✗      | ✔           | ✗           | ✗             |
| [içinde](../../../sql-reference/functions/in-functions.md#in-functions)                                    | ✔                | ✔      | ✔           | ✔           | ✔             |
| [notİn](../../../sql-reference/functions/in-functions.md#in-functions)                                     | ✔                | ✔      | ✔           | ✔           | ✔             |
| [daha az (\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                     | ✔                | ✔      | ✗           | ✗           | ✗             |
| [büyük (\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                    | ✔                | ✔      | ✗           | ✗           | ✗             |
| [lessOrEquals (\<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)       | ✔                | ✔      | ✗           | ✗           | ✗             |
| [greaterOrEquals (\>=)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals) | ✔                | ✔      | ✗           | ✗           | ✗             |
| [boş](../../../sql-reference/functions/array-functions.md#function-empty)                                  | ✔                | ✔      | ✗           | ✗           | ✗             |
| [notEmpty](../../../sql-reference/functions/array-functions.md#function-notempty)                          | ✔                | ✔      | ✗           | ✗           | ✗             |
| hasToken                                                                                                   | ✗                | ✗      | ✗           | ✔           | ✗             |

Ngram boyutundan daha az olan sabit bir argümana sahip işlevler tarafından kullanılamaz `ngrambf_v1` sorgu optimizasyonu için.

Bloom filtreleri yanlış pozitif eşleşmelere sahip olabilir, bu yüzden `ngrambf_v1`, `tokenbf_v1`, ve `bloom_filter` dizinler, örneğin bir işlevin sonucunun false olması beklenen sorguları en iyi duruma getirmek için kullanılamaz:

-   Optimize edilebilir:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   Optimize edilemez:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## Eşzamanlı Veri Erişimi {#concurrent-data-access}

Eşzamanlı tablo erişimi için çoklu sürüm kullanıyoruz. Başka bir deyişle, bir tablo aynı anda okunup güncelleştirildiğinde, sorgu sırasında geçerli olan bir parça kümesinden veri okunur. Uzun kilitler yok. Ekler okuma işlemlerinin yoluna girmez.

Bir tablodan okuma otomatik olarak paralelleştirilir.

## Sütunlar ve tablolar için TTL {#table_engine-mergetree-ttl}

Değerlerin ömrünü belirler.

Bu `TTL` yan tümcesi tüm tablo ve her sütun için ayarlanabilir. Tablo düzeyinde TTL ayrıca diskler ve birimler arasında otomatik veri taşıma mantığını belirtebilirsiniz.

İfadeleri değerlendirmek gerekir [Tarihli](../../../sql-reference/data-types/date.md) veya [DateTime](../../../sql-reference/data-types/datetime.md) veri türü.

Örnek:

``` sql
TTL time_column
TTL time_column + interval
```

Tanımlamak `interval`, kullanma [zaman aralığı](../../../sql-reference/operators/index.md#operators-datetime) operatörler.

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Sütun TTL {#mergetree-column-ttl}

Sütundaki değerler sona erdiğinde, ClickHouse bunları sütun veri türü için varsayılan değerlerle değiştirir. Veri bölümündeki tüm sütun değerleri sona ererse, ClickHouse bu sütunu bir dosya sistemindeki veri bölümünden siler.

Bu `TTL` yan tümcesi anahtar sütunlar için kullanılamaz.

Örnekler:

TTL ile tablo oluşturma

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

Varolan bir tablonun sütununa TTL ekleme

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

Sütun TTL değiştirme

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Tablo TTL {#mergetree-table-ttl}

Tablo, süresi dolmuş satırların kaldırılması için bir ifadeye ve parçaların arasında otomatik olarak taşınması için birden fazla ifadeye sahip olabilir [diskler veya birimler](#table_engine-mergetree-multiple-volumes). Tablodaki satırların süresi dolduğunda, ClickHouse ilgili tüm satırları siler. Parça taşıma özelliği için, bir parçanın tüm satırları hareket ifadesi ölçütlerini karşılaması gerekir.

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

TTL kuralı türü her TTL ifadesini takip edebilir. İfade tatmin edildikten sonra yapılacak bir eylemi etkiler (şimdiki zamana ulaşır):

-   `DELETE` - süresi dolmuş satırları sil (varsayılan eylem);
-   `TO DISK 'aaa'` - parçayı diske taşı `aaa`;
-   `TO VOLUME 'bbb'` - parçayı diske taşı `bbb`.

Örnekler:

TTL ile tablo oluşturma

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

Tablonun TTL değiştirme

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**Verileri Kaldırma**

ClickHouse veri parçalarını birleştirdiğinde süresi dolmuş bir TTL ile veri kaldırılır.

ClickHouse, verilerin süresi dolduğunu gördüğünde, zamanlama dışı bir birleştirme gerçekleştirir. Bu tür birleştirmelerin sıklığını kontrol etmek için şunları ayarlayabilirsiniz `merge_with_ttl_timeout`. Değer çok düşükse, çok fazla kaynak tüketebilecek birçok zamanlama dışı birleştirme gerçekleştirir.

Gerçekleştir theirseniz `SELECT` birleştirme arasında sorgu, süresi dolmuş veri alabilirsiniz. Bunu önlemek için, [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) önce sorgu `SELECT`.

## Veri depolama için birden fazla blok cihazı kullanma {#table_engine-mergetree-multiple-volumes}

### Giriş {#introduction}

`MergeTree` aile tablo motorları birden fazla blok cihazlarda veri saklayabilirsiniz. Örneğin, belirli bir tablonun verileri örtük olarak bölündüğünde yararlı olabilir “hot” ve “cold”. En son veriler düzenli olarak talep edilir, ancak yalnızca az miktarda alan gerektirir. Aksine, yağ kuyruklu tarihsel veriler nadiren talep edilir. Birkaç disk varsa, “hot” veriler hızlı disklerde (örneğin, NVMe SSD'ler veya bellekte) bulunabilir; “cold” veri-nispeten yavaş olanlar (örneğin, HDD).

Veri kısmı için minimum hareketli birimdir `MergeTree`- motor masaları. Bir parçaya ait veriler bir diskte saklanır. Veri parçaları arka planda diskler arasında (kullanıcı ayarlarına göre) ve aynı zamanda [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) sorgular.

### Şartlar {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [yol](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) sunucu ayarı.
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

Açıklanan varlıklara verilen isimler sistem tablolarında bulunabilir, [sistem.storage\_policies](../../../operations/system-tables.md#system_tables-storage_policies) ve [sistem.diskler](../../../operations/system-tables.md#system_tables-disks). Bir tablo için yapılandırılmış depolama ilkelerinden birini uygulamak için `storage_policy` ayarı `MergeTree`- motor aile tabloları.

### Yapılandırma {#table_engine-mergetree-multiple-volumes_configure}

Diskler, birimler ve depolama politikaları içinde bildirilmelidir `<storage_configuration>` ana dosyada ya etiket `config.xml` veya farklı bir dosyada `config.d` dizin.

Yapılandırma yapısı:

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Etiketler:

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` ve `shadow` klasörler) ile Sonlandır shouldılmalıdır ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

Disk tanımının sırası önemli değildir.

Depolama ilkeleri yapılandırma biçimlendirme:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Etiketler:

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

Cofiguration örnekleri:

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>
    </policies>
    ...
</storage_configuration>
```

Verilen örnekte, `hdd_in_order` politika uygular [Ro -und-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) yaklaşmak. Böylece bu politika yalnızca bir birim tanımlar (`single`), veri parçaları tüm disklerinde dairesel sırayla saklanır. Bu tür bir politika, sisteme birkaç benzer disk takılıysa, ancak RAID yapılandırılmamışsa oldukça yararlı olabilir. Her bir disk sürücüsünün güvenilir olmadığını ve bunu 3 veya daha fazla çoğaltma faktörü ile telafi etmek isteyebileceğinizi unutmayın.

Sistemde farklı türde diskler varsa, `moving_from_ssd_to_hdd` politika yerine kullanılabilir. Birim `hot` bir SSD disk oluşur (`fast_ssd`) ve bu birimde saklanabilecek bir parçanın maksimum boyutu 1GB. Tüm parçaları ile boyutu daha büyük 1 GB üzerinde doğrudan saklanır `cold` bir HDD diski içeren birim `disk1`.
Ayrıca, bir kez disk `fast_ssd` 80'den fazla % tarafından doldurulur, veri transfer edilecektir `disk1` bir arka plan işlemi ile.

Depolama ilkesi içindeki birim numaralandırma sırası önemlidir. Bir birim aşırı doldurulduktan sonra, veriler bir sonrakine taşınır. Disk numaralandırma sırası da önemlidir, çünkü veriler sırayla depolanır.

Bir tablo oluştururken, yapılandırılmış depolama ilkelerinden birini ona uygulayabilirsiniz:

``` sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

Bu `default` depolama ilkesi, Yalnızca verilen bir diskten oluşan yalnızca bir birim kullanmayı ima eder `<path>`. Bir tablo oluşturulduktan sonra, depolama ilkesi değiştirilemez.

### Ayrıntı {#details}

Bu durumda `MergeTree` tablolar, veriler diske farklı şekillerde giriyor:

-   Bir ekleme sonucunda (`INSERT` sorgu).
-   Arka plan birleştirmeleri sırasında ve [mutasyonlar](../../../sql-reference/statements/alter.md#alter-mutations).
-   Başka bir kopyadan indirirken.
-   Bölüm Don ofması sonucu [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

Mutasyonlar ve bölüm dondurma hariç tüm bu durumlarda, bir parça verilen depolama politikasına göre bir birim ve bir diskte saklanır:

1.  Bir parçayı depolamak için yeterli disk alanına sahip olan ilk birim (tanım sırasına göre) (`unreserved_space > current_part_size`) ve belirli bir boyuttaki parçaların saklanmasına izin verir (`max_data_part_size_bytes > current_part_size`) seçilir.
2.  Bu birimde, önceki veri yığınını depolamak için kullanılan ve parça boyutundan daha fazla boş alana sahip olan diski izleyen disk seçilir (`unreserved_space - keep_free_space_bytes > current_part_size`).

Kap hoodut underun altında, [sabit linkler](https://en.wikipedia.org/wiki/Hard_link). Farklı diskler arasındaki sabit bağlantılar desteklenmez, bu nedenle bu gibi durumlarda ortaya çıkan parçalar ilk disklerle aynı disklerde saklanır.

Arka planda, parçalar boş alan miktarına göre hacimler arasında taşınır (`move_factor` parametre) sırasına göre birimler yapılandırma dosyasında beyan edilir.
Veriler asla sonuncudan ve birincisine aktarılmaz. Bir sistem tabloları kullanabilirsiniz [sistem.part\_log](../../../operations/system-tables.md#system_tables-part-log) (alan `type = MOVE_PART`) ve [sistem.parçalar](../../../operations/system-tables.md#system_tables-parts) (alanlar `path` ve `disk`) arka plan hareketlerini izlemek için. Ayrıca, ayrıntılı bilgi sunucu günlüklerinde bulunabilir.

Kullanıcı, sorguyu kullanarak bir bölümü veya bölümü bir birimden diğerine taşımaya zorlayabilir [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition), arka plan işlemleri için tüm kısıtlamalar dikkate alınır. Sorgu, kendi başına bir hareket başlatır ve tamamlanması için arka plan işlemleri beklemez. Yeterli boş alan yoksa veya gerekli koşullardan herhangi biri karşılanmazsa kullanıcı bir hata mesajı alır.

Veri taşıma veri çoğaltma ile müdahale etmez. Bu nedenle, farklı depolama ilkeleri aynı tablo için farklı yinelemeler üzerinde belirtilebilir.

Arka plan birleşimlerinin ve mutasyonlarının tamamlanmasından sonra, eski parçalar yalnızca belirli bir süre sonra çıkarılır (`old_parts_lifetime`).
Bu süre zarfında, diğer birimlere veya disklere taşınmazlar. Bu nedenle, parçalar nihayet çıkarılıncaya kadar, işgal edilen disk alanının değerlendirilmesi için hala dikkate alınır.

[Orijinal makale](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
