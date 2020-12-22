---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: CREATE
---

# Sorgu oluştur {#create-queries}

## CREATE DATABASE {#query-language-create-database}

Veritabanı oluşturur.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Yanlar {#clauses}

-   `IF NOT EXISTS`
    Eğer... `db_name` veritabanı zaten var, daha sonra ClickHouse yeni bir veritabanı oluşturmuyor ve:

    -   If yan tümcesi belirtilmişse bir istisna atmaz.
    -   Bir istisna atar if yan tümcesi belirtilmemiş.

-   `ON CLUSTER`
    ClickHouse oluşturur `db_name` belirtilen bir kümenin tüm sunucularında veritabanı.

-   `ENGINE`

    -   [MySQL](../../engines/database-engines/mysql.md)
        Uzak MySQL sunucusundan veri almanızı sağlar.
        Varsayılan olarak, ClickHouse kendi kullanır [Veritabanı Altyapısı](../../engines/database-engines/index.md).

## CREATE TABLE {#create-table-query}

Bu `CREATE TABLE` sorgu çeşitli formlara sahip olabilir.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

Adlı bir tablo oluşturur ‘name’ in the ‘db’ veritabanı veya geçerli veritabanı ise ‘db’ küme değil, parantez içinde belirtilen yapı ve ‘engine’ motor.
Tablonun yapısı sütun açıklamalarının bir listesidir. Dizinler altyapısı tarafından destekleniyorsa, tablo altyapısı için parametreler olarak gösterilir.

Bir sütun açıklaması `name type` en basit durumda. Örnek: `RegionID UInt32`.
İfadeler varsayılan değerler için de tanımlanabilir (aşağıya bakın).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Başka bir tablo ile aynı yapıya sahip bir tablo oluşturur. Tablo için farklı bir motor belirtebilirsiniz. Motor belirtilmemişse, aynı motor için olduğu gibi kullanılacaktır `db2.name2` Tablo.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Yapısı ve veri tarafından döndürülen bir tablo oluşturur. [tablo fonksiyonu](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Sonucu gibi bir yapıya sahip bir tablo oluşturur `SELECT` Sorgu, ile ‘engine’ motor ve SELECT verilerle doldurur.

Her durumda, eğer `IF NOT EXISTS` tablo zaten varsa, sorgu bir hata döndürmez. Bu durumda, sorgu hiçbir şey yapmaz.

Sonra başka maddeler olabilir `ENGINE` sorguda yan tümcesi. Açıklamalarda tabloların nasıl oluşturulacağına ilişkin ayrıntılı belgelere bakın [masa motorları](../../engines/table-engines/index.md#table_engines).

### Varsayılan Değerler {#create-default-values}

Sütun açıklaması, aşağıdaki yollardan biriyle varsayılan değer için bir ifade belirtebilir:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Örnek: `URLDomain String DEFAULT domain(URL)`.

Varsayılan değer için bir ifade tanımlanmamışsa, varsayılan değerler sayılar için sıfırlar, dizeler için boş dizeler, diziler için boş diziler ve `1970-01-01` tarihler için veya zero unix timestamp zamanla tarihler için. Boş alanlar desteklenmez.

Varsayılan ifade tanımlanmışsa, sütun türü isteğe bağlıdır. Açıkça tanımlanmış bir tür yoksa, varsayılan ifade türü kullanılır. Örnek: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ türü için kullanılacak ‘EventDate’ sütun.

Veri türü ve varsayılan ifade açıkça tanımlanırsa, bu ifade type casting işlevleri kullanılarak belirtilen türe aktarılır. Örnek: `Hits UInt32 DEFAULT 0` aynı şeyi ifade eder `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don't contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

Normal varsayılan değer. INSERT sorgusu karşılık gelen sütunu belirtmezse, ilgili ifadeyi hesaplayarak doldurulur.

`MATERIALIZED expr`

Somut ifade. Böyle bir sütun INSERT için belirtilemez, çünkü her zaman hesaplanır.
Sütun listesi olmayan bir ekleme için bu sütunlar dikkate alınmaz.
Buna ek olarak, bir SELECT sorgusunda Yıldız İşareti kullanıldığında bu sütun değiştirilmez. Bu, dökümü kullanarak elde edilen değişmezi korumaktır `SELECT *` sütun listesini belirtmeden INSERT kullanarak tabloya geri eklenebilir.

`ALIAS expr`

Eşanlamlı sözcük. Böyle bir sütun tabloda hiç depolanmaz.
Değerleri bir tabloya eklenemez ve bir SELECT sorgusunda Yıldız İşareti kullanılırken değiştirilmez.
Sorgu ayrıştırma sırasında diğer ad genişletilirse, seçimlerde kullanılabilir.

Yeni sütunlar eklemek için ALTER sorgusunu kullanırken, bu sütunlar için eski veriler yazılmaz. Bunun yerine, yeni sütunlar için değerleri olmayan eski verileri okurken, ifadeler varsayılan olarak anında hesaplanır. Ancak, ifadeleri çalıştırmak sorguda belirtilmeyen farklı sütunlar gerektiriyorsa, bu sütunlar ayrıca okunur, ancak yalnızca buna ihtiyaç duyan veri blokları için okunur.

Bir tabloya yeni bir sütun eklerseniz, ancak daha sonra varsayılan ifadesini değiştirirseniz, eski veriler için kullanılan değerler değişir (değerlerin diskte depolanmadığı veriler için). Arka plan birleştirmeleri çalıştırırken, birleştirme parçalarından birinde eksik olan sütunların verileri birleştirilmiş parçaya yazıldığını unutmayın.

İç içe geçmiş veri yapılarındaki öğeler için varsayılan değerleri ayarlamak mümkün değildir.

### Kısıtlamalar {#constraints}

Sütun açıklamaları kısıtlamaları ile birlikte tanımlanabilir:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` herhangi bir Boole ifadesi ile olabilir. Tablo için kısıtlamalar tanımlanırsa, her biri her satır için kontrol edilir `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Büyük miktarda kısıtlama eklemek, büyük `INSERT` sorgular.

### TTL ifadesi {#ttl-expression}

Değerler için depolama süresini tanımlar. Sadece MergeTree-family tabloları için belirtilebilir. Ayrıntılı açıklama için, bkz. [Sütunlar ve tablolar için TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### Sütun Sıkıştırma Kodekleri {#codecs}

Varsayılan olarak, ClickHouse `lz4` sıkıştırma yöntemi. İçin `MergeTree`- motor ailesi varsayılan sıkıştırma yöntemini değiştirebilirsiniz [sıkıştırma](../../operations/server-configuration-parameters/settings.md#server-settings-compression) bir sunucu yapılandırması bölümü. Her bir sütun için sıkıştırma yöntemini de tanımlayabilirsiniz. `CREATE TABLE` sorgu.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Bir codec bileşeni belirtilmişse, varsayılan codec bileşeni geçerli değildir. Kodekler bir boru hattında birleştirilebilir, örneğin, `CODEC(Delta, ZSTD)`. Projeniz için en iyi codec kombinasyonunu seçmek için, Altınlıkta açıklanana benzer kriterler geçirin [ClickHouse verimliliğini artırmak için yeni Kodlamalar](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) Makale.

!!! warning "Uyarıcı"
    ClickHouse veritabanı dosyalarını harici yardımcı programlarla açamazsınız `lz4`. Bunun yerine, özel kullanın [clickhouse-kompresör](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) program.

Sıkıştırma Aşağıdaki tablo motorları için desteklenir:

-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) aile. Sütun sıkıştırma kodeklerini destekler ve varsayılan sıkıştırma yöntemini seçerek [sıkıştırma](../../operations/server-configuration-parameters/settings.md#server-settings-compression) ayarlar.
-   [Günlük](../../engines/table-engines/log-family/index.md) aile. Kullanır `lz4` sıkıştırma yöntemi varsayılan olarak ve sütun sıkıştırma codec destekler.
-   [Koymak](../../engines/table-engines/special/set.md). Yalnızca varsayılan sıkıştırmayı destekledi.
-   [Katmak](../../engines/table-engines/special/join.md). Yalnızca varsayılan sıkıştırmayı destekledi.

ClickHouse ortak amaçlı codec ve özel codec destekler.

#### Özel Kodekler {#create-query-specialized-codecs}

Bu kodekler, verilerin belirli özelliklerini kullanarak sıkıştırmayı daha etkili hale getirmek için tasarlanmıştır. Bu kodeklerden bazıları verileri kendileri sıkıştırmaz. Bunun yerine, verileri ortak bir amaç için hazırlarlar codec, bu hazırlık olmadan daha iyi sıkıştırır.

Özel kodekler:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` delta değerlerini saklamak için kullanılır, böylece `delta_bytes` ham değerlerin maksimum boyutudur. Mümkün `delta_bytes` değerler: 1, 2, 4, 8. İçin varsayılan değer `delta_bytes` oluyor `sizeof(type)` 1, 2, 4 veya 8'e eşitse. Diğer tüm durumlarda, 1.
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: Hızlı, Ölçeklenebilir, Bellek İçi Zaman Serisi Veritabanı](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla: Hızlı, Ölçeklenebilir, Bellek İçi Zaman Serisi Veritabanı](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` ve `DateTime`). Algoritmasının her adımında, codec 64 değerden oluşan bir blok alır, 64x64 bit matrisine koyar, aktarır, kullanılmayan değer bitlerini kırpar ve gerisini bir dizi olarak döndürür. Kullanılmayan bitler, sıkıştırmanın kullanıldığı tüm veri bölümündeki maksimum ve minimum değerler arasında farklılık göstermeyen bitlerdir.

`DoubleDelta` ve `Gorilla` kodekler, Gorilla TSDB'DE sıkıştırma algoritmasının bileşenleri olarak kullanılır. Gorilla yaklaşımı, zaman damgaları ile yavaş yavaş değişen değerler dizisi olduğunda senaryolarda etkilidir. Zaman damgaları tarafından etkili bir şekilde sıkıştırılır `DoubleDelta` codec ve değerler etkin bir şekilde sıkıştırılır `Gorilla` codec. Örneğin, etkili bir şekilde saklanan bir tablo elde etmek için, aşağıdaki yapılandırmada oluşturabilirsiniz:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### Genel Amaçlı Kodekler {#create-query-general-purpose-codecs}

Cod codecsec codecs'ler:

-   `NONE` — No compression.
-   `LZ4` — Lossless [veri sıkıştırma algoritması](https://github.com/lz4/lz4) varsayılan olarak kullanılır. Lz4 hızlı sıkıştırma uygular.
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` varsayılan düzeyi uygular. Olası seviyeleri: \[1, 12\]. Önerilen seviye aralığı: \[4, 9\].
-   `ZSTD[(level)]` — [Zstd sıkıştırma algoritması](https://en.wikipedia.org/wiki/Zstandard) yapılandırılabilir ile `level`. Olası seviyeler: \[1, 22\]. Varsayılan değer: 1.

Yüksek Sıkıştırma seviyeleri asimetrik senaryolar için kullanışlıdır, örneğin bir kez sıkıştırın, tekrar tekrar sıkıştırın. Daha yüksek seviyeler daha iyi sıkıştırma ve daha yüksek CPU kullanımı anlamına gelir.

## Geçici Tablolar {#temporary-tables}

ClickHouse aşağıdaki özelliklere sahip geçici tabloları destekler:

-   Bağlantı kaybolursa da dahil olmak üzere oturum sona erdiğinde geçici tablolar kaybolur.
-   Geçici bir tablo yalnızca bellek altyapısını kullanır.
-   DB geçici bir tablo için belirtilemez. Veritabanları dışında oluşturulur.
-   Tüm küme sunucularında dağıtılmış DDL sorgusu ile geçici bir tablo oluşturmak imkansız (kullanarak `ON CLUSTER`): bu tablo yalnızca geçerli oturumda bulunur.
-   Geçici bir tablo başka bir ile aynı ada sahip ve bir sorgu DB belirtmeden tablo adını belirtir, geçici tablo kullanılır.
-   Dağıtılmış sorgu işleme için bir sorguda kullanılan geçici tablolar uzak sunuculara geçirilir.

Geçici bir tablo oluşturmak için aşağıdaki sözdizimini kullanın:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

Çoğu durumda, geçici tablolar el ile oluşturulmaz, ancak bir sorgu için veya dağıtılmış için dış verileri kullanırken `(GLOBAL) IN`. Daha fazla bilgi için uygun bölümlere bakın

İle tabloları kullanmak mümkündür [Motor = bellek](../../engines/table-engines/special/memory.md) geçici tablolar yerine.

## Dağıtılmış DDL sorguları (küme yan tümcesinde) {#distributed-ddl-queries-on-cluster-clause}

Bu `CREATE`, `DROP`, `ALTER`, ve `RENAME` sorgular, bir kümede dağıtılmış yürütmeyi destekler.
Örneğin, aşağıdaki sorgu oluşturur `all_hits` `Distributed` her ana bilgisayarda tablo `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Bu sorguları doğru bir şekilde çalıştırmak için, her ana bilgisayarın aynı küme tanımına sahip olması gerekir (senkronizasyon yapılandırmalarını basitleştirmek için zookeeper'dan değiştirmeleri kullanabilirsiniz). Ayrıca ZooKeeper sunucularına bağlanmaları gerekir.
Bazı ana bilgisayarlar şu anda mevcut olmasa bile, sorgunun yerel sürümü sonunda kümedeki her ana bilgisayarda uygulanır. Tek bir ana makine içinde sorguları yürütme sırası garanti edilir.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Bir görünüm oluşturur. İki tür görüş vardır: normal ve SOMUTLAŞTIRILMIŞ.

Normal görünümler herhangi bir veri depolamaz, ancak başka bir tablodan bir okuma gerçekleştirir. Başka bir deyişle, normal bir görünüm kaydedilmiş bir sorgudan başka bir şey değildir. Bir görünümden okurken, bu kaydedilmiş sorgu FROM yan tümcesinde bir alt sorgu olarak kullanılır.

Örnek olarak, bir görünüm oluşturduğunuzu varsayalım:

``` sql
CREATE VIEW view AS SELECT ...
```

ve bir sorgu yazdı:

``` sql
SELECT a, b, c FROM view
```

Bu sorgu, alt sorguyu kullanmaya tam olarak eşdeğerdir:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

Materialized görünümler, ilgili SELECT sorgusu tarafından dönüştürülmüş verileri depolar.

Olmadan hayata bir görünüm oluştururken `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

İle somutlaştırılmış bir görünüm oluştururken `TO [db].[table]`, kullanma mustmalısınız `POPULATE`.

Materialized görünüm aşağıdaki gibi düzenlenmiştir: SELECT belirtilen tabloya veri eklerken, eklenen verilerin bir kısmı bu SELECT sorgusu tarafından dönüştürülür ve sonuç görünümde eklenir.

Doldur belirtirseniz, varolan tablo verilerini oluştururken görünümde, sanki bir `CREATE TABLE ... AS SELECT ...` . Aksi takdirde, sorgu yalnızca görünümü oluşturduktan sonra tabloya eklenen verileri içerir. Görünüm oluşturma sırasında tabloya eklenen veriler EKLENMEYECEĞİNDEN, doldur kullanmanızı önermiyoruz.

A `SELECT` sorgu içerebilir `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` ayarlanır, veri ekleme sırasında toplanır, ancak yalnızca tek bir eklenen veri paketi içinde toplanır. Veriler daha fazla toplanmayacaktır. Özel durum, bağımsız olarak veri toplama, gibi gerçekleştiren bir motor kullanırken olur `SummingMergeTree`.

Yürütme `ALTER` somut görünümlerle ilgili sorgular tam olarak geliştirilmemiştir, bu nedenle rahatsız edici olabilirler. Eğer hayata görünüm inşaat kullanıyorsa `TO [db.]name` yapabilirsiniz `DETACH` the view, run `ALTER` hedef tablo için ve sonra `ATTACH` daha önce müstakil (`DETACH`) görünüm.

Görünümler normal tablolarla aynı görünür. Örneğin, bunlar sonucu listelenir `SHOW TABLES` sorgu.

Görünümleri silmek için ayrı bir sorgu yok. Bir görünümü silmek için şunları kullanın `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
```

Oluşturuyor [dış sözlük](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) verilen ile [yapılı](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [kaynaklı](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [düzen](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) ve [ömür](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

Dış sözlük yapısı özniteliklerden oluşur. Sözlük öznitelikleri tablo sütunlarına benzer şekilde belirtilir. Tek gerekli öznitelik özelliği türüdür, diğer tüm özelliklerin varsayılan değerleri olabilir.

Sözlüğe bağlı olarak [düzen](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) bir veya daha fazla öznitelik sözlük anahtarları olarak belirtilebilir.

Daha fazla bilgi için, bkz. [Dış Söz Dictionarieslükler](../dictionaries/external-dictionaries/external-dicts.md) bölme.

## CREATE USER {#create-user-statement}

Oluşturur bir [kullanıcı hesabı](../../operations/access-rights.md#user-account-management).

### Sözdizimi {#create-user-syntax}

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Tanıma {#identification}

Kullanıcı tanımlama birden çok yolu vardır:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` veya `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### Kullanıcı Host {#user-host}

Kullanıcı ana bilgisayar, ClickHouse sunucusuna bağlantı kurulabilen bir ana bilgisayardır. Ev sahibi belirtilebilir `HOST` aşağıdaki yollarla sorgu bölümü:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [alt ağ](https://en.wikipedia.org/wiki/Subnetwork). Örnekler: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. Üretimde kullanım için, sadece belirtin `HOST IP` elemanları (IP adresleri ve maskeleri), kullanıl ,dığından beri `host` ve `host_regexp` ekstra gecikmeye neden olabilir.
-   `HOST ANY` — User can connect from any location. This is default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) kullanıcı ana bilgisayarlarını belirtirken düzenli ifadeler. Mesela, `HOST NAME REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you use the [LIKE](../functions/string-search-functions.md#function-like) kullanıcı ana filtrelemek için operatör. Mesela, `HOST LIKE '%'` eşdeğ toer equivalentdir `HOST ANY`, `HOST LIKE '%.mysite.com'` tüm host filtersları filtreler `mysite.com` etki.

Host belirtme başka bir yolu kullanmaktır `@` kullanıcı adı ile sözdizimi. Örnekler:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` sözdizimi.
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` sözdizimi.
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` sözdizimi.

!!! info "Uyarıcı"
    ClickHouse davranır `user_name@'address'` bir bütün olarak bir kullanıcı adı olarak. Böylece, teknik olarak birden fazla kullanıcı oluşturabilirsiniz `user_name` ve sonra farklı yapılar `@`. Ben bunu tavsiye etmiyoruz.

### Örnekler {#create-user-examples}

Kullanıcı hesabı oluşturma `mira` şifre ile korunmaktadır `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` ClickHouse sunucusunun çalıştığı ana bilgisayarda istemci uygulamasını başlatmalıdır.

Kullanıcı hesabı oluşturma `john`, ona roller atayın ve bu rolleri varsayılan yapın:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Kullanıcı hesabı oluşturma `john` ve gelecekteki tüm rollerini varsayılan hale getirin:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Ne zaman bazı rol atanacak `john` gelecekte otomatik olarak varsayılan hale gelecektir.

Kullanıcı hesabı oluşturma `john` ve gelecekteki tüm rollerini varsayılan olarak yapın `role1` ve `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```

## CREATE ROLE {#create-role-statement}

Oluşturur bir [rol](../../operations/access-rights.md#role-management).

### Sözdizimi {#create-role-syntax}

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Açıklama {#create-role-description}

Rol bir dizi [ayrıcalıklar](grant.md#grant-privileges). Bir rolle verilen bir kullanıcı, bu rolün tüm ayrıcalıklarını alır.

Bir kullanıcı birden çok rolle atanabilir. Kullanıcılar tarafından keyfi kombinasyonlarda verilen rolleri uygulayabilirsiniz [SET ROLE](misc.md#set-role-statement) deyim. Ayrıcalıkların son kapsamı, uygulanan tüm rollerin tüm ayrıcalıklarının birleştirilmiş kümesidir. Bir kullanıcının doğrudan kullanıcı hesabına verilen ayrıcalıkları varsa, bunlar roller tarafından verilen ayrıcalıklarla da birleştirilir.

Kullanıcı, kullanıcı girişinde geçerli olan varsayılan rollere sahip olabilir. Varsayılan rolleri ayarlamak için [SET DEFAULT ROLE](misc.md#set-default-role-statement) beyan veya [ALTER USER](alter.md#alter-user-statement) deyim.

Bir rolü iptal etmek için [REVOKE](revoke.md) deyim.

Rolü silmek için [DROP ROLE](misc.md#drop-role-statement) deyim. Silinen rol, kendisine verilen tüm kullanıcılardan ve rollerden otomatik olarak iptal edilir.

### Örnekler {#create-role-examples}

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Bu sorgu sırası rolü oluşturur `accountant` bu veri okuma ayrıcalığına sahip `accounting` veritabanı.

Kullanıcıya rol verilmesi `mira`:

``` sql
GRANT accountant TO mira;
```

Rol verildikten sonra kullanıcı bunu kullanabilir ve izin verilen sorguları gerçekleştirebilir. Mesela:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

Oluşturur bir [satırlar için filtre](../../operations/access-rights.md#row-policy-management), bir kullanıcı bir tablodan okuyabilir.

### Sözdizimi {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Bölüm olarak {#create-row-policy-as}

Bu bölümü kullanarak izin veren veya kısıtlayıcı ilkeler oluşturabilirsiniz.

İzin verme ilkesi satırlara erişim sağlar. Aynı tabloya uygulanan izin veren politikalar boolean kullanılarak birlikte birleştirilir `OR` operatör. İlkeler varsayılan olarak izinlidir.

Kısıtlayıcı ilke satıra erişimi kısıtlar. Aynı tabloya uygulanan kısıtlayıcı ilkeler, boolean kullanılarak birlikte birleştirilir `AND` operatör.

Kısıtlayıcı ilkeler, izin veren süzgeçleri geçen satırlara uygulanır. Kısıtlayıcı ilkeler ayarlarsanız, ancak izin veren ilkeler yoksa, kullanıcı tablodan herhangi bir satır alamaz.

#### Bölüm için {#create-row-policy-to}

Bölümünde `TO` örneğin, rollerin ve kullanıcıların karışık bir listesini verebilirsiniz, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Kelime `ALL` geçerli kullanıcı dahil olmak üzere tüm ClickHouse kullanıcıları anlamına gelir. Kelimeler `ALL EXCEPT` bazı kullanıcıları tüm kullanıcılar listesinden çıkarmak için izin ver, örneğin `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Örnekler {#examples}

-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`

## CREATE QUOTA {#create-quota-statement}

Oluşturur bir [kota](../../operations/access-rights.md#quotas-management) bu bir kullanıcıya veya bir role atanabilir.

### Sözdizimi {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Örnek {#create-quota-example}

15 ay içinde 123 Sorgu ile geçerli kullanıcı için sorgu sayısını sınır constraintlayın:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```

## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Oluşturur bir [ayarlar profili](../../operations/access-rights.md#settings-profiles-management) bu bir kullanıcıya veya bir role atanabilir.

### Sözdizimi {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

# Örnek {#create-settings-profile-syntax}

Create the `max_memory_usage_profile` ayar profili için değer ve kısıtlamalarla `max_memory_usage` ayar. At itayın `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
