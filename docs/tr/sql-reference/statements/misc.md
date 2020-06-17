---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Di\u011Fer"
---

# Çeşitli Sorgular {#miscellaneous-queries}

## ATTACH {#attach}

Bu sorgu tam olarak aynıdır `CREATE`, ama

-   Kelime yerine `CREATE` kelime kullanır `ATTACH`.
-   Sorgu diskte veri oluşturmaz, ancak verilerin zaten uygun yerlerde olduğunu ve yalnızca tablo hakkında bilgi sunucuya eklediğini varsayar.
    Bir ekleme sorgusu çalıştırdıktan sonra, sunucu tablonun varlığı hakkında bilgi sahibi olacaktır.

Tablo daha önce ayrılmış olsaydı (`DETACH`), yapısının bilindiği anlamına gelir, yapıyı tanımlamadan steno kullanabilirsiniz.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Bu sorgu, sunucu başlatılırken kullanılır. Sunucu, tablo meta verilerini dosyalar olarak depolar `ATTACH` başlangıçta çalıştığı sorgular (sunucuda açıkça oluşturulan sistem tabloları hariç).

## CHECK TABLE {#check-table}

Tablodaki verilerin bozuk olup olmadığını denetler.

``` sql
CHECK TABLE [db.]name
```

Bu `CHECK TABLE` sorgu, gerçek dosya boyutlarını sunucuda depolanan beklenen değerlerle karşılaştırır. Dosya boyutları depolanan değerlerle eşleşmiyorsa, verilerin bozuk olduğu anlamına gelir. Bu, örneğin, sorgu yürütme sırasında bir sistem çökmesine neden olabilir.

Sorgu yanıtı içerir `result` tek satırlı sütun. Satır bir değere sahiptir
[Boeanoleanean](../../sql-reference/data-types/boolean.md) tür:

-   0-tablodaki veriler bozuk.
-   1 - veri bütünlüğünü korur.

Bu `CHECK TABLE` sorgu Aşağıdaki tablo motorlarını destekler:

-   [Günlük](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [MergeTree ailesi](../../engines/table-engines/mergetree-family/mergetree.md)

Başka bir tablo motorları ile tablolar üzerinde gerçekleştirilen bir özel duruma neden olur.

Motor fromlardan `*Log` aile başarısızlık otomatik veri kurtarma sağlamaz. Kullan... `CHECK TABLE` veri kaybını zamanında izlemek için sorgu.

İçin `MergeTree` aile motorları, `CHECK TABLE` sorgu, yerel sunucudaki bir tablonun her bir veri bölümü için bir kontrol durumunu gösterir.

**Veri bozuksa**

Tablo bozuksa, bozuk olmayan verileri başka bir tabloya kopyalayabilirsiniz. Bunu yapmak için :

1.  Bozuk tablo ile aynı yapıya sahip yeni bir tablo oluşturun. Bunu yapmak için sorguyu yürütün `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Ayarla... [max\_threads](../../operations/settings/settings.md#settings-max_threads) bir sonraki sorguyu tek bir iş parçacığında işlemek için 1 değeri. Bunu yapmak için sorguyu çalıştırın `SET max_threads = 1`.
3.  Sorgu yürütme `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Bu istek bozuk olmayan verileri bozuk tablodan başka bir tabloya kopyalar. Yalnızca bozuk parçadan önceki veriler kopyalanır.
4.  Yeniden Başlat `clickhouse-client` sıfırlamak için `max_threads` değer.

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Aşağıdaki döndürür `String` sütun tipi:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [varsayılan ifade](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` veya `ALIAS`). Varsayılan ifade belirtilmemişse, sütun boş bir dize içerir.
-   `default_expression` — Value specified in the `DEFAULT` yan.
-   `comment_expression` — Comment text.

İç içe veri yapıları çıktı “expanded” biçimli. Her sütun ayrı ayrı gösterilir, bir noktadan sonra adı ile.

## DETACH {#detach}

Hakkında bilgi siler ‘name’ sunucudan tablo. Sunucu, tablonun varlığını bilmeyi durdurur.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Bu tablonun veri veya meta verileri silmez. Bir sonraki sunucu lansmanında, sunucu meta verileri okuyacak ve tablo hakkında tekrar bilgi edinecektir.
Benzer şekilde, bir “detached” tablo kullanılarak yeniden eklenebilir `ATTACH` sorgu (bunlar için depolanan meta verilere sahip olmayan sistem tabloları hariç).

Hiç yok... `DETACH DATABASE` sorgu.

## DROP {#drop}

Bu sorgu iki türü vardır: `DROP DATABASE` ve `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

İçindeki tüm tabloları siler ‘db’ veritabanı, daha sonra siler ‘db’ veritabanı kendisi.
Eğer `IF EXISTS` belirtilen, veritabanı yoksa bir hata döndürmez.

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Tabloyu siler.
Eğer `IF EXISTS` belirtilmişse, tablo yoksa veya veritabanı yoksa bir hata döndürmez.

    DROP DICTIONARY [IF EXISTS] [db.]name

Sözlük Delets.
Eğer `IF EXISTS` belirtilmişse, tablo yoksa veya veritabanı yoksa bir hata döndürmez.

## DROP USER {#drop-user-statement}

Bir kullanıcıyı siler.

### Sözdizimi {#drop-user-syntax}

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

Bir rolü siler.

Silinen rol, verildiği tüm varlıklardan iptal edilir.

### Sözdizimi {#drop-role-syntax}

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

Bir satır ilkesi siler.

Silinen satır ilkesi, atandığı tüm varlıklardan iptal edilir.

### Sözdizimi {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

Bir kota siler.

Silinen kota, atandığı tüm varlıklardan iptal edilir.

### Sözdizimi {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

Bir kota siler.

Silinen kota, atandığı tüm varlıklardan iptal edilir.

### Sözdizimi {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Bir tek döndürür `UInt8`- tek değeri içeren sütun yazın `0` tablo veya veritabanı yoksa veya `1` tablo belirtilen veritabanında varsa.

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Şu anda çalışan sorguları zorla sonlandırmaya çalışır.
Sonlandırılacak sorgular sistemden seçilir.tanımlanan kriterleri kullanarak işlemler tablosu `WHERE` fıkra ofsı `KILL` sorgu.

Örnekler:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

Salt okunur kullanıcılar yalnızca kendi sorgularını durdurabilir.

Varsayılan olarak, sorguların zaman uyumsuz sürümü kullanılır (`ASYNC`), sorguların durduğuna dair onay beklemez.

Senkron versiyonu (`SYNC`) tüm sorguların durmasını bekler ve durduğunda her işlem hakkında bilgi görüntüler.
Yanıt içerir `kill_status` aşağıdaki değerleri alabilen sütun:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

Bir test sorgusu (`TEST`) yalnızca kullanıcının haklarını denetler ve durdurulacak sorguların bir listesini görüntüler.

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

İptal etmek ve kaldırmak için çalışır [mutasyonlar](alter.md#alter-mutations) şu anda yürütülüyor. İptal etmek için mutationsasyonlar seçilir [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) tablo tarafından belirtilen filtreyi kullanarak `WHERE` fıkra ofsı `KILL` sorgu.

Bir test sorgusu (`TEST`) yalnızca kullanıcının haklarını denetler ve durdurulacak sorguların bir listesini görüntüler.

Örnekler:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Mutasyon tarafından yapılan değişiklikler geri alınmaz.

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

Bu sorgu, bir tablo altyapısı ile tablolar için veri parçaları planlanmamış birleştirme başlatmaya çalışır. [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) aile.

Bu `OPTMIZE` sorgu için de desteklenmektedir [MaterializedView](../../engines/table-engines/special/materializedview.md) ve... [Arabellek](../../engines/table-engines/special/buffer.md) motorlar. Diğer tablo motorları desteklenmiyor.

Ne zaman `OPTIMIZE` ile kullanılır [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) Tablo motorları ailesi, ClickHouse birleştirme için bir görev oluşturur ve tüm düğümlerde yürütülmeyi bekler (eğer `replication_alter_partitions_sync` ayar etkinse) ' dir.

-   Eğer `OPTIMIZE` herhangi bir nedenle bir birleştirme gerçekleştirmez, müşteriye bildirmez. Bildirimleri etkinleştirmek için [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) ayar.
-   Belirtir aseniz bir `PARTITION`, sadece belirtilen bölüm optimize edilmiştir. [Bölüm ifadesi nasıl ayarlanır](alter.md#alter-how-to-specify-part-expr).
-   Belirtir specifyseniz `FINAL`, optimizasyon, tüm veriler zaten bir parçada olsa bile gerçekleştirilir.
-   Belirtir specifyseniz `DEDUPLICATE`, sonra tamamen aynı satırlar tekilleştirilecektir (tüm sütunlar karşılaştırılır), sadece MergeTree motoru için anlamlıdır.

!!! warning "Uyarıcı"
    `OPTIMIZE` Düzelt canemiyorum “Too many parts” hatasız.

## RENAME {#misc_operations-rename}

Bir veya daha fazla tabloyu yeniden adlandırır.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Tüm tablolar genel kilitleme altında yeniden adlandırılır. Tabloları yeniden adlandırma hafif bir işlemdir. İÇİN'DEN sonra başka bir veritabanı belirttiyseniz, tablo bu veritabanına taşınacaktır. Ancak, veritabanlarına sahip dizinlerin aynı dosya sisteminde bulunması gerekir (aksi takdirde bir hata döndürülür).

## SET {#query-set}

``` sql
SET param = value
```

Atıyor `value` to the `param` [ayar](../../operations/settings/index.md) geçerli oturum için. Değiştiremezsiniz [sunucu ayarları](../../operations/server-configuration-parameters/index.md) bu şekilde.

Belirtilen ayarlar profilindeki tüm değerleri tek bir sorguda da ayarlayabilirsiniz.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Daha fazla bilgi için, bkz. [Ayarlar](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

Geçerli kullanıcı için rolleri etkinleştirir.

### Sözdizimi {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Varsayılan rolleri bir kullanıcıya ayarlar.

Varsayılan roller kullanıcı girişinde otomatik olarak etkinleştirilir. Varsayılan olarak yalnızca önceden verilen rolleri ayarlayabilirsiniz. Rol bir kullanıcıya verilmezse, ClickHouse bir istisna atar.

### Sözdizimi {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

### Örnekler {#set-default-role-examples}

Bir kullanıcıya birden çok varsayılan rol ayarlama:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Verilen tüm rolleri bir kullanıcıya varsayılan olarak ayarlayın:

``` sql
SET DEFAULT ROLE ALL TO user
```

Bir kullanıcıdan varsayılan rolleri temizleme:

``` sql
SET DEFAULT ROLE NONE TO user
```

Verilen tüm rolleri bazıları hariç varsayılan olarak ayarlayın:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Bir tablodaki tüm verileri kaldırır. Fık thera ne zaman `IF EXISTS` tablo yoksa, sorgu bir hata döndürür.

Bu `TRUNCATE` sorgu için desteklenmiyor [Görünüm](../../engines/table-engines/special/view.md), [Dosya](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) ve [Boş](../../engines/table-engines/special/null.md) masa motorları.

## USE {#use}

``` sql
USE db
```

Oturum için geçerli veritabanını ayarlamanızı sağlar.
Geçerli veritabanı, veritabanı sorguda tablo adından önce bir nokta ile açıkça tanımlanmamışsa, tabloları aramak için kullanılır.
Bir oturum kavramı olmadığından, bu sorgu HTTP protokolünü kullanırken yapılamaz.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
