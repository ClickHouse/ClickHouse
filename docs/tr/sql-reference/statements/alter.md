---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

Bu `ALTER` sorgu yalnızca için desteklenir `*MergeTree` tablo gibi `Merge`ve`Distributed`. Sorgunun çeşitli varyasyonları vardır.

### Sütun Manipülasyonları {#column-manipulations}

Tablo yapısını değiştirme.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

Sorguda, bir veya daha fazla virgülle ayrılmış eylemlerin bir listesini belirtin.
Her eylem bir sütun üzerinde bir işlemdir.

Aşağıdaki eylemler desteklenir:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column's type, default expression and TTL.

Bu eylemler aşağıda ayrıntılı olarak açıklanmıştır.

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

Belirtilen tabloya yeni bir sütun ekler `name`, `type`, [`codec`](create.md#codecs) ve `default_expr` (bkz [Varsayılan ifadeler](create.md#create-default-values)).

Eğer... `IF NOT EXISTS` yan tümcesi dahil, sütun zaten varsa sorgu bir hata döndürmez. Belirtir specifyseniz `AFTER name_after` (başka bir sütunun adı), sütun tablo sütunları listesinde belirtilen sonra eklenir. Aksi takdirde, sütun tablonun sonuna eklenir. Bir tablonun başına bir sütun eklemek için bir yol olduğunu unutmayın. Bir eylem zinciri için, `name_after` önceki eylemlerden birine eklenen bir sütunun adı olabilir.

Bir sütun eklemek, verilerle herhangi bir işlem yapmadan tablo yapısını değiştirir. Sonra veriler diskte görünmüyor `ALTER`. Tablodan okurken bir sütun için veri eksikse, varsayılan değerlerle doldurulur (varsa, varsayılan ifadeyi gerçekleştirerek veya sıfır veya boş dizeler kullanarak). Sütun, veri parçalarını birleştirdikten sonra diskte görünür (bkz. [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)).

Bu yaklaşım bize tamamlamak için izin verir `ALTER` eski verilerin hacmini arttırmadan anında sorgulayın.

Örnek:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Sütun adı ile siler `name`. Eğer... `IF EXISTS` yan tümcesi belirtilir, sütun yoksa sorgu bir hata döndürmez.

Dosya sisteminden veri siler. Bu, tüm dosyaları sildiğinden, sorgu neredeyse anında tamamlanır.

Örnek:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Belirtilen bölüm için bir sütundaki tüm verileri sıfırlar. Bölümdeki bölüm adını ayarlama hakkında daha fazla bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

Eğer... `IF EXISTS` yan tümcesi belirtilir, sütun yoksa sorgu bir hata döndürmez.

Örnek:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

Sütuna bir yorum ekler. Eğer... `IF EXISTS` yan tümcesi belirtilir, sütun yoksa sorgu bir hata döndürmez.

Her sütunun bir yorumu olabilir. Sütun için bir yorum zaten varsa, yeni bir yorum önceki yorumun üzerine yazar.

Yorumlar saklanır `comment_expression` tarafından döndürülen sütun [DESCRIBE TABLE](misc.md#misc-describe-table) sorgu.

Örnek:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

Bu sorgu değişiklikleri `name` sütun özellikleri:

-   Tür

-   Varsayılan ifade

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

Eğer... `IF EXISTS` yan tümcesi belirtilir, sütun yoksa sorgu bir hata döndürmez.

Türü değiştirirken, değerler sanki [toType](../../sql-reference/functions/type-conversion-functions.md) fonksiyonlar onlara uygulandı. Yalnızca varsayılan ifade değiştirilirse, sorgu karmaşık bir şey yapmaz ve neredeyse anında tamamlanır.

Örnek:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

Birkaç işlem aşaması vardır:

-   Geçici (yeni) dosyaları değiştirilmiş verilerle hazırlama.
-   Eski dosyaları yeniden adlandırma.
-   Geçici (yeni) dosyaları eski adlara yeniden adlandırma.
-   Eski dosyaları silme.

Sadece ilk aşama zaman alır. Bu aşamada bir hata varsa, veriler değişmez.
Ardışık aşamalardan biri sırasında bir hata varsa, veriler el ile geri yüklenebilir. Eski dosyalar dosya sisteminden silindi, ancak yeni dosyaların verileri diske yazılmadı ve kaybolduysa istisnadır.

Bu `ALTER` sütunları değiştirmek için sorgu çoğaltılır. Talimatlar ZooKeeper kaydedilir, daha sonra her kopya bunları uygular. Tüm `ALTER` sorgular aynı sırada çalıştırılır. Sorgu, diğer yinelemeler üzerinde tamamlanması uygun eylemleri bekler. Ancak, yinelenen bir tablodaki sütunları değiştirmek için bir sorgu kesilebilir ve tüm eylemler zaman uyumsuz olarak gerçekleştirilir.

#### Sorgu sınırlamalarını değiştir {#alter-query-limitations}

Bu `ALTER` sorgu oluşturmak ve iç içe veri yapıları, ancak tüm iç içe veri yapıları ayrı öğeleri (sütunlar) silmenizi sağlar. İç içe geçmiş bir veri yapısı eklemek için, aşağıdaki gibi bir ada sahip sütunlar ekleyebilirsiniz `name.nested_name` ve türü `Array(T)`. İç içe geçmiş bir veri yapısı, noktadan önce aynı öneki olan bir ada sahip birden çok dizi sütununa eşdeğerdir.

Birincil anahtardaki veya örnekleme anahtarındaki sütunları silmek için destek yoktur. `ENGINE` ifade). Birincil anahtarda bulunan sütunların türünü değiştirmek, yalnızca bu değişiklik verilerin değiştirilmesine neden olmazsa mümkündür (örneğin, bir numaraya değer eklemenize veya bir türden değiştirmenize izin verilir `DateTime` -e doğru `UInt32`).

Eğer... `ALTER` sorgu, ihtiyacınız olan tablo değişikliklerini yapmak için yeterli değildir, yeni bir tablo oluşturabilir, verileri kullanarak kopyalayabilirsiniz. [INSERT SELECT](insert-into.md#insert_query_insert-select) sorgu, daha sonra tabloları kullanarak geçiş [RENAME](misc.md#misc_operations-rename) sorgu ve eski tabloyu silin. Kullanabilirsiniz [clickhouse-fotokopi makinesi](../../operations/utilities/clickhouse-copier.md) bir alternatif olarak `INSERT SELECT` sorgu.

Bu `ALTER` sorgu tüm okur ve tablo için yazar engeller. Başka bir deyişle, Eğer uzun `SELECT` zamanda çalışıyor `ALTER` sorgu `ALTER` sorgu tamamlanmasını bekleyecektir. Aynı zamanda, aynı tablodaki tüm yeni sorgular bu sırada bekleyecektir `ALTER` çalışıyor.

Verileri kendileri saklamayan tablolar için (örneğin `Merge` ve `Distributed`), `ALTER` sadece tablo yapısını değiştirir ve alt tabloların yapısını değiştirmez. Örneğin, ALTER for a çalıştırırken `Distributed` tablo, ayrıca çalıştırmak gerekir `ALTER` tüm uzak sunuculardaki tablolar için.

### Anahtar ifadelerle manipülasyonlar {#manipulations-with-key-expressions}

Aşağıdaki komut desteklenir:

``` sql
MODIFY ORDER BY new_expression
```

Sadece tablolar için çalışır [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) aile (dahil
[çoğaltıyordu](../../engines/table-engines/mergetree-family/replication.md) Tablolar). Komutu değiştirir
[sıralama anahtarı](../../engines/table-engines/mergetree-family/mergetree.md) tablonun
-e doğru `new_expression` (bir ifade veya ifadelerin bir tuple). Birincil anahtar aynı kalır.

Komut, yalnızca meta verileri değiştirdiği bir anlamda hafiftir. Veri parçası özelliği tutmak için
satırlar sıralama anahtarı ifadesi tarafından sıralanır varolan sütunları içeren ifadeler ekleyemezsiniz
sıralama anahtarına (yalnızca sütun tarafından eklenen `ADD COLUMN` aynı komut `ALTER` sorgu).

### Veri atlama endeksleri ile manipülasyonlar {#manipulations-with-data-skipping-indices}

Sadece tablolar için çalışır [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) aile (dahil
[çoğaltıyordu](../../engines/table-engines/mergetree-family/replication.md) Tablolar). Aşağıdaki işlemler
mevcuttur:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Tablolar meta dizin açıklama ekler.

-   `ALTER TABLE [db].name DROP INDEX name` - Tablolar meta dizin açıklama kaldırır ve diskten dizin dosyalarını siler.

Bu komutlar, yalnızca meta verileri değiştirdikleri veya dosyaları kaldırdıkları bir anlamda hafiftir.
Ayrıca, çoğaltılırlar (ZooKeeper aracılığıyla indeks meta verilerini senkronize etme).

### Kısıtlamalar ile manipülasyonlar {#manipulations-with-constraints}

Daha fazla görmek [kısıtlamalar](create.md#constraints)

Kısıtlamalar eklenebilir veya aşağıdaki sözdizimi kullanılarak silinebilir:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Sorgular eklemek veya hemen işlenir, böylece tablodan kısıtlamaları hakkında meta verileri kaldırın.

Kısıtlama kontrolü *idam edilm willeyecek* eklen .mişse mevcut ver .ilerde

Çoğaltılmış tablolardaki tüm değişiklikler Zookeeper'a yayınlanır, bu nedenle diğer kopyalara uygulanır.

### Bölümler ve parçalar ile manipülasyonlar {#alter_manipulations-with-partitions}

Aşağıdaki işlemler ile [bölümler](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) mevcuttur:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` dizin ve unutun.
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` tabloya dizin.
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) - Veri bölümünü bir tablodan diğerine kopyalar ve değiştirir.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition)(\#alter\_move\_to\_table-partition) - veri bölümünü bir tablodan diğerine taşıyın.
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) - Bir bölümdeki belirtilen sütunun değerini sıfırlar.
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) - Bir bölümde belirtilen ikincil dizini sıfırlar.
-   [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) – Move partition/data part to another disk or volume.

<!-- -->

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Belirtilen bölüm için tüm verileri `detached` dizin. Sunucu, yok gibi ayrılmış veri Bölümü hakkında unutur. Sunucu, bu verileri siz yapana kadar bilmeyecektir. [ATTACH](#alter_attach-partition) sorgu.

Örnek:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

Bir bölümdeki bölüm ifadesini ayarlama hakkında bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

Sorgu yürütüldükten sonra, veri ile istediğiniz her şeyi yapabilirsiniz `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` tüm kopyalarda dizin. Bu sorguyu yalnızca bir lider yinelemesinde yürütebileceğinizi unutmayın. Bir kopya bir lider olup olmadığını öğrenmek için `SELECT` sorgu için [sistem.yinelemeler](../../operations/system-tables.md#system_tables-replicas) Tablo. Alternatif olarak, bir yapmak daha kolaydır `DETACH` tüm yinelemelerde sorgu - tüm yinelemeler, lider yinelemesi dışında bir özel durum oluşturur.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Belirtilen bölümü tablodan siler. Bu sorgu bölümü etkin olarak etiketler ve verileri tamamen yaklaşık 10 dakika içinde siler.

Bir bölümdeki bölüm ifadesini ayarlama hakkında bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Belirtilen bölümü veya belirtilen bölümün tüm bölümlerini kaldırır `detached`.
Bir bölümdeki bölüm ifadesini ayarlama hakkında daha fazla bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Tablodan veri ekler `detached` dizin. Tüm bir bölüm veya ayrı bir bölüm için veri eklemek mümkündür. Örnekler:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Bir bölümdeki bölüm ifadesini ayarlama hakkında daha fazla bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

Bu sorgu çoğaltılır. Çoğaltma başlatıcısı, veri olup olmadığını denetler. `detached` dizin. Veri varsa, sorgu bütünlüğünü denetler. Her şey doğruysa, sorgu verileri tabloya ekler. Diğer tüm yinelemeler, çoğaltma başlatıcısından verileri karşıdan yükleyin.

Böylece veri koyabilirsiniz `detached` bir kopya üzerinde dizin ve `ALTER ... ATTACH` tüm yinelemelerde tabloya eklemek için sorgu.

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

Bu sorgu, veri bölümünü `table1` -e doğru `table2` exsisting için veri ekler `table2`. Verilerin silinmeyeceğini unutmayın `table1`.

Sorgunun başarıyla çalışması için aşağıdaki koşulların karşılanması gerekir:

-   Her iki tablo da aynı yapıya sahip olmalıdır.
-   Her iki tablo da aynı bölüm anahtarına sahip olmalıdır.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

Bu sorgu, veri bölümünü `table1` -e doğru `table2` ve mevcut bölümün yerini alır `table2`. Verilerin silinmeyeceğini unutmayın `table1`.

Sorgunun başarıyla çalışması için aşağıdaki koşulların karşılanması gerekir:

-   Her iki tablo da aynı yapıya sahip olmalıdır.
-   Her iki tablo da aynı bölüm anahtarına sahip olmalıdır.

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

Bu sorgu, veri bölümünü `table_source` -e doğru `table_dest` verileri silme ile `table_source`.

Sorgunun başarıyla çalışması için aşağıdaki koşulların karşılanması gerekir:

-   Her iki tablo da aynı yapıya sahip olmalıdır.
-   Her iki tablo da aynı bölüm anahtarına sahip olmalıdır.
-   Her iki tablo da aynı motor ailesi olmalıdır. (çoğaltılmış veya çoğaltılmamış)
-   Her iki tablo da aynı depolama ilkesine sahip olmalıdır.

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Bir bölümdeki belirtilen sütundaki tüm değerleri sıfırlar. Eğer... `DEFAULT` bir tablo oluştururken yan tümcesi belirlendi, bu sorgu sütun değerini belirtilen varsayılan değere ayarlar.

Örnek:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

Bu sorgu, belirtilen bir bölümün yerel yedeğini oluşturur. Eğer... `PARTITION` yan tümcesi atlandı, sorgu aynı anda tüm bölümlerin yedeğini oluşturur.

!!! note "Not"
    Tüm yedekleme işlemi sunucuyu durdurmadan gerçekleştirilir.

Eski tarz tablolar için bölüm adının önekini belirtebileceğinizi unutmayın (örneğin, ‘2019’)- daha sonra sorgu tüm ilgili bölümler için yedek oluşturur. Bir bölümdeki bölüm ifadesini ayarlama hakkında bilgi edinin [Bölüm ifadesi nasıl belirlenir](#alter-how-to-specify-part-expr).

Yürütme sırasında, bir veri anlık görüntüsü için sorgu, bir tablo verilerine sabit bağlantılar oluşturur. Hardlinks dizine yerleştirilir `/var/lib/clickhouse/shadow/N/...`, nere:

-   `/var/lib/clickhouse/` yapılandırmada belirtilen çalışma ClickHouse dizinidir.
-   `N` yedeklemenin artımlı sayısıdır.

!!! note "Not"
    Kullanıyorsanız [bir tablodaki veri depolama için disk kümesi](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes), bu `shadow/N` dizin tarafından eşleşen veri parçalarını depolamak, her diskte görünür `PARTITION` İfade.

Dizinlerin aynı yapısı, içinde olduğu gibi yedek içinde oluşturulur `/var/lib/clickhouse/`. Sorgu gerçekleştirir ‘chmod’ tüm dosyalar için, onlara yazmayı yasaklamak.

Yedeklemeyi oluşturduktan sonra, verileri `/var/lib/clickhouse/shadow/` uzak sunucuya ve sonra yerel sunucudan silin. Not `ALTER t FREEZE PARTITION` sorgu çoğaltılmaz. Yalnızca yerel sunucuda yerel bir yedekleme oluşturur.

Sorgu neredeyse anında yedekleme oluşturur (ancak önce geçerli sorguları ilgili tabloya çalışmayı bitirmek için bekler).

`ALTER TABLE t FREEZE PARTITION` tablo meta verilerini değil, yalnızca verileri kopyalar. Tablo meta verilerinin yedeğini almak için dosyayı kopyalayın `/var/lib/clickhouse/metadata/database/table.sql`

Bir yedekten veri geri yüklemek için aşağıdakileri yapın:

1.  Yoksa tablo oluşturun. Sorguyu görüntülemek için kullanın .sql dosyası (değiştir `ATTACH` içinde ile `CREATE`).
2.  Veri kopyalama `data/database/table/` yedekleme içindeki dizin `/var/lib/clickhouse/data/database/table/detached/` dizin.
3.  Koşmak `ALTER TABLE t ATTACH PARTITION` verileri bir tabloya eklemek için sorgular.

Yedeklemeden geri yükleme, sunucuyu durdurmayı gerektirmez.

Yedekleme ve geri yükleme verileri hakkında daha fazla bilgi için bkz: [Veri Yedekleme](../../operations/backup.md) bölme.

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

Sorgu benzer çalışır `CLEAR COLUMN`, ancak bir sütun verileri yerine bir dizini sıfırlar.

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Başka bir sunucudan bir bölüm indirir. Bu sorgu yalnızca çoğaltılmış tablolar için çalışır.

Sorgu aşağıdakileri yapar:

1.  Bölümü belirtilen parçadan indirir. İçinde ‘path-in-zookeeper’ zookeeper içinde shard için bir yol belirtmeniz gerekir.
2.  Sonra sorgu indirilen verileri `detached` directory of the `table_name` Tablo. Kullan... [ATTACH PARTITION\|PART](#alter_attach-partition) tabloya veri eklemek için sorgu.

Mesela:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Not thate that:

-   Bu `ALTER ... FETCH PARTITION` sorgu çoğaltılmaz. Bu bölüm için yerleştirir `detached` yalnızca yerel sunucuda dizin.
-   Bu `ALTER TABLE ... ATTACH` sorgu çoğaltılır. Verileri tüm yinelemelere ekler. Veriler, kopyalardan birine eklenir. `detached` dizin ve diğerlerine - komşu kopyalardan.

İndirmeden önce, sistem bölümün olup olmadığını ve tablo yapısının eşleşip eşleşmediğini kontrol eder. En uygun yineleme, sağlıklı yinelemeler otomatik olarak seçilir.

Sorgu çağrılsa da `ALTER TABLE`, tablo yapısını değiştirmez ve tabloda bulunan verileri hemen değiştirmez.

#### MOVE PARTITION\|PART {#alter_move-partition}

Bölümleri veya veri parçalarını başka bir birime veya diske taşır. `MergeTree`- motor masaları. Görmek [Veri depolama için birden fazla blok cihazı kullanma](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

Bu `ALTER TABLE t MOVE` sorgu:

-   Çoğaltılamaz, çünkü farklı çoğaltmalar farklı depolama ilkelerine sahip olabilir.
-   Belirtilen disk veya birim yapılandırılmamışsa bir hata döndürür. Depolama ilkesinde belirtilen veri taşıma koşulları uygulanamazsa, sorgu da bir hata döndürür.
-   Durumda bir hata döndürebilir, taşınacak veriler zaten bir arka plan işlemi tarafından taşındığında, eşzamanlı `ALTER TABLE t MOVE` sorgu veya arka plan veri birleştirme sonucu. Bir kullanıcı bu durumda herhangi bir ek eylem gerçekleştirmemelidir.

Örnek:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### Bölüm ifadesi nasıl ayarlanır {#alter-how-to-specify-part-expr}

Bölüm ifadesini şu şekilde belirtebilirsiniz `ALTER ... PARTITION` farklı şekillerde sorgular:

-   Bu gibi bir değer `partition` sütun `system.parts` Tablo. Mesela, `ALTER TABLE visits DETACH PARTITION 201901`.
-   Tablo sütunundan ifade olarak. Sabitler ve sabit ifadeler desteklenir. Mesela, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   Bölüm kimliğini kullanma. Partition ID, dosya sistemindeki ve Zookeeper'daki bölümlerin adları olarak kullanılan bölümün (mümkünse insan tarafından okunabilir) bir dize tanımlayıcısıdır. Bölüm kimliği belirtilmelidir `PARTITION ID` fık .ra, tek tırnak içinde. Mesela, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   İn the [ALTER ATTACH PART](#alter_attach-partition) ve [DROP DETACHED PART](#alter_drop-detached) sorgu, bir parçanın adını belirtmek için, bir değer ile dize literal kullanın `name` sütun [sistem.detached\_parts](../../operations/system-tables.md#system_tables-detached_parts) Tablo. Mesela, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Bölüm belirtilirken tırnak kullanımı bölüm ifadesi türüne bağlıdır. Örneğin, için `String` yazın, adını tırnak içinde belirtmeniz gerekir (`'`). İçin `Date` ve `Int*` türleri hiçbir tırnak gereklidir.

Eski stil tablolar için, bölümü bir sayı olarak belirtebilirsiniz `201901` veya bir dize `'201901'`. Yeni stil tabloları için sözdizimi türleri ile daha sıkı (değerleri giriş biçimi için ayrıştırıcı benzer).

Yukarıdaki tüm kurallar için de geçerlidir [OPTIMIZE](misc.md#misc_operations-optimize) sorgu. Bölümlenmemiş bir tabloyu en iyi duruma getirirken tek bölümü belirtmeniz gerekiyorsa, ifadeyi ayarlayın `PARTITION tuple()`. Mesela:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

Örnekleri `ALTER ... PARTITION` sorgular testlerde gösterilmiştir [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) ve [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Tablo TTL ile manipülasyonlar {#manipulations-with-table-ttl}

Değiştirebilirsiniz [tablo TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) aşağıdaki formun bir isteği ile:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### Alter sorgularının eşzamanlılığı {#synchronicity-of-alter-queries}

Replicatable olmayan tablolar için, tüm `ALTER` sorgular eşzamanlı olarak gerçekleştirilir. Replicatable tablolar için, sorgu yalnızca uygun eylemler için yönergeler ekler `ZooKeeper` ve eylemlerin kendileri mümkün olan en kısa sürede gerçekleştirilir. Ancak, sorgu tüm yinelemeler üzerinde tamamlanması için bu eylemleri bekleyebilir.

İçin `ALTER ... ATTACH|DETACH|DROP` sorgular, kullanabilirsiniz `replication_alter_partitions_sync` bekleyen kurmak için ayarlama.
Olası değerler: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### Mutasyonlar {#alter-mutations}

Mutasyonlar, bir tablodaki satırların değiştirilmesine veya silinmesine izin veren bir alter query varyantıdır. Standart aksine `UPDATE` ve `DELETE` nokta veri değişikliklerine yönelik sorgular, mutasyonlar, bir tablodaki çok sayıda satırı değiştiren ağır işlemler için tasarlanmıştır. İçin desteklenen `MergeTree` çoğaltma desteği olan motorlar da dahil olmak üzere tablo motorları ailesi.

Varolan tablolar olduğu gibi mutasyonlar için hazırdır(dönüştürme gerekmez), ancak ilk mutasyon bir tabloya uygulandıktan sonra Meta Veri formatı önceki sunucu sürümleriyle uyumsuz hale gelir ve önceki bir sürüme geri dönmek imkansız hale gelir.

Şu anda mevcut komutlar:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

Bu `filter_expr` tip olmalıdır `UInt8`. Sorgu, bu ifadenin sıfır olmayan bir değer aldığı tablodaki satırları siler.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

Bu `filter_expr` tip olmalıdır `UInt8`. Bu sorgu, belirtilen sütunların değerlerini, satırlardaki karşılık gelen ifadelerin değerlerine güncelleştirir. `filter_expr` sıfır olmayan bir değer alır. Değerleri kullanarak sütun türüne döküm `CAST` operatör. Birincil veya bölüm anahtarının hesaplanmasında kullanılan sütunları güncelleştirme desteklenmiyor.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

Sorgu ikincil dizini yeniden oluşturur `name` bölümünde `partition_name`.

Bir sorgu virgülle ayrılmış birkaç komut içerebilir.

\* MergeTree tabloları mutasyonları için tüm veri parçalarını yeniden yazarak yürütün. Atomiklik yoktur-parçalar, hazır oldukları anda mutasyona uğramış parçalar için ikame edilir ve bir `SELECT` bir mutasyon sırasında yürütülmeye başlayan sorgu, henüz mutasyona uğramamış olan parçalardan gelen verilerle birlikte mutasyona uğramış olan parçalardan gelen verileri görecektir.

Mutasyonlar tamamen yaratılış sırasına göre sıralanır ve her bir parçaya bu sırayla uygulanır. Mutasyonlar da kısmen ekler ile sıralanır-mutasyon gönderilmeden önce tabloya eklenen veriler mutasyona uğrayacak ve bundan sonra eklenen veriler mutasyona uğramayacaktır. Mutasyonların ekleri hiçbir şekilde engellemediğini unutmayın.

Mutasyon girişi eklendikten hemen sonra bir mutasyon sorgusu döner(çoğaltılmış tablolar Zookeeper'a, çoğaltılmamış tablolar için-dosya sistemine). Mutasyonun kendisi sistem profili ayarlarını kullanarak eşzamansız olarak yürütür. Mutasyonların ilerlemesini izlemek için kullanabilirsiniz [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) Tablo. Başarıyla gönderilen BIR mutasyon, ClickHouse sunucuları yeniden başlatılmış olsa bile yürütmeye devam edecektir. Gönderildikten sonra mutasyonu geri almanın bir yolu yoktur, ancak mutasyon herhangi bir nedenle sıkışmışsa, [`KILL MUTATION`](misc.md#kill-mutation) sorgu.

Bitmiş mutasyonlar için girişler hemen silinmez (korunmuş girişlerin sayısı, `finished_mutations_to_keep` depolama motoru parametresi). Eski mutasyon girişleri silinir.

## ALTER USER {#alter-user-statement}

ClickHouse kullanıcı hesaplarını değiştirir.

### Sözdizimi {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Açıklama {#alter-user-dscr}

Kullanmak `ALTER USER` sen olmalı [ALTER USER](grant.md#grant-access-management) ayrıcalık.

### Örnekler {#alter-user-examples}

Verilen rolleri varsayılan olarak ayarla:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

Roller daha önce bir kullanıcıya verilmezse, ClickHouse bir istisna atar.

Verilen tüm rolleri varsayılan olarak ayarlayın:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Gelecekte bir kullanıcıya bir rol verilecekse, otomatik olarak varsayılan hale gelecektir.

Verilen tüm rolleri varsayılan olarak ayarlama `role1` ve `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

## ALTER ROLE {#alter-role-statement}

Rolleri değiştirir.

### Sözdizimi {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## ALTER ROW POLICY {#alter-row-policy-statement}

Satır ilkesini değiştirir.

### Sözdizimi {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER QUOTA {#alter-quota-statement}

Kotaları değiştirir.

### Sözdizimi {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Kotaları değiştirir.

### Sözdizimi {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
