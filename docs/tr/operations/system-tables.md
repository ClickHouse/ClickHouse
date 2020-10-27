---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "Sistem Tablolar\u0131"
---

# Sistem Tabloları {#system-tables}

Sistem tabloları, sistemin işlevselliğinin bir kısmını uygulamak ve sistemin nasıl çalıştığı hakkında bilgilere erişim sağlamak için kullanılır.
Bir sistem tablosunu silemezsiniz (ancak ayırma işlemini gerçekleştirebilirsiniz).
Sistem tablolarında diskte veri bulunan dosyalar veya meta verilere sahip dosyalar yoktur. Sunucu, başlatıldığında tüm sistem tablolarını oluşturur.
Sistem tabloları salt okunur.
Bulun theurlar. ‘system’ veritabanı.

## sistem.asynchronous\_metrics {#system_tables-asynchronous_metrics}

Arka planda periyodik olarak hesaplanan metrikleri içerir. Örneğin, kullanılan RAM miktarı.

Sütun:

-   `metric` ([Dize](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

**Örnek**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**Ayrıca Bakınız**

-   [İzleme](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [sistem.metrik](#system_tables-metrics) — Contains instantly calculated metrics.
-   [sistem.etkinlik](#system_tables-events) — Contains a number of events that have occurred.
-   [sistem.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## sistem.kümeler {#system-clusters}

Yapılandırma dosyasında bulunan kümeler ve içindeki sunucular hakkında bilgi içerir.

Sütun:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (Uİnt32) - bu ana bilgisayarın çoğaltma ulaşamadı sayısı.
-   `estimated_recovery_time` (Uİnt32) - çoğaltma hata sayısı sıfırlanana kadar saniye kaldı ve normale döndü olarak kabul edilir.

Lütfen unutmayın `errors_count` küme için sorgu başına bir kez güncelleştirilir, ancak `estimated_recovery_time` isteğe bağlı olarak yeniden hesaplanır. Yani sıfır olmayan bir durum olabilir `errors_count` ve sıfır `estimated_recovery_time`, sonraki sorgu sıfır olacak `errors_count` ve hiçbir hata yokmuş gibi çoğaltma kullanmayı deneyin.

**Ayrıca bakınız**

-   [Masa motoru Dağıt Distributedıldı](../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap ayarı](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life ayarı](settings/settings.md#settings-distributed_replica_error_half_life)

## sistem.sütun {#system-columns}

Tüm tablolardaki sütunlar hakkında bilgi içerir.

Benzer bilgileri almak için bu tabloyu kullanabilirsiniz [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) sorgu, ancak aynı anda birden çok tablo için.

Bu `system.columns` tablo aşağıdaki sütunları içerir (sütun türü parantez içinde gösterilir):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) varsayılan değer veya tanımlanmamışsa boş bir dize için.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## sistem.katılımcılar {#system-contributors}

Katkıda bulunanlar hakkında bilgi içerir. Rastgele sırayla tüm constributors. Sipariş, sorgu yürütme zamanında rasgele olur.

Sütun:

-   `name` (String) — Contributor (author) name from git log.

**Örnek**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

Tabloda kendinizi bulmak için bir sorgu kullanın:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## sistem.veritabanılar {#system-databases}

Bu tablo, adı verilen tek bir dize sütunu içerir ‘name’ – the name of a database.
Sunucunun bildiği her veritabanı, tabloda karşılık gelen bir girdiye sahiptir.
Bu sistem tablosu uygulamak için kullanılır `SHOW DATABASES` sorgu.

## sistem.detached\_parts {#system_tables-detached_parts}

Müstakil parçaları hakkında bilgiler içerir [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) Tablolar. Bu `reason` sütun, parçanın neden ayrıldığını belirtir. Kullanıcı tarafından ayrılmış parçalar için sebep boştur. Bu tür parçalar ile eklenebilir [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) komut. Diğer sütunların açıklaması için bkz. [sistem.parçalar](#system_tables-parts). Bölüm adı geçersiz ise, bazı sütunların değerleri olabilir `NULL`. Bu tür parçalar ile silinebilir [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## sistem.sözlükler {#system_tables-dictionaries}

Hakkında bilgi içerir [dış söz dictionarieslükler](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Sütun:

-   `database` ([Dize](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([Dize](../sql-reference/data-types/string.md)) — [Sözlük adı](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) sorgu, zaman aşımı, sözlük yapılandırması değişti).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([Dize](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([Dize](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [Sözlükleri bellekte saklama](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Anahtar tipi](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Sayısal Tuş ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([Dize](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([Dizi](../sql-reference/data-types/array.md)([Dize](../sql-reference/data-types/string.md))) — Array of [öznitelik adları](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) sözlük tarafından sağlanmıştır.
-   `attribute.types` ([Dizi](../sql-reference/data-types/array.md)([Dize](../sql-reference/data-types/string.md))) — Corresponding array of [öznitelik türleri](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) sözlük tarafından sağlanmaktadır.
-   `bytes_allocated` ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([Dize](../sql-reference/data-types/string.md)) — Text describing the [veri kaynağı](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) sözlük için.
-   `lifetime_min` ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [ömür](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) bellekteki sözlüğün ardından ClickHouse sözlüğü yeniden yüklemeye çalışır (eğer `invalidate_query` ayarlanır, daha sonra sadece değiştiyse). Saniyeler içinde ayarlayın.
-   `lifetime_max` ([Uİnt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [ömür](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) bellekteki sözlüğün ardından ClickHouse sözlüğü yeniden yüklemeye çalışır (eğer `invalidate_query` ayarlanır, daha sonra sadece değiştiyse). Saniyeler içinde ayarlayın.
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([Dize](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**Örnek**

Sözlüğü yapılandırın.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Sözlüğün yüklendiğinden emin olun.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## sistem.etkinlik {#system_tables-events}

Sistemde meydana gelen olayların sayısı hakkında bilgi içerir. Örneğin, tabloda kaç tane bulabilirsiniz `SELECT` ClickHouse sunucusu başladığından beri sorgular işlendi.

Sütun:

-   `event` ([Dize](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([Dize](../sql-reference/data-types/string.md)) — Event description.

**Örnek**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [sistem.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistem.metrik](#system_tables-metrics) — Contains instantly calculated metrics.
-   [sistem.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [İzleme](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistem.işlevler {#system-functions}

Normal ve toplama işlevleri hakkında bilgi içerir.

Sütun:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## sistem.graphite\_retentions {#system-graphite-retentions}

Parametreleri hakkında bilgi içerir [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) tablo usedlarında kullanılan [\* Graphıtemergetree](../engines/table-engines/mergetree-family/graphitemergetree.md) motorlar.

Sütun:

-   `config_name` (Dize) - `graphite_rollup` parametre adı.
-   `regexp` (String) - metrik adı için bir desen.
-   `function` (String) - toplama işlevinin adı.
-   `age` (Uint64) - saniye cinsinden verilerin minimum yaş.
-   `precision` (Uİnt64) - verilerin yaşını saniyeler içinde tam olarak tanımlamak için.
-   `priority` (Uİnt16) - desen önceliği.
-   `is_default` (Uİnt8) - desenin varsayılan olup olmadığı.
-   `Tables.database` (Array (String)) - kullanılan veritabanı tablolarının adlarının dizisi `config_name` parametre.
-   `Tables.table` (Array (String)) - kullanılan tablo adları dizisi `config_name` parametre.

## sistem.birleştiriyor {#system-merges}

Mergetree ailesindeki tablolar için şu anda işlemde olan birleştirme ve parça mutasyonları hakkında bilgi içerir.

Sütun:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (Uİnt8 ) - 1 Bu işlem bir parça mutasyonu ise.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## sistem.metrik {#system_tables-metrics}

Anında hesaplanan veya geçerli bir değere sahip olabilir metrikleri içerir. Örneğin, aynı anda işlenen sorguların sayısı veya geçerli yineleme gecikmesi. Bu tablo her zaman güncel.

Sütun:

-   `metric` ([Dize](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([Dize](../sql-reference/data-types/string.md)) — Metric description.

Desteklenen metriklerin listesi [src / ortak / CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) ClickHouse kaynak dosyası.

**Örnek**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [sistem.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistem.etkinlik](#system_tables-events) — Contains a number of events that occurred.
-   [sistem.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [İzleme](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistem.metric\_log {#system_tables-metric_log}

Tablolardan metrik değerlerinin geçmişini içerir `system.metrics` ve `system.events`, periyodik olarak diske boşaltılır.
Metrik geçmişi koleksiyonunu açmak için `system.metric_log`, oluşturmak `/etc/clickhouse-server/config.d/metric_log.xml` aşağıdaki içerik ile:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**Örnek**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**Ayrıca bakınız**

-   [sistem.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistem.etkinlik](#system_tables-events) — Contains a number of events that occurred.
-   [sistem.metrik](#system_tables-metrics) — Contains instantly calculated metrics.
-   [İzleme](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistem.şiir {#system-numbers}

Bu tablo adında tek bir uint64 sütunu içerir ‘number’ bu sıfırdan başlayarak hemen hemen tüm doğal sayıları içerir.
Bu tabloyu testler için veya kaba kuvvet araması yapmanız gerekiyorsa kullanabilirsiniz.
Bu tablodan okumalar parallelized değil.

## sistem.numbers\_mt {#system-numbers-mt}

Olarak aynı ‘system.numbers’ ancak okumalar paralelleştirilmiştir. Sayılar herhangi bir sırayla iade edilebilir.
Testler için kullanılır.

## sistem.bir {#system-one}

Bu tablo, tek bir satır içeren tek bir satır içerir ‘dummy’ 0 değerini içeren uint8 sütunu.
SELECT sorgusu FROM yan tümcesi belirtmezse, bu tablo kullanılır.
Bu, diğer Dbms'lerde bulunan ikili tabloya benzer.

## sistem.parçalar {#system_tables-parts}

Bölümleri hakkında bilgi içerir [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) Tablolar.

Her satır bir veri bölümünü açıklar.

Sütun:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) sorgu.

    Biçimliler:

    -   `YYYYMM` ay otomatik bölümleme için.
    -   `any_string` el ile bölümleme yaparken.

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` dizin ayrıntısına göre (genellikle 8192) (bu ipucu uyarlanabilir ayrıntı için çalışmaz).

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sifash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) sıkıştırılmış dosyaların.

-   `hash_of_uncompressed_files` (`String`) – [sifash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) sıkıştırılmamış dosyaların (işaretli dosyalar, dizin dosyası vb.)).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sifash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) sıkıştırılmış dosyalardaki verilerin sıkıştırılmamış gibi.

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## sistem.part\_log {#system_tables-part-log}

Bu `system.part_log` tablo yalnızca aşağıdaki durumlarda oluşturulur: [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) sunucu ayarı belirtilir.

Bu tablo ile oluşan olaylar hakkında bilgi içerir [veri parçaları](../engines/table-engines/mergetree-family/custom-partitioning-key.md) in the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) veri ekleme veya birleştirme gibi aile tabloları.

Bu `system.part_log` tablo aşağıdaki sütunları içerir:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ bölümleme tarafından ise değer `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

Bu `system.part_log` tablo ilk veri ekleme sonra oluşturulur `MergeTree` Tablo.

## sistem.işleyişler {#system_tables-processes}

Bu sistem tablosu uygulamak için kullanılır `SHOW PROCESSLIST` sorgu.

Sütun:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` kullanan. Alan, bu sorgunun başlattığı bir sorgu için değil, belirli bir sorgunun kullanıcı adını içerir.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` sorgu istek sahibi sunucuda.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage) ayar.
-   `query` (String) – The query text. For `INSERT`, eklemek için veri içermez.
-   `query_id` (String) – Query ID, if defined.

## sistem.text\_log {#system-tables-text-log}

Günlük girişleri içerir. Bu tabloya giden günlük seviyesi ile sınırlı olabilir `text_log.level` sunucu ayarı.

Sütun:

-   `event_date` (`Date`)- Giriş tarihi.
-   `event_time` (`DateTime`)- Giriş zamanı.
-   `microseconds` (`UInt32`)- Girişin mikrosaniye.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`)- Giriş seviyesi.
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`)- Sorgunun kimliği.
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`)- Mesajın kendisi .
-   `revision` (`UInt32`)- ClickHouse revizyon.
-   `source_file` (`LowCardinality(String)`)- Günlüğü yapıldığı kaynak dosya.
-   `source_line` (`UInt64`)- Kaynak satır hangi günlüğü yapıldı.

## sistem.query\_log {#system_tables-query_log}

Sorguların yürütülmesi hakkında bilgi içerir. Her sorgu için, işlem başlangıç saatini, işlem süresini, hata mesajlarını ve diğer bilgileri görebilirsiniz.

!!! note "Not"
    Tablo için giriş verileri içermiyor `INSERT` sorgular.

ClickHouse bu tabloyu yalnızca [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) sunucu parametresi belirtilir. Bu parametre, günlük aralığı veya sorguların oturum açacağı tablonun adı gibi günlük kurallarını ayarlar.

Sorgu günlüğünü etkinleştirmek için, [log\_queries](settings/settings.md#settings-log-queries) parametre 1. Ayrıntılar için, bkz. [Ayarlar](settings/settings.md) bölme.

Bu `system.query_log` tablo iki tür sorgu kaydeder:

1.  Doğrudan istemci tarafından çalıştırılan ilk sorgular.
2.  Diğer sorgular tarafından başlatılan alt sorgular (dağıtılmış sorgu yürütme için). Bu tür sorgular için, üst sorgular hakkında bilgi `initial_*` sütun.

Sütun:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` sorgular, yazılı satır sayısı. Diğer sorgular için sütun değeri 0'dır.
-   `written_bytes` (UInt64) — For `INSERT` sorgular, yazılı bayt sayısı. Diğer sorgular için sütun değeri 0'dır.
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-müşteri](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi çalıştırılır.
-   `client_name` (String) — The [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemci adı.
-   `client_revision` (UInt32) — Revision of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_major` (UInt32) — Major version of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemci sürümü.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` yöntem kullanılmıştır.
    -   2 — `POST` yöntem kullanılmıştır.
-   `http_user_agent` (String) — The `UserAgent` başlık http isteğinde geçti.
-   `quota_key` (String) — The “quota key” belirtilen [kotalar](quotas.md) ayarı (bakınız `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [sistem.etkinlik](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` sütun.
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parametre 1.
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` sütun.

Her sorgu bir veya iki satır oluşturur `query_log` tablo, sorgunun durumuna bağlı olarak:

1.  Sorgu yürütme başarılı olursa, tip 1 ve 2 ile iki olay oluşturulur (bkz. `type` sütun).
2.  Sorgu işleme sırasında bir hata oluştu, iki olay türleri 1 ve 4 oluşturulur.
3.  Sorguyu başlatmadan önce bir hata oluşmuşsa, 3 tipi olan tek bir olay oluşturulur.

Varsayılan olarak, günlükleri 7.5 saniye aralıklarla tabloya eklenir. Bu aralığı ayarlayabilirsiniz [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) sunucu ayarı (bkz. `flush_interval_milliseconds` parametre). Günlükleri zorla bellek arabelleğinden tabloya temizlemek için `SYSTEM FLUSH LOGS` sorgu.

Tablo elle silindiğinde, otomatik olarak anında oluşturulur. Önceki tüm günlüklerin silineceğini unutmayın.

!!! note "Not"
    Günlüklerin depolama süresi sınırsızdır. Günlükler tablodan otomatik olarak silinmez. Eski günlüklerin kaldırılmasını kendiniz düzenlemeniz gerekir.

İçin keyfi bir bölümleme anahtarı belirtebilirsiniz `system.query_log` tablo içinde [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) sunucu ayarı (bkz. `partition_by` parametre).

## sistem.query\_thread\_log {#system_tables-query-thread-log}

Tablo, her sorgu yürütme iş parçacığı hakkında bilgi içerir.

ClickHouse bu tabloyu yalnızca [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) sunucu parametresi belirtilir. Bu parametre, günlük aralığı veya sorguların oturum açacağı tablonun adı gibi günlük kurallarını ayarlar.

Sorgu günlüğünü etkinleştirmek için, [log\_query\_threads](settings/settings.md#settings-log-query-threads) parametre 1. Ayrıntılar için, bkz. [Ayarlar](settings/settings.md) bölme.

Sütun:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` sorgular, yazılı satır sayısı. Diğer sorgular için sütun değeri 0'dır.
-   `written_bytes` (UInt64) — For `INSERT` sorgular, yazılı bayt sayısı. Diğer sorgular için sütun değeri 0'dır.
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-müşteri](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi çalıştırılır.
-   `client_name` (String) — The [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemci adı.
-   `client_revision` (UInt32) — Revision of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_major` (UInt32) — Major version of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemcisi.
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-müşteri](../interfaces/cli.md) veya başka bir TCP istemci sürümü.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` yöntem kullanılmıştır.
    -   2 — `POST` yöntem kullanılmıştır.
-   `http_user_agent` (String) — The `UserAgent` başlık http isteğinde geçti.
-   `quota_key` (String) — The “quota key” belirtilen [kotalar](quotas.md) ayarı (bakınız `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [sistem.etkinlik](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` sütun.

Varsayılan olarak, günlükleri 7.5 saniye aralıklarla tabloya eklenir. Bu aralığı ayarlayabilirsiniz [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) sunucu ayarı (bkz. `flush_interval_milliseconds` parametre). Günlükleri zorla bellek arabelleğinden tabloya temizlemek için `SYSTEM FLUSH LOGS` sorgu.

Tablo elle silindiğinde, otomatik olarak anında oluşturulur. Önceki tüm günlüklerin silineceğini unutmayın.

!!! note "Not"
    Günlüklerin depolama süresi sınırsızdır. Günlükler tablodan otomatik olarak silinmez. Eski günlüklerin kaldırılmasını kendiniz düzenlemeniz gerekir.

İçin keyfi bir bölümleme anahtarı belirtebilirsiniz `system.query_thread_log` tablo içinde [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) sunucu ayarı (bkz. `partition_by` parametre).

## sistem.trace\_log {#system_tables-trace_log}

Örnekleme sorgusu profiler tarafından toplanan yığın izlemeleri içerir.

ClickHouse bu tabloyu oluşturduğunda [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) sunucu yapılandırma bölümü ayarlanır. Ayrıca [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) ve [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) ayarlar ayarlan .malıdır.

Günlükleri analiz etmek için `addressToLine`, `addressToSymbol` ve `demangle` iç gözlem fonksiyonları.

Sütun:

-   `event_date` ([Tarihli](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([Uİnt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    Tarafından sunucuya Bağlan byırken `clickhouse-client`, benzer diz theg seeeyi görüyorsunuz `Connected to ClickHouse server version 19.18.1 revision 54429.`. Bu alan şunları içerir `revision` ama `version` bir sunucunun.

-   `timer_type` ([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` duvar saati zamanını temsil eder.
    -   `CPU` CPU süresini temsil eder.

-   `thread_number` ([Uİnt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([Dize](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) sistem tablosu.

-   `trace` ([Dizi (Uİnt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Örnek**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## sistem.yinelemeler {#system_tables-replicas}

Yerel sunucuda bulunan çoğaltılmış tablolar için bilgi ve durum içerir.
Bu tablo izleme için kullanılabilir. Tablo, her çoğaltılmış \* tablo için bir satır içerir.

Örnek:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

Sütun:

-   `database` (`String`)- Veritabanı adı
-   `table` (`String`)- Tablo adı
-   `engine` (`String`)- Tablo motor adı
-   `is_leader` (`UInt8`)- Kopya lider olup olmadığı.
    Bir seferde sadece bir kopya lider olabilir. Lider, gerçekleştirmek için arka plan birleştirmelerini seçmekten sorumludur.
    Yazma kullanılabilir ve bir oturum ZK, bir lider olup olmadığına bakılmaksızın olan herhangi bir yineleme için gerçekleştirilebilir unutmayın.
-   `can_become_leader` (`UInt8`)- Rep .lik leaderanın lider olarak seçil .ip seçil .emeyeceği.
-   `is_readonly` (`UInt8`)- Yinelemenin salt okunur modda olup olmadığı.
    Yapılandırmanın ZooKeeper ile bölümleri yoksa, zookeeper'daki oturumları yeniden başlatırken ve Zookeeper'daki oturum yeniden başlatılırken bilinmeyen bir hata oluşmuşsa bu mod açılır.
-   `is_session_expired` (`UInt8`)- ZooKeeper ile oturum süresi doldu. Temelde aynı `is_readonly`.
-   `future_parts` (`UInt32`)- Henüz yapılmamış ekler veya birleştirmelerin sonucu olarak görünecek veri parçalarının sayısı.
-   `parts_to_check` (`UInt32`)- Doğrulama için kuyruktaki veri parçalarının sayısı. Hasar görebileceğinden şüphe varsa, bir parça doğrulama kuyruğuna konur.
-   `zookeeper_path` (`String`)- ZooKeeper tablo verilerine yolu.
-   `replica_name` (`String`)- Zookeeper çoğaltma adı. Aynı tablonun farklı kopyaları farklı adlara sahiptir.
-   `replica_path` (`String`)- ZooKeeper çoğaltma veri yolu. Birleştirme ile aynı ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)- Tablo yapısının sürüm numarası. ALTER kaç kez gerçekleştirildiğini gösterir. Kopyaların farklı sürümleri varsa, bazı kopyaların tüm değişiklikleri henüz yapmadığı anlamına gelir.
-   `queue_size` (`UInt32`)- Yapılması beklenen işlemler için sıranın büyüklüğü. İşlemler, veri bloklarını, birleştirmeleri ve diğer bazı eylemleri eklemeyi içerir. Genellikle ile çakışmaktadır `future_parts`.
-   `inserts_in_queue` (`UInt32`)- Yapılması gereken veri bloklarının eklerinin sayısı. Eklemeler genellikle oldukça hızlı bir şekilde çoğaltılır. Bu sayı büyükse, bir şeylerin yanlış olduğu anlamına gelir.
-   `merges_in_queue` (`UInt32`)- Yapılmasını bekleyen birleştirme sayısı. Bazen birleştirmeler uzundur, bu nedenle bu değer uzun süre sıfırdan büyük olabilir.
-   `part_mutations_in_queue` (`UInt32`)- Yapılması beklenen Mut numberasyon sayısı.
-   `queue_oldest_time` (`DateTime`) - Eğer `queue_size` daha büyük 0, en eski işlem sıraya eklendiğinde gösterir.
-   `inserts_oldest_time` (`DateTime`) - Görmek `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`) - Görmek `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`) - Görmek `queue_oldest_time`

Sonraki 4 sütun, yalnızca ZK ile aktif bir oturumun olduğu sıfır olmayan bir değere sahiptir.

-   `log_max_index` (`UInt64`)- Genel faaliyet günlüğüne maksimum giriş numarası.
-   `log_pointer` (`UInt64`)- Çoğaltma yürütme kuyruğuna kopyalanan genel faaliyet günlüğüne maksimum giriş numarası, artı bir. Eğer `log_pointer` daha küçük `log_max_index` yanlış bir şey olduğunu.
-   `last_queue_update` (`DateTime`)- Kuyruk son kez güncellendiğinde.
-   `absolute_delay` (`UInt64`)- Geçerli kopyanın saniyeler içinde ne kadar büyük gecikme var.
-   `total_replicas` (`UInt8`)- Bu tablonun bilinen kopyalarının toplam sayısı.
-   `active_replicas` (`UInt8`)- ZooKeeper bir oturum var bu tablonun kopyaları sayısı (yani, işleyen kopyaları sayısı).

Tüm sütunları talep ederseniz, Tablo biraz yavaş çalışabilir, çünkü ZooKeeper birkaç okuma her satır için yapılır.
Son 4 sütun (log\_max\_ındex, log\_pointer, total\_replicas, active\_replicas) istemiyorsanız, tablo hızlı bir şekilde çalışır.

Örneğin, her şeyin böyle düzgün çalıştığını kontrol edebilirsiniz:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

Bu sorgu hiçbir şey döndürmezse, her şeyin yolunda olduğu anlamına gelir.

## sistem.ayarlar {#system-tables-system-settings}

Geçerli kullanıcı için oturum ayarları hakkında bilgi içerir.

Sütun:

-   `name` ([Dize](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([Dize](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([Uİnt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([Dize](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Nullable](../sql-reference/data-types/nullable.md)([Dize](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [kısıtlamalar](settings/constraints-on-settings.md#constraints-on-settings). Ayarın minimum değeri yoksa, şunları içerir [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../sql-reference/data-types/nullable.md)([Dize](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [kısıtlamalar](settings/constraints-on-settings.md#constraints-on-settings). Ayarın maksimum değeri yoksa, şunları içerir [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([Uİnt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**Örnek**

Aşağıdaki örnek, adı içeren ayarlar hakkında bilgi almak gösterilmiştir `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

Kullanımı `WHERE changed` örneğin, kontrol etmek istediğinizde yararlı olabilir:

-   Olsun yapılandırma dosyaları, ayarları doğru şekilde yüklenmiş ve kullanımdadır.
-   Geçerli oturumda değişen ayarlar.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**Ayrıca bakınız**

-   [Ayarlar](settings/index.md#session-settings-intro)
-   [Sorgular için izinler](settings/permissions-for-queries.md#settings_readonly)
-   [Ayarlardaki kısıtlamalar](settings/constraints-on-settings.md)

## sistem.table\_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## sistem.merge\_tree\_settings {#system-merge_tree_settings}

İçin ayarlar hakkında bilgi içerir `MergeTree` Tablolar.

Sütun:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## sistem.table\_engines {#system-table-engines}

Sunucu tarafından desteklenen tablo motorlarının açıklamasını ve özellik destek bilgilerini içerir.

Bu tablo aşağıdaki sütunları içerir (sütun türü parantez içinde gösterilir):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` yan.
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [endeksleri atlama](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` ve `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [veri çoğaltma](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

Örnek:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**Ayrıca bakınız**

-   MergeTree ailesi [sorgu yan tümceleri](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   Kafka [ayarlar](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   Katmak [ayarlar](../engines/table-engines/special/join.md#join-limitations-and-settings)

## sistem.Tablolar {#system-tables}

Sunucunun bildiği her tablonun meta verilerini içerir. Müstakil tablolar gösterilmez `system.tables`.

Bu tablo aşağıdaki sütunları içerir (sütun türü parantez içinde gösterilir):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (Uİnt8) - tablonun geçici olup olmadığını gösteren bayrak.

-   `data_path` (String) - dosya sistemindeki tablo verilerinin yolu.

-   `metadata_path` (String) - dosya sistemindeki tablo Meta Veri Yolu.

-   `metadata_modification_time` (DateTime) - tablo meta son değişiklik zamanı.

-   `dependencies_database` (Array (String)) - veritabanı bağımlılıkları.

-   `dependencies_table` (Array (String)) - Tablo bağımlılıkları ([MaterializedView](../engines/table-engines/special/materializedview.md) geçerli tabloya dayalı tablolar).

-   `create_table_query` (String) - tablo oluşturmak için kullanılan sorgu.

-   `engine_full` (String) - tablo motorunun parametreleri.

-   `partition_key` (String) - tabloda belirtilen bölüm anahtarı ifadesi.

-   `sorting_key` (String) - tabloda belirtilen sıralama anahtarı ifadesi.

-   `primary_key` (String) - tabloda belirtilen birincil anahtar ifadesi.

-   `sampling_key` (String) - tabloda belirtilen örnekleme anahtar ifadesi.

-   `storage_policy` (String) - depolama politikası:

    -   [MergeTree](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Dağılı](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable (Uİnt64)) - tablodaki tam satır sayısını hızlı bir şekilde belirlemek mümkün ise, toplam satır sayısı `Null` (underying dahil `Buffer` Tablo).

-   `total_bytes` (Nullable (Uİnt64)) - toplam bayt sayısı, eğer depolama alanındaki tablo için tam bayt sayısını hızlı bir şekilde belirlemek mümkün ise, aksi takdirde `Null` (**do Notes not** herhangi bir temel depolama içerir).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   Tablo verileri bellekte depolarsa, bellekte kullanılan bayt sayısını yaklaşık olarak döndürür.

Bu `system.tables` tablo kullanılır `SHOW TABLES` sorgu uygulaması.

## sistem.zookeeper {#system-zookeeper}

ZooKeeper yapılandırılmamışsa, tablo yok. Yapılandırmada tanımlanan ZooKeeper kümesinden veri okumayı sağlar.
Sorgu bir olmalıdır ‘path’ WH .ere madd .esindeki eşitlik koşulu. Bu veri almak istediğiniz çocuklar için ZooKeeper yoludur.

Sorgu `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` tüm çocuklar için veri çıkışı `/clickhouse` düğümlü.
Tüm kök düğümler için veri çıkışı yapmak için, path = yazın ‘/’.
Belirtilen yol ise ‘path’ yok, bir istisna atılır.

Sütun:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

Örnek:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## sistem.mutasyonlar {#system_tables-mutations}

Tablo hakkında bilgi içerir [mutasyonlar](../sql-reference/statements/alter.md#alter-mutations) MergeTree tabloları ve bunların ilerleme. Her mutasyon komutu tek bir satırla temsil edilir. Tablo aşağıdaki sütunlara sahiptir:

**veritabanı**, **Tablo** - Mutasyonun uygulandığı veritabanı ve tablonun adı.

**mutation\_id** - Mutasyonun kimliği. Çoğaltılmış tablolar için bu kimlikler znode adlarına karşılık gelir `<table_path_in_zookeeper>/mutations/` ZooKeeper dizin. Yinelenmemiş tablolar için kimlikler, tablonun veri dizinindeki dosya adlarına karşılık gelir.

**komut** - Mut commandasyon komut diz (gesi (sorgu afterdan sonra `ALTER TABLE [db.]table`).

**create\_time** - Bu mutasyon komutu idam için sunulduğunda.

**block\_numbers.partition\_id**, **block\_numbers.numara** - İç içe geçmiş bir sütun. Çoğaltılmış tabloların mutasyonları için, her bölüm için bir kayıt içerir: bölüm kimliği ve mutasyon tarafından elde edilen blok numarası (her bölümde, yalnızca bu bölümdeki mutasyon tarafından elde edilen blok sayısından daha az sayıda blok içeren parçalar mutasyona uğrayacaktır). Çoğaltılmamış tablolarda, tüm bölümlerdeki blok numaraları tek bir sıra oluşturur. Bu, çoğaltılmamış tabloların mutasyonları için, sütunun mutasyon tarafından elde edilen tek bir blok numarasına sahip bir kayıt içereceği anlamına gelir.

**parts\_to\_do** - Mutasyonun bitmesi için mutasyona uğraması gereken veri parçalarının sayısı.

**is\_done** - Mutasyon bitti mi? Not bile `parts_to_do = 0` çoğaltılmış bir tablonun mutasyonu, mutasyona uğraması gereken yeni bir veri parçası yaratacak uzun süren bir ekleme nedeniyle henüz yapılmamıştır.

Bazı bölümleri mutasyon ile ilgili sorunlar varsa, aşağıdaki sütunlar ek bilgi içerir:

**latest\_failed\_part** - Mutasyona uğramayan en son bölümün adı.

**latest\_fail\_time** - En son bölüm mutasyon başarısızlığı zamanı.

**latest\_fail\_reason** - En son bölüm mutasyon başarısızlığına neden olan istisna mesajı.

## sistem.diskler {#system_tables-disks}

İçinde tanımlanan diskler hakkında bilgi içerir [sunucu yapılandırması](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Sütun:

-   `name` ([Dize](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([Dize](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` disk yapılandırması parametresi.

## sistem.storage\_policies {#system_tables-storage_policies}

Depolama ilkeleri ve birimlerinde tanımlanan bilgiler içerir. [sunucu yapılandırması](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Sütun:

-   `policy_name` ([Dize](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([Dize](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([Ar Arrayray (String)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([Uİnt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

Depolama ilkesi birden fazla birim içeriyorsa, her birim için bilgiler tablonun tek tek satırında saklanır.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
