---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: SYSTEM
---

# Sistem sorguları {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Daha önce başarıyla yüklenen tüm sözlükleri yeniden yükler.
Varsayılan olarak, sözlükler tembel yüklenir (bkz [dictionaries\_lazy\_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), bu nedenle başlangıçta otomatik olarak yüklenmek yerine, dictGet işlevi aracılığıyla ilk erişimde başlatılır veya ENGİNE = Dictionary ile tablolardan seçim yapılır. Bu `SYSTEM RELOAD DICTIONARIES` sorgu bu sözlükleri yeniden yükler (yüklü).
Her zaman döner `Ok.` sözlük güncellemesinin sonucu ne olursa olsun.

## Sözlük Dictionary\_name yeniden yükle {#query_language-system-reload-dictionary}

Tamamen bir sözlük reloads `dictionary_name`, sözlük durumuna bakılmaksızın (LOADED / NOT\_LOADED / FAİLED).
Her zaman döner `Ok.` ne olursa olsun sözlük güncelleme sonucu.
Sözlüğün durumu sorgulanarak kontrol edilebilir `system.dictionaries` Tablo.

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Clickhouse'un iç DNS önbelleğini sıfırlar. Bazen (eski ClickHouse sürümleri için) altyapıyı değiştirirken (başka bir ClickHouse sunucusunun IP adresini veya sözlükler tarafından kullanılan sunucuyu değiştirirken) bu komutu kullanmak gerekir.

Daha uygun (otomatik) önbellek yönetimi için bkz: disable\_internal\_dns\_cache, dns\_cache\_update\_period parametreleri.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

İşaret önbelleğini sıfırlar. ClickHouse ve performans testlerinin geliştirilmesinde kullanılır.

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query\_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

ClickHouse yapılandırmasını yeniden yükler. Yapılandırma ZooKeeeper saklandığında kullanılır.

## SHUTDOWN {#query_language-system-shutdown}

Normalde Clickhouse'u kapatır (gibi `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

ClickHouse işlemini iptal eder (gibi `kill -9 {$ pid_clickhouse-server}`)

## Dağıtılmış Tabloları Yönetme {#query-language-system-distributed}

ClickHouse yönetebilir [dağılı](../../engines/table-engines/special/distributed.md) Tablolar. Bir kullanıcı bu tablolara veri eklediğinde, ClickHouse önce küme düğümlerine gönderilmesi gereken verilerin bir sırası oluşturur, sonra zaman uyumsuz olarak gönderir. İle kuyruk işleme yönetebilirsiniz [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), ve [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) sorgular. Ayrıca, dağıtılmış verileri eşzamanlı olarak `insert_distributed_sync` ayar.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Dağıtılmış tablolara veri eklerken arka plan veri dağıtımını devre dışı bırakır.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Küme düğümlerine eşzamanlı olarak veri göndermek için Clickhouse'u zorlar. Herhangi bir düğüm kullanılamıyorsa, ClickHouse bir özel durum atar ve sorgu yürütülmesini durdurur. Tüm düğümler tekrar çevrimiçi olduğunda gerçekleşecek olan başarılı olana kadar sorguyu yeniden deneyebilirsiniz.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Dağıtılmış tablolara veri eklerken arka plan veri dağıtımını etkinleştirir.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

MergeTree ailesindeki tablolar için arka plan birleşmelerini durdurma imkanı sağlar:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "Not"
    `DETACH / ATTACH` tablo, daha önce tüm MergeTree tabloları için birleştirmeler durdurulduğunda bile tablo için arka plan birleştirmelerini başlatır.

### START MERGES {#query_language-system-start-merges}

MergeTree ailesindeki tablolar için arka plan birleştirmelerini başlatma imkanı sağlar:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
