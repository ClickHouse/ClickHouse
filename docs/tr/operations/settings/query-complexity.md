---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "Sorgu karma\u015F\u0131kl\u0131\u011F\u0131 \xFCzerindeki k\u0131s\u0131\
  tlamalar"
---

# Sorgu karmaşıklığı üzerindeki kısıtlamalar {#restrictions-on-query-complexity}

Sorgu karmaşıklığı üzerindeki kısıtlamalar ayarların bir parçasıdır.
Kullanıcı arabiriminden daha güvenli yürütme sağlamak için kullanılırlar.
Hemen hemen tüm kısıtlamalar sadece aşağıdakiler için geçerlidir `SELECT`. Dağıtılmış sorgu işleme için kısıtlamalar her sunucuda ayrı ayrı uygulanır.

ClickHouse, her satır için değil, veri bölümleri için kısıtlamaları denetler. Bu, veri parçasının boyutu ile kısıtlama değerini aşabileceğiniz anlamına gelir.

Üzerindeki kısıtlamalar “maximum amount of something” 0 değerini alabilir, yani “unrestricted”.
Çoğu kısıtlama da bir ‘overflow_mode’ ayar, sınır aşıldığında ne yapılması gerektiği anlamına gelir.
İki değerden birini alabilir: `throw` veya `break`. Toplama (group_by_overflow_mode) üzerindeki kısıtlamalar da değere sahiptir `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don't add new keys to the set.

## max_memory_usage {#settings_max_memory_usage}

Tek bir sunucuda bir sorgu çalıştırmak için kullanılacak en fazla RAM miktarı.

Varsayılan yapılandırma dosyasında maksimum 10 GB'DİR.

Bu ayar, kullanılabilir belleğin hacmini veya makinedeki toplam bellek hacmini dikkate almaz.
Kısıtlama, tek bir sunucu içindeki tek bir sorgu için geçerlidir.
Kullanabilirsiniz `SHOW PROCESSLIST` her sorgu için geçerli bellek tüketimini görmek için.
Ayrıca, en yüksek bellek tüketimi her sorgu için izlenir ve günlüğe yazılır.

Bellek kullanımı, belirli toplama işlevlerinin durumları için izlenmez.

Toplam işlevlerin durumları için bellek kullanımı tam olarak izlenmiyor `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` itibaren `String` ve `Array` değişkenler.

Bellek tüketimi de parametrelerle sınırlıdır `max_memory_usage_for_user` ve `max_memory_usage_for_all_queries`.

## max_memory_usage_for_user {#max-memory-usage-for-user}

Tek bir sunucuda bir kullanıcının sorguları çalıştırmak için kullanılacak en fazla RAM miktarı.

Varsayılan değerler [Ayarlar.sa](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288). Varsayılan olarak, tutar sınırlı değildir (`max_memory_usage_for_user = 0`).

Ayrıca açıklamasına bakın [max_memory_usage](#settings_max_memory_usage).

## max_memory_usage_for_all_queries {#max-memory-usage-for-all-queries}

Tek bir sunucuda tüm sorguları çalıştırmak için kullanılacak en fazla RAM miktarı.

Varsayılan değerler [Ayarlar.sa](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L289). Varsayılan olarak, tutar sınırlı değildir (`max_memory_usage_for_all_queries = 0`).

Ayrıca açıklamasına bakın [max_memory_usage](#settings_max_memory_usage).

## max_rows_to_read {#max-rows-to-read}

Aşağıdaki kısıtlamalar her blokta kontrol edilebilir (her satır yerine). Yani, kısıtlamalar biraz kırılabilir.
Birden çok iş parçacığında bir sorgu çalıştırırken, aşağıdaki kısıtlamalar her iş parçacığı için ayrı ayrı uygulanır.

Bir sorgu çalıştırırken bir tablodan okunabilen satır sayısı.

## max_bytes_to_read {#max-bytes-to-read}

Bir sorgu çalıştırırken bir tablodan okunabilen bayt sayısı (sıkıştırılmamış veri).

## read_overflow_mode {#read-overflow-mode}

Okunan veri hacmi sınırlardan birini aştığında ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## max_rows_to_group_by {#settings-max-rows-to-group-by}

Toplama alınan benzersiz anahtarların maksimum sayısı. Bu ayar, toplama sırasında bellek tüketimini sınırlamanızı sağlar.

## group_by_overflow_mode {#group-by-overflow-mode}

Toplama için benzersiz anahtarların sayısı sınırı aştığında ne yapmalı: ‘throw’, ‘break’, veya ‘any’. Varsayılan olarak, atın.
Kullanarak ‘any’ değer, GROUP BY'NİN bir yaklaşımını çalıştırmanızı sağlar. Bu yaklaşımın kalitesi, verilerin istatistiksel niteliğine bağlıdır.

## max_bytes_before_external_group_by {#settings-max_bytes_before_external_group_by}

Çalıştırmayı etkinleştirir veya devre dışı bırakır `GROUP BY` harici bellekte yan tümceleri. Görmek [Harici bellekte grupla](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory).

Olası değerler:

-   Tek tarafından kullanılabilecek maksimum RAM hacmi (bayt cinsinden) [GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause) operasyon.
-   0 — `GROUP BY` harici bellekte devre dışı.

Varsayılan değer: 0.

## max_rows_to_sort {#max-rows-to-sort}

Sıralamadan önce en fazla satır sayısı. Bu, sıralama yaparken bellek tüketimini sınırlamanıza izin verir.

## max_bytes_to_sort {#max-bytes-to-sort}

Sıralamadan önce en fazla bayt sayısı.

## sort_overflow_mode {#sort-overflow-mode}

Sıralamadan önce alınan satır sayısı sınırlardan birini aşarsa ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## max_result_rows {#setting-max_result_rows}

Sonuçtaki satır sayısını sınırlayın. Ayrıca, dağıtılmış bir sorgunun parçalarını çalıştırırken alt sorgular ve uzak sunucularda da kontrol edildi.

## max_result_bytes {#max-result-bytes}

Sonuçtaki bayt sayısını sınırlayın. Önceki ayar ile aynı.

## result_overflow_mode {#result-overflow-mode}

Sonucun hacmi sınırlardan birini aşarsa ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

Kullanım ‘break’ LİMİT kullanmaya benzer. `Break` yürütmeyi yalnızca blok düzeyinde keser. Bu, döndürülen satırların miktarının daha büyük olduğu anlamına gelir [max_result_rows](#setting-max_result_rows) birden çok [max_block_size](settings.md#setting-max_block_size) ve bağlıdır [max_threads](settings.md#settings-max_threads).

Örnek:

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

Sonuç:

``` text
6666 rows in set. ...
```

## max_execution_time {#max-execution-time}

Saniye cinsinden maksimum sorgu yürütme süresi.
Şu anda, sıralama aşamalarından biri için veya toplama işlevlerini birleştirirken ve sonlandırırken kontrol edilmez.

## timeout_overflow_mode {#timeout-overflow-mode}

Sorgu daha uzun çalıştırılırsa ne yapmalı ‘max_execution_time’: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## min_execution_speed {#min-execution-speed}

Saniyede satırlarda minimum yürütme hızı. Her veri bloğunda ne zaman kontrol edildi ‘timeout_before_checking_execution_speed’ doluyor. Yürütme hızı düşükse, bir istisna atılır.

## min_execution_speed_bytes {#min-execution-speed-bytes}

Saniyede en az yürütme bayt sayısı. Her veri bloğunda ne zaman kontrol edildi ‘timeout_before_checking_execution_speed’ doluyor. Yürütme hızı düşükse, bir istisna atılır.

## max_execution_speed {#max-execution-speed}

Saniyede en fazla yürütme satırı sayısı. Her veri bloğunda ne zaman kontrol edildi ‘timeout_before_checking_execution_speed’ doluyor. Yürütme hızı yüksekse, yürütme hızı azaltılır.

## max_execution_speed_bytes {#max-execution-speed-bytes}

Saniyede en fazla yürütme bayt sayısı. Her veri bloğunda ne zaman kontrol edildi ‘timeout_before_checking_execution_speed’ doluyor. Yürütme hızı yüksekse, yürütme hızı azaltılır.

## timeout_before_checking_execution_speed {#timeout-before-checking-execution-speed}

Yürütme hızının çok yavaş olmadığını kontrol eder (en az ‘min_execution_speed’), saniye içinde belirtilen süre dolduktan sonra.

## max_columns_to_read {#max-columns-to-read}

Tek bir sorguda bir tablodan okunabilen sütun sayısı. Bir sorgu daha fazla sayıda sütun okuma gerektiriyorsa, bir özel durum atar.

## max_temporary_columns {#max-temporary-columns}

Sabit sütunlar da dahil olmak üzere bir sorgu çalıştırırken aynı anda RAM'de tutulması gereken geçici sütun sayısı. Bundan daha fazla geçici sütun varsa, bir istisna atar.

## max_temporary_non_const_columns {#max-temporary-non-const-columns}

Aynı şey ‘max_temporary_columns’, ancak sabit sütunları saymadan.
Bir sorgu çalıştırırken sabit sütunların oldukça sık oluşturulduğunu, ancak yaklaşık sıfır bilgi işlem kaynağı gerektirdiğini unutmayın.

## max_subquery_depth {#max-subquery-depth}

Alt sorguların maksimum yuvalama derinliği. Alt sorgular daha derinse, bir istisna atılır. Varsayılan olarak, 100.

## max_pipeline_depth {#max-pipeline-depth}

Maksimum boru hattı derinliği. Sorgu işleme sırasında her veri bloğunun geçtiği dönüşümlerin sayısına karşılık gelir. Tek bir sunucunun sınırları içinde sayılır. Boru hattı derinliği büyükse, bir istisna atılır. Varsayılan olarak, 1000.

## max_ast_depth {#max-ast-depth}

Sorgu sözdizimsel ağacının en fazla yuvalama derinliği. Aşılırsa, bir istisna atılır.
Şu anda, ayrıştırma sırasında değil, yalnızca sorguyu ayrıştırdıktan sonra kontrol edilir. Yani, ayrıştırma sırasında çok derin bir sözdizimsel ağaç oluşturulabilir, ancak sorgu başarısız olur. Varsayılan olarak, 1000.

## max_ast_elements {#max-ast-elements}

Sorgu sözdizimsel ağacındaki en fazla öğe sayısı. Aşılırsa, bir istisna atılır.
Önceki ayarla aynı şekilde, yalnızca sorguyu ayrıştırdıktan sonra kontrol edilir. Varsayılan olarak, 50.000.

## max_rows_in_set {#max-rows-in-set}

Bir alt sorgudan oluşturulan In yan tümcesinde bir veri kümesi için satır sayısı.

## max_bytes_in_set {#max-bytes-in-set}

Bir alt sorgudan oluşturulan In yan tümcesinde bir set tarafından kullanılan en fazla bayt sayısı (sıkıştırılmamış veri).

## set_overflow_mode {#set-overflow-mode}

Veri miktarı sınırlardan birini aştığında ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## max_rows_ın_distinct {#max-rows-in-distinct}

DISTINCT kullanırken en fazla sayıda farklı satır.

## max_bytes_ın_distinct {#max-bytes-in-distinct}

DISTINCT kullanırken bir karma tablo tarafından kullanılan bayt sayısı.

## distinct_overflow_mode {#distinct-overflow-mode}

Veri miktarı sınırlardan birini aştığında ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## max_rows_to_transfer {#max-rows-to-transfer}

Uzak bir sunucuya geçirilen veya GLOBAL In kullanırken geçici bir tabloya kaydedilen satır sayısı.

## max_bytes_to_transfer {#max-bytes-to-transfer}

Uzak bir sunucuya geçirilen veya GLOBAL In kullanırken geçici bir tabloya kaydedilen bayt sayısı (sıkıştırılmamış veri).

## transfer_overflow_mode {#transfer-overflow-mode}

Veri miktarı sınırlardan birini aştığında ne yapmalı: ‘throw’ veya ‘break’. Varsayılan olarak, atın.

## max_rows_in_join {#settings-max_rows_in_join}

Tabloları birleştirirken kullanılan karma tablodaki satır sayısını sınırlar.

Bu ayarlar aşağıdakiler için geçerlidir [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) işlemleri ve [Katmak](../../engines/table-engines/special/join.md) masa motoru.

Bir sorgu birden çok birleşim içeriyorsa, ClickHouse her Ara sonuç için bu ayarı denetler.

Limit ulaşıldığında ClickHouse farklı eylemlerle devam edebilirsiniz. Kullan... [join_overflow_mode](#settings-join_overflow_mode) eylemi seçmek için ayarlama.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Unlimited number of rows.

Varsayılan değer: 0.

## max_bytes_in_join {#settings-max_bytes_in_join}

Tabloları birleştirirken kullanılan karma tablonun bayt cinsinden boyutunu sınırlar.

Bu ayarlar aşağıdakiler için geçerlidir [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) işlemleri ve [Jo tablein table engine](../../engines/table-engines/special/join.md).

Sorgu birleşimler içeriyorsa, ClickHouse her Ara sonuç için bu ayarı denetler.

Limit ulaşıldığında ClickHouse farklı eylemlerle devam edebilirsiniz. Kullanmak [join_overflow_mode](#settings-join_overflow_mode) eylemi seçmek için ayarlar.

Olası değerler:

-   Pozitif tamsayı.
-   0 — Memory control is disabled.

Varsayılan değer: 0.

## join_overflow_mode {#settings-join_overflow_mode}

Tanımlar katılın aşağıdaki sınırlar her zaman eylem ClickHouse gerçekleştirdiği ulaştı:

-   [max_bytes_in_join](#settings-max_bytes_in_join)
-   [max_rows_in_join](#settings-max_rows_in_join)

Olası değerler:

-   `THROW` — ClickHouse throws an exception and breaks operation.
-   `BREAK` — ClickHouse breaks operation and doesn't throw an exception.

Varsayılan değer: `THROW`.

**Ayrıca Bakınız**

-   [Jo](../../sql-reference/statements/select/join.md#select-join)
-   [Jo tablein table engine](../../engines/table-engines/special/join.md)

## max_partitions_per_ınsert_block {#max-partitions-per-insert-block}

Eklenen tek bir bloktaki en fazla bölüm sayısını sınırlar.

-   Pozitif tamsayı.
-   0 — Unlimited number of partitions.

Varsayılan değer: 100.

**Ayrıntı**

Veri eklerken, ClickHouse eklenen bloktaki bölüm sayısını hesaplar. Bölüm sayısı fazla ise `max_partitions_per_insert_block`, ClickHouse aşağıdaki metinle bir özel durum atar:

> “Too many partitions for single INSERT block (more than” + toString (max_parts) + “). The limit is controlled by ‘max_partitions_per_insert_block’ setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).”

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->
