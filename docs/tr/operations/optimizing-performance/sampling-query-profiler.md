---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Sorgu Profili Olu\u015Fturma"
---

# Örnekleme Sorgusu Profiler {#sampling-query-profiler}

ClickHouse, sorgu yürütülmesini analiz etmeyi sağlayan örnekleme profiler'i çalıştırır. Profiler kullanarak sorgu yürütme sırasında en sık kullanılan kaynak kodu yordamları bulabilirsiniz. Boşta kalma süresi de dahil olmak üzere harcanan CPU zamanını ve duvar saati zamanını izleyebilirsiniz.

Profiler kullanmak için:

-   Kurulum [trace\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) sunucu yapılandırması bölümü.

    Bu bölüm yapılandırır [trace\_log](../../operations/system-tables.md#system_tables-trace_log) profiler işleyişinin sonuçlarını içeren sistem tablosu. Varsayılan olarak yapılandırılmıştır. Bu tablodaki verilerin yalnızca çalışan bir sunucu için geçerli olduğunu unutmayın. Sunucu yeniden başlatıldıktan sonra ClickHouse tabloyu temizlemez ve depolanan tüm sanal bellek adresi geçersiz hale gelebilir.

-   Kurulum [query\_profiler\_cpu\_time\_period\_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) veya [query\_profiler\_real\_time\_period\_ns](../settings/settings.md#query_profiler_real_time_period_ns) ayarlar. Her iki ayar da aynı anda kullanılabilir.

    Bu ayarlar, profiler zamanlayıcılarını yapılandırmanıza izin verir. Bunlar oturum ayarları olduğundan, tüm sunucu, bireysel kullanıcılar veya kullanıcı profilleri, etkileşimli oturumunuz ve her bir sorgu için farklı örnekleme sıklığı elde edebilirsiniz.

Varsayılan örnekleme frekansı saniyede bir örnektir ve hem CPU hem de gerçek zamanlayıcılar etkindir. Bu frekans, ClickHouse kümesi hakkında yeterli bilgi toplamaya izin verir. Aynı zamanda, bu sıklıkla çalışan profiler, ClickHouse sunucusunun performansını etkilemez. Her bir sorguyu profillemeniz gerekiyorsa, daha yüksek örnekleme frekansı kullanmayı deneyin.

Analiz etmek `trace_log` sistem tablosu:

-   Yüklemek `clickhouse-common-static-dbg` paket. Görmek [DEB paketlerinden yükleyin](../../getting-started/install.md#install-from-deb-packages).

-   Tarafından iç gözlem işlevlerine izin ver [allow\_introspection\_functions](../settings/settings.md#settings-allow_introspection_functions) ayar.

    Güvenlik nedenleriyle, iç gözlem işlevleri varsayılan olarak devre dışı bırakılır.

-   Kullan... `addressToLine`, `addressToSymbol` ve `demangle` [iç gözlem fonksiyonları](../../sql-reference/functions/introspection.md) ClickHouse kodu işlev adları ve konumlarını almak için. Bazı sorgu için bir profil almak için, `trace_log` Tablo. Bireysel fonksiyonları bütün yığın izleri ya da veri toplama yapabilirsiniz.

Görselleştirmeniz gerekiyorsa `trace_log` bilgi, deneyin [flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) ve [speedscope](https://github.com/laplab/clickhouse-speedscope).

## Örnek {#example}

Bu örnekte biz:

-   Filtre `trace_log` bir sorgu tanımlayıcısı ve geçerli tarihe göre veri.

-   Yığın izleme ile toplama.

-   İç gözlem işlevlerini kullanarak, bir rapor alacağız:

    -   Sembollerin isimleri ve karşılık gelen kaynak kodu işlevleri.
    -   Bu işlevlerin kaynak kodu konumları.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
