---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u0130zleme"
---

# İzleme {#monitoring}

İzleyebilirsiniz:

-   Donanım kaynaklarının kullanımı.
-   ClickHouse sunucu metrikleri.

## Kaynak Kullanımı {#resource-utilization}

ClickHouse, donanım kaynaklarının durumunu tek başına izlemez.

İzleme ayarlamak için önerilir :

-   İşlemcilerde yük ve sıcaklık.

    Kullanabilirsiniz [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) ya da diğer aletler.

-   Depolama sistemi, RAM ve ağ kullanımı.

## ClickHouse Sunucu Metrikleri {#clickhouse-server-metrics}

ClickHouse sunucu kendini devlet izleme için araçlar gömülü vardır.

Sunucu olaylarını izlemek için sunucu günlüklerini kullanın. Görmek [kaydedici](server-configuration-parameters/settings.md#server_configuration_parameters-logger) yapılandırma dosyasının bölümü.

ClickHouse toplar:

-   Sunucunun hesaplama kaynaklarını nasıl kullandığına dair farklı metrikler.
-   Sorgu işleme ile ilgili ortak istatistikler.

Metrikleri şu adreste bulabilirsiniz: [sistem.metrik](../operations/system-tables.md#system_tables-metrics), [sistem.etkinlik](../operations/system-tables.md#system_tables-events), ve [sistem.asynchronous\_metrics](../operations/system-tables.md#system_tables-asynchronous_metrics) Tablolar.

Clickhouse'u metrikleri dışa aktaracak şekilde yapılandırabilirsiniz [Grafit](https://github.com/graphite-project). Görmek [Graf sectionit bölümü](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) ClickHouse sunucu yapılandırma dosyasında. Metriklerin dışa aktarımını yapılandırmadan önce, grafit'i resmi olarak takip ederek ayarlamanız gerekir [kılavuz](https://graphite.readthedocs.io/en/latest/install.html).

Clickhouse'u metrikleri dışa aktaracak şekilde yapılandırabilirsiniz [Prometheus](https://prometheus.io). Görmek [Prometheus bölümü](server-configuration-parameters/settings.md#server_configuration_parameters-prometheus) ClickHouse sunucu yapılandırma dosyasında. Metriklerin dışa aktarılmasını yapılandırmadan önce, prometheus'u yetkililerini takip ederek ayarlamanız gerekir [kılavuz](https://prometheus.io/docs/prometheus/latest/installation/).

Ayrıca, http API aracılığıyla sunucu kullanılabilirliğini izleyebilirsiniz. Sen sendd the `HTTP GET` istek için `/ping`. Sunucu mevcutsa, yanıt verir `200 OK`.

Bir küme yapılandırmasındaki sunucuları izlemek için [max\_replica\_delay\_for\_distributed\_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) parametre ve HTTP kaynağını kullanın `/replicas_status`. Bir istek için `/replicas_status` dönüşler `200 OK` çoğaltma kullanılabilir ve diğer yinelemeler gecikmiş değil. Bir çoğaltma gecikirse, döndürür `503 HTTP_SERVICE_UNAVAILABLE` boşluk hakkında bilgi ile.
