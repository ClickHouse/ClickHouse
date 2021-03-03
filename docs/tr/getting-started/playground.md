---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 14
toc_title: "Bah\xE7e"
---

# ClickHouse Oyun Alanı {#clickhouse-playground}

[ClickHouse Oyun Alanı](https://play.clickhouse.tech?file=welcome) kullanıcıların kendi sunucu veya küme kurmadan, anında sorguları çalıştırarak ClickHouse ile deneme sağlar.
Oyun alanında çeşitli örnek veri kümelerinin yanı sıra ClickHouse özelliklerini gösteren örnek sorgular da mevcuttur.

Sorgular salt okunur bir kullanıcı olarak yürütülür. Bazı sınırlamaları ima eder:

-   DDL sorgularına İzin Verilmiyor
-   Sorgu Ekle izin verilmez

Aşağıdaki ayarlar da uygulanır:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouse oyun alanı m2 deneyimini sunar.küçükler
[ClickHouse için yönetilen hizmet](https://cloud.yandex.com/services/managed-clickhouse)
örnek host hosteded in [Üye.Bulut](https://cloud.yandex.com/).
Hakkında daha fazla bilgi [bulut sağlayıcıları](../commercial/cloud.md).

ClickHouse Playground web arayüzü clickhouse üzerinden istekleri yapar [HTTP API](../interfaces/http.md).
Bahçesi arka uç herhangi bir ek sunucu tarafı uygulaması olmadan sadece bir ClickHouse kümesidir.
ClickHouse HTTPS bitiş noktası da oyun alanının bir parçası olarak kullanılabilir.

Herhangi bir HTTP istemcisi kullanarak oyun alanına sorgu yapabilirsiniz, örneğin [kıvrılma](https://curl.haxx.se) veya [wget](https://www.gnu.org/software/wget/), veya kullanarak bir bağlantı kurmak [JDBC](../interfaces/jdbc.md) veya [ODBC](../interfaces/odbc.md) sürücüler.
Clickhouse'u destekleyen yazılım ürünleri hakkında daha fazla bilgi mevcuttur [burada](../interfaces/index.md).

| Parametre | Değer                                   |
|:----------|:----------------------------------------|
| Nokta     | https://play-api.clickhouse.teknik:8443 |
| Kullanan  | `playground`                            |
| Şifre     | `clickhouse`                            |

Bu bitiş noktasının güvenli bir bağlantı gerektirdiğini unutmayın.

Örnek:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
