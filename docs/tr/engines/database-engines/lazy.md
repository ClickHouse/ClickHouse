---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: Tembel
---

# Tembel {#lazy}

Tabloları yalnızca RAM'de tutar `expiration_time_in_seconds` son erişimden saniyeler sonra. Sadece \* Log tabloları ile kullanılabilir.

Erişimler arasında uzun bir zaman aralığı olan birçok küçük \* günlük tablosunu saklamak için optimize edilmiştir.

## Veritabanı oluşturma {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[Orijinal makale](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
