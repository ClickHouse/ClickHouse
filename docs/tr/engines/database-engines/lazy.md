---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 31
toc_title: Tembel
---

# Tembel {#lazy}

Tabloları yalnızca RAM’de tutar `expiration_time_in_seconds` son erişimden saniyeler sonra. Sadece \* Log tabloları ile kullanılabilir.

Erişimler arasında uzun bir zaman aralığı olan birçok küçük \* günlük tablosunu saklamak için optimize edilmiştir.

## Veritabanı oluşturma {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[Orijinal makale](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
