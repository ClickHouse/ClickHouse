---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 29
toc_title: Vekiller
---

# Üçüncü Taraf geliştiricilerin Proxy sunucuları {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy), ClickHouse veritabanı için bir HTTP proxy ve yük dengeleyici.

Özellikler:

-   Kullanıcı başına Yönlendirme ve yanıt önbelleğe alma.
-   Esnek sınırlar.
-   Otomatik SSL sertifikası yenileme.

Go uygulanan.

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) ClickHouse ve uygulama sunucusu arasında yerel bir proxy olacak şekilde tasarlanmıştır.

Özellikler:

-   Bellek içi ve diskteki veri arabelleği.
-   Tablo başına yönlendirme.
-   Yük dengeleme ve sağlık kontrolü.

Go uygulanan.

## ClickHouse-Toplu {#clickhouse-bulk}

[ClickHouse-Toplu](https://github.com/nikepan/clickhouse-bulk) basit bir ClickHouse ekleme toplayıcı.

Özellikler:

-   Grup istekleri ve eşik veya aralık ile gönderin.
-   Birden çok uzak sunucu.
-   Temel kimlik doğrulama.

Go uygulanan.

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/third-party/proxy/) <!--hide-->
