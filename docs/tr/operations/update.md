---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 47
toc_title: "ClickHouse G\xFCncelleme"
---

# ClickHouse Güncelleme {#clickhouse-update}

ClickHouse DEB paketlerinden yüklüyse, sunucuda aşağıdaki komutları çalıştırın:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Önerilen deb paketleri dışında bir şey kullanarak ClickHouse yüklediyseniz, uygun güncelleştirme yöntemini kullanın.

ClickHouse dağıtılmış bir güncelleştirmeyi desteklemiyor. İşlem, her ayrı sunucuda ardışık olarak gerçekleştirilmelidir. Bir kümedeki tüm sunucuları aynı anda güncelleştirmeyin veya küme Bir süre kullanılamaz.
