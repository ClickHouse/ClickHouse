---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 76
toc_title: "G\xFCvenlik Changelog"
---

## ClickHouse sürümünde düzeltildi 19.14.3.3, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

Аn attacker that has write access to ZooKeeper and who ican run a custom server available from the network where ClickHouse runs, can create a custom-built malicious server that will act as a ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from the malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem.

Kredi: Yandex Bilgi Güvenliği ekibinden Eldar Zaitov

### CVE-2019-16535 {#cve-2019-16535}

Аn OOB read, OOB write and integer underflow in decompression algorithms can be used to achieve RCE or DoS via native protocol.

Kredi: Yandex Bilgi Güvenliği ekibinden Eldar Zaitov

### CVE-2019-16536 {#cve-2019-16536}

DOS'A giden yığın taşması, kötü amaçlı kimliği doğrulanmış bir istemci tarafından tetiklenebilir.

Kredi: Yandex Bilgi Güvenliği ekibinden Eldar Zaitov

## ClickHouse sürümü 19.13.6.1, 2019-09-20'de düzeltildi {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

Tablo fonksiyonu `url` güvenlik açığı saldırganın istekte rasgele HTTP üstbilgileri enjekte etmesine izin vermişti.

Krediler: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## ClickHouse sürümünde sabit 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

Catboost modellerini yüklemek için işlevler, yol geçişine izin verdi ve hata mesajları aracılığıyla keyfi dosyaları okudu.

Kredi: Yandex Bilgi Güvenliği ekibinden Andrey Krasichkov

## ClickHouse sürüm 18.10.3, 2018-08-13 sabit {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC, dosya sisteminden rasgele paylaşılan nesnelerin yüklenmesine izin verdi ve bu da uzaktan kod yürütme güvenlik açığına yol açtı.

Kredi: Yandex Bilgi Güvenliği ekibinden Andrey Krasichkov ve Evgeny Sidorov

## ClickHouse sürüm 1.1.54388, 2018-06-28 sabit {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

“remote” tablo fonksiyonu izin keyfi semboller “user”, “password” ve “default_database” çapraz Protokol isteği sahtecilik saldırılarına yol açan alanlar.

Kredi: Yandex Bilgi Güvenliği ekibinden Andrey Krasichkov

## ClickHouse sürüm 1.1.54390, 2018-07-06 sabit {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

ClickHouse MySQL istemcisi vardı “LOAD DATA LOCAL INFILE” işlevsellik, kötü niyetli bir MySQL veritabanının bağlı ClickHouse sunucusundan rasgele dosyaları okumasına izin verdi.

Kredi: Yandex Bilgi Güvenliği ekibinden Andrey Krasichkov ve Evgeny Sidorov

## ClickHouse sürüm 1.1.54131, 2017-01-10 sabit {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

Deb paketindeki yanlış yapılandırma, veritabanının yetkisiz kullanımına neden olabilir.

Kredi: İngiltere'nin Ulusal siber güvenlik merkezi (NCSC)

{## [Orijinal makale](https://clickhouse.tech/docs/en/security_changelog/) ##}
