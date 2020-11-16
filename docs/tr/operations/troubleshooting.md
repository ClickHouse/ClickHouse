---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "Ar\u0131za"
---

# Arıza {#troubleshooting}

-   [Kurulum](#troubleshooting-installation-errors)
-   [Sunucuya bağlanma](#troubleshooting-accepts-no-connections)
-   [Sorgu işleme](#troubleshooting-does-not-process-queries)
-   [Sorgu işleme verimliliği](#troubleshooting-too-slow)

## Kurulum {#troubleshooting-installation-errors}

### Apt-get ile ClickHouse deposundan Deb paketleri alınamıyor {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   Güvenlik Duvarı ayarlarını kontrol edin.
-   Depoya herhangi bir nedenle erişemiyorsanız, paketleri aşağıda açıklandığı gibi indirin [Başlarken](../getting-started/index.md) makale ve bunları kullanarak manuel olarak yükleyin `sudo dpkg -i <packages>` komut. Ayrıca ihtiyacınız olacak `tzdata` paket.

## Sunucuya bağlanma {#troubleshooting-accepts-no-connections}

Olası sorunlar:

-   Sunucu çalışmıyor.
-   Beklenmeyen veya yanlış yapılandırma parametreleri.

### Sunucu Çalışmıyor {#server-is-not-running}

**Sunucu runnnig olup olmadığını kontrol edin**

Komut:

``` bash
$ sudo service clickhouse-server status
```

Sunucu çalışmıyorsa, komutla başlatın:

``` bash
$ sudo service clickhouse-server start
```

**Günlükleri kontrol et**

Ana günlüğü `clickhouse-server` içinde `/var/log/clickhouse-server/clickhouse-server.log` varsayılan olarak.

Sunucu başarıyla başlatıldıysa, dizeleri görmelisiniz:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

Eğer `clickhouse-server` Başlat bir yapılandırma hatası ile başarısız oldu, görmelisiniz `<Error>` bir hata açıklaması ile dize. Mesela:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Dosyanın sonunda bir hata görmüyorsanız, dizeden başlayarak tüm dosyaya bakın:

``` text
<Information> Application: starting up.
```

İkinci bir örneğini başlatmaya çalışırsanız `clickhouse-server` sunucuda, aşağıdaki günlük bakın:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**Bkz. sistem.d günlükleri**

Eğer herhangi bir yararlı bilgi bulamazsanız `clickhouse-server` günlükler veya herhangi bir günlük yok, görüntüleyebilirsiniz `system.d` komutu kullanarak günlükleri:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Clickhouse-Server'ı etkileşimli modda Başlat**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Bu komut, sunucuyu otomatik başlatma komut dosyasının standart parametreleriyle etkileşimli bir uygulama olarak başlatır. Bu modda `clickhouse-server` konsoldaki tüm olay iletilerini yazdırır.

### Yapılandırma Parametreleri {#configuration-parameters}

Kontrol:

-   Docker ayarları.

    Bir IPv6 ağında Docker'da ClickHouse çalıştırırsanız, `network=host` ayar .lanmıştır.

-   Bitiş noktası ayarları.

    Kontrol [listen_host](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) ve [tcp_port](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) ayarlar.

    ClickHouse server, yalnızca varsayılan olarak localhost bağlantılarını kabul eder.

-   HTTP protokolü ayarları.

    HTTP API protokol ayarlarını denetleyin.

-   Güvenli bağlantı ayarları.

    Kontrol:

    -   Bu [tcp_port_secure](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) ayar.
    -   İçin ayarlar [SSL sertifikaları](server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    Bağlanırken uygun parametreleri kullanın. Örneğin, kullanın `port_secure` parametre ile `clickhouse_client`.

-   Kullanıcı ayarları.

    Yanlış kullanıcı adı veya parola kullanıyor olabilirsiniz.

## Sorgu İşleme {#troubleshooting-does-not-process-queries}

ClickHouse sorguyu işlemek mümkün değilse, istemciye bir hata açıklaması gönderir. İn the `clickhouse-client` konsoldaki hatanın bir açıklamasını alırsınız. Http arabirimini kullanıyorsanız, ClickHouse yanıt gövdesinde hata açıklamasını gönderir. Mesela:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Eğer başlarsanız `clickhouse-client` ile... `stack-trace` parametre, ClickHouse bir hata açıklaması ile sunucu yığın izleme döndürür.

Bozuk bir bağlantı hakkında bir mesaj görebilirsiniz. Bu durumda, sorguyu tekrarlayabilirsiniz. Sorguyu her gerçekleştirdiğinizde bağlantı kesilirse, sunucu günlüklerini hatalar için denetleyin.

## Sorgu işleme verimliliği {#troubleshooting-too-slow}

Clickhouse'un çok yavaş çalıştığını görürseniz, sorgularınız için sunucu kaynakları ve ağdaki yükü profillemeniz gerekir.

Profil sorguları için clickhouse-benchmark yardımcı programını kullanabilirsiniz. Saniyede işlenen sorgu sayısını, saniyede işlenen satır sayısını ve sorgu işleme sürelerinin yüzdelerini gösterir.
