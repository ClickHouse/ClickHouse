---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "Sunucu Ayarlar\u0131"
---

# Sunucu Ayarları {#server-settings}

## buıltın_dıctıonarıes_reload_ınterval {#builtin-dictionaries-reload-interval}

Dahili sözlükleri yeniden yüklemeden önce saniye cinsinden Aralık.

ClickHouse, her x saniyede bir yerleşik sözlükleri yeniden yükler. Bu, sözlükleri düzenlemeyi mümkün kılar “on the fly” sunucuyu yeniden başlatmadan.

Varsayılan değer: 3600.

**Örnek**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## sıkıştırma {#server-settings-compression}

İçin veri sıkıştırma ayarları [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)- motor masaları.

!!! warning "Uyarıcı"
    Sadece ClickHouse kullanmaya başladıysanız kullanmayın.

Yapılandırma şablonu:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` alanlar:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` veya `zstd`.

Birden fazla yapılandırabilirsiniz `<case>` bölmeler.

Koşullar yerine getirildiğinde eylemler:

-   Bir veri parçası bir koşul kümesiyle eşleşirse, ClickHouse belirtilen sıkıştırma yöntemini kullanır.
-   Bir veri parçası birden çok koşul kümesiyle eşleşirse, ClickHouse ilk eşleşen koşul kümesini kullanır.

Bir veri bölümü için herhangi bir koşul karşılanmazsa, ClickHouse `lz4` sıkıştırma.

**Örnek**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default_database {#default-database}

Varsayılan veritabanı.

Veritabanlarının bir listesini almak için [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) sorgu.

**Örnek**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

Varsayılan ayarlar profili.

Ayarlar profilleri parametrede belirtilen dosyada bulunur `user_config`.

**Örnek**

``` xml
<default_profile>default</default_profile>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

Dış sözlükler için yapılandırma dosyasının yolu.

Yol:

-   Mutlak yolu veya sunucu yapılandırma dosyasına göre yolu belirtin.
-   Yol joker karakterler içerebilir \* ve ?.

Ayrıca bakınız “[Dış söz dictionarieslükler](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**Örnek**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

Sözlüklerin tembel yüklenmesi.

Eğer `true`, sonra her sözlük ilk kullanımda oluşturulur. Sözlük oluşturma başarısız olursa, sözlüğü kullanan işlev bir özel durum atar.

Eğer `false`, sunucu başladığında tüm sözlükler oluşturulur ve bir hata varsa, sunucu kapanır.

Varsayılan değer `true`.

**Örnek**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

Dizin için şemalar gibi giriş verileri için şemaları ile yolu [CapnProto](../../interfaces/formats.md#capnproto) biçimli.

**Örnek**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## grafit {#server_configuration_parameters-graphite}

Veri gönderme [Grafit](https://github.com/graphite-project).

Ayarlar:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root_path – Prefix for keys.
-   metrics – Sending data from the [sistem.metrik](../../operations/system-tables.md#system_tables-metrics) Tablo.
-   events – Sending deltas data accumulated for the time period from the [sistem.etkinlik](../../operations/system-tables.md#system_tables-events) Tablo.
-   events_cumulative – Sending cumulative data from the [sistem.etkinlik](../../operations/system-tables.md#system_tables-events) Tablo.
-   asynchronous_metrics – Sending data from the [sistem.asynchronous_metrics](../../operations/system-tables.md#system_tables-asynchronous_metrics) Tablo.

Birden fazla yapılandırabilirsiniz `<graphite>` yanlar. Örneğin, bunu farklı aralıklarla farklı veri göndermek için kullanabilirsiniz.

**Örnek**

``` xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

Grafit için inceltme verileri için ayarlar.

Daha fazla ayrıntı için bkz. [Graphıtemergetree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Örnek**

``` xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## http_port/https_port {#http-porthttps-port}

Http(ler) üzerinden sunucuya bağlanmak için bağlantı noktası.

Eğer `https_port` belirtilen, [openSSL](#server_configuration_parameters-openssl) yapılandırılmalıdır.

Eğer `http_port` belirtilmişse, OpenSSL yapılandırması ayarlanmış olsa bile göz ardı edilir.

**Örnek**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

ClickHouse HTTP (s) sunucusuna eriştiğinizde varsayılan olarak gösterilen sayfa.
Varsayılan değer “Ok.” (sonunda bir çizgi besleme ile)

**Örnek**

Açıyor `https://tabix.io/` eriş whenirken `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include_from {#server_configuration_parameters-include_from}

Değiştirmeleri ile dosyanın yolu.

Daha fazla bilgi için bölüme bakın “[Yapılandırma dosyaları](../configuration-files.md#configuration_files)”.

**Örnek**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## ınterserver_http_port {#interserver-http-port}

ClickHouse sunucuları arasında veri alışverişi için bağlantı noktası.

**Örnek**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## ınterserver_http_host {#interserver-http-host}

Bu sunucuya erişmek için diğer sunucular tarafından kullanılabilecek ana bilgisayar adı.

Eğer ihmal edilirse, aynı şekilde tanımlanır `hostname-f` komut.

Belirli bir ağ arayüzünden kopmak için kullanışlıdır.

**Örnek**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## ınterserver_http_credentials {#server-settings-interserver-http-credentials}

Sırasında kimlik doğrulaması için kullanılan kullanıcı adı ve şifre [çoğalma](../../engines/table-engines/mergetree-family/replication.md) çoğaltılan \* motorlarla. Bu kimlik bilgileri yalnızca yinelemeler arasındaki iletişim için kullanılır ve ClickHouse istemcileri için kimlik bilgileri ile ilgisizdir. Sunucu, yinelemeleri bağlamak için bu kimlik bilgilerini denetliyor ve diğer yinelemelere bağlanırken aynı kimlik bilgilerini kullanıyor. Bu nedenle, bu kimlik bilgileri kümedeki tüm yinelemeler için aynı şekilde ayarlanmalıdır.
Varsayılan olarak, kimlik doğrulama kullanılmaz.

Bu bölüm aşağıdaki parametreleri içerir:

-   `user` — username.
-   `password` — password.

**Örnek**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep_alive_timeout {#keep-alive-timeout}

ClickHouse bağlantıyı kapatmadan önce gelen istekleri bekler saniye sayısı. Varsayılan 3 saniye.

**Örnek**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

İsteklerin gelebileceği ana bilgisayarlarda kısıtlama. Sunucunun hepsini yanıtlamasını istiyorsanız, belirtin `::`.

Örnekler:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## kaydedici {#server_configuration_parameters-logger}

Günlük ayarları.

Anahtarlar:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`ve`errorlog`. Dosya ulaştıktan sonra `size`, ClickHouse arşivleri ve yeniden adlandırır ve onun yerine yeni bir günlük dosyası oluşturur.
-   count – The number of archived log files that ClickHouse stores.

**Örnek**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Syslog yazma da desteklenmektedir. Yapılandırma örneği:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Anahtarlar:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [Syslog tesisi anahtar sözcüğü](https://en.wikipedia.org/wiki/Syslog#Facility) ile büyük harf inlerle “LOG_” önek: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` vb.).
    Varsayılan değer: `LOG_USER` eğer `address` belirtilen, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` ve `syslog.`

## makrolar {#macros}

Çoğaltılmış tablolar için parametre değiştirmeleri.

Çoğaltılmış tablolar kullanılmazsa atlanabilir.

Daha fazla bilgi için bölüme bakın “[Çoğaltılmış tablolar oluşturma](../../engines/table-engines/mergetree-family/replication.md)”.

**Örnek**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

Tablo motorları tarafından kullanılan işaretlerin önbelleğinin yaklaşık boyutu (bayt cinsinden) [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) aile.

Önbellek sunucu için paylaşılır ve bellek gerektiği gibi ayrılır. Önbellek boyutu en az 5368709120 olmalıdır.

**Örnek**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_concurrent_queries {#max-concurrent-queries}

Aynı anda işlenen isteklerin maksimum sayısı.

**Örnek**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max_connections {#max-connections}

En fazla gelen bağlantı sayısı.

**Örnek**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

Maksimum açık dosya sayısı.

Varsayılan olarak: `maximum`.

Biz beri Mac OS X bu seçeneği kullanmanızı öneririz `getrlimit()` işlev yanlış bir değer döndürür.

**Örnek**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

Tabloları silme konusunda kısıtlama.

Eğer bir boyutu [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tablo aşıyor `max_table_size_to_drop` (bayt cinsinden), bir bırakma sorgusu kullanarak silemezsiniz.

ClickHouse sunucusunu yeniden başlatmadan tabloyu silmeniz gerekiyorsa, `<clickhouse-path>/flags/force_drop_table` dosya ve bırakma sorgusunu çalıştırın.

Varsayılan değer: 50 GB.

0 değeri, herhangi bir kısıtlama olmaksızın tüm tabloları silebileceğiniz anlamına gelir.

**Örnek**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge_tree {#server_configuration_parameters-merge_tree}

Tablolar için ince ayar [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Daha fazla bilgi için bkz: MergeTreeSettings.h başlık dosyası.

**Örnek**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSL istemci / sunucu yapılandırması.

SSL desteği tarafından sağlanmaktadır `libpoco` kitaplık. Arayüz dosyada açıklanmıştır [SSLManager.sa](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Sunucu/istemci ayarları için tuşlar:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` sertifika içerir.
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node's certificates. Details are in the description of the [Bağlam](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) sınıf. Olası değerler: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Kabul edilebilir değerler: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. Bu parametre her zaman sunucu oturumu önbelleğe alır ve istemci önbellekleme istedi, sorunları önlemek yardımcı olduğundan önerilir. Varsayılan değer: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**Ayarlar örneği:**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## part_log {#server_configuration_parameters-part-log}

İlişkili olayları günlüğe kaydetme [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Örneğin, veri ekleme veya birleştirme. Birleştirme algoritmalarını simüle etmek ve özelliklerini karşılaştırmak için günlüğü kullanabilirsiniz. Birleştirme işlemini görselleştirebilirsiniz.

Sorgular günlüğe kaydedilir [sistem.part_log](../../operations/system-tables.md#system_tables-part-log) tablo, ayrı bir dosyada değil. Bu tablonun adını aşağıdaki tabloda yapılandırabilirsiniz: `table` parametre (aşağıya bakınız).

Günlüğü yapılandırmak için aşağıdaki parametreleri kullanın:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [özel bölümleme anahtarı](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**Örnek**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## yol {#server_configuration_parameters-path}

Veri içeren dizinin yolu.

!!! note "Not"
    Sondaki eğik çizgi zorunludur.

**Örnek**

``` xml
<path>/var/lib/clickhouse/</path>
```

## prometheus {#server_configuration_parameters-prometheus}

Kazıma için metrik verilerini açığa çıkarma [Prometheus](https://prometheus.io).

Ayarlar:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [sistem.metrik](../system-tables.md#system_tables-metrics) Tablo.
-   `events` – Flag that sets to expose metrics from the [sistem.etkinlik](../system-tables.md#system_tables-events) Tablo.
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [sistem.asynchronous_metrics](../system-tables.md#system_tables-asynchronous_metrics) Tablo.

**Örnek**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query_log {#server_configuration_parameters-query-log}

İle alınan günlük sorgu settinglarının ayarlanması [log_queries = 1](../settings/settings.md) ayar.

Sorgular günlüğe kaydedilir [sistem.query_log](../../operations/system-tables.md#system_tables-query_log) tablo, ayrı bir dosyada değil. Tablonun adını değiştirebilirsiniz. `table` parametre (aşağıya bakınız).

Günlüğü yapılandırmak için aşağıdaki parametreleri kullanın:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [özel bölümleme anahtarı](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) bir masa için.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Tablo yoksa, ClickHouse bunu oluşturur. ClickHouse sunucusu güncelleştirildiğinde sorgu günlüğünün yapısı değiştiyse, eski yapıya sahip tablo yeniden adlandırılır ve otomatik olarak yeni bir tablo oluşturulur.

**Örnek**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query-thread-log}

İle alınan sorguların günlük iş parçacıklarının ayarlanması [log_query_threads = 1](../settings/settings.md#settings-log-query-threads) ayar.

Sorgular günlüğe kaydedilir [sistem.query_thread_log](../../operations/system-tables.md#system_tables-query-thread-log) tablo, ayrı bir dosyada değil. Tablonun adını değiştirebilirsiniz. `table` parametre (aşağıya bakınız).

Günlüğü yapılandırmak için aşağıdaki parametreleri kullanın:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [özel bölümleme anahtarı](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) bir sistem tablosu için.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Tablo yoksa, ClickHouse bunu oluşturur. Sorgu iş parçacığı günlüğü yapısını değiştirdiyseniz ClickHouse sunucu güncelleştirildi, tablo eski yapısı ile yeniden adlandırılır ve yeni bir tablo otomatik olarak oluşturulur.

**Örnek**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace_log {#server_configuration_parameters-trace_log}

İçin ayarlar [trace_log](../../operations/system-tables.md#system_tables-trace_log) sistem tablosu çalışması.

Parametre:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [Özel bölümleme anahtarı](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) bir sistem tablosu için.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

Varsayılan sunucu yapılandırma dosyası `config.xml` aşağıdaki ayarlar bölümünü içerir:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query_masking_rules {#query-masking-rules}

Regexp tabanlı kurallar, sorgulara ve tüm günlük iletilerine sunucu günlüklerinde depolamadan önce uygulanacak,
`system.query_log`, `system.text_log`, `system.processes` tablo ve istemciye gönderilen günlüklerde. Önlem allowseyi sağlayan
SQL sorgularından hassas veri sızıntısı (isimler, e-postalar, kişisel
kimlik veya kredi kartı numaraları) günlükleri için.

**Örnek**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Config alanları:
- `name` - kuralın adı (isteğe bağlı)
- `regexp` - Re2 uyumlu düzenli ifade (zorunlu)
- `replace` - hassas veriler için ikame dizesi (isteğe bağlı, varsayılan olarak-altı Yıldız İşareti)

Maskeleme kuralları tüm sorguya uygulanır (hatalı biçimlendirilmiş / ayrıştırılamayan sorgulardan hassas verilerin sızıntılarını önlemek için).

`system.events` tablo sayacı var `QueryMaskingRulesMatch` hangi sorgu maskeleme kuralları maçları genel bir numarası var.

Dağıtılmış sorgular için her sunucu ayrı ayrı yapılandırılmalıdır, aksi takdirde alt sorgular diğerine iletilir
düğümler maskeleme olmadan saklanır.

## remote_servers {#server-settings-remote-servers}

Tarafından kullanılan küm ofelerin yapılandırması [Dağılı](../../engines/table-engines/special/distributed.md) tablo motoru ve `cluster` tablo işlevi.

**Örnek**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

Değeri için `incl` öznitelik, bölümüne bakın “[Yapılandırma dosyaları](../configuration-files.md#configuration_files)”.

**Ayrıca Bakınız**

-   [skip_unavailable_shards](../settings/settings.md#settings-skip_unavailable_shards)

## saat dilimi {#server_configuration_parameters-timezone}

Sunucunun saat dilimi.

UTC saat dilimi veya coğrafi konum (örneğin, Afrika / Abidjan) için bir IANA tanımlayıcısı olarak belirtilir.

Saat dilimi, datetime alanları metin biçimine (ekranda veya dosyada yazdırıldığında) çıktığında ve datetime'ı bir dizeden alırken dize ve DateTime biçimleri arasındaki dönüşümler için gereklidir. Ayrıca, saat dilimi, giriş parametrelerinde saat dilimini almadıkları takdirde saat ve tarih ile çalışan işlevlerde kullanılır.

**Örnek**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

TCP protokolü üzerinden istemcilerle iletişim kurmak için bağlantı noktası.

**Örnek**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

İstemcilerle güvenli iletişim için TCP bağlantı noktası. İle kullanın [OpenSSL](#server_configuration_parameters-openssl) ayarlar.

**Olası değerler**

Pozitif tamsayı.

**Varsayılan değer**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

MySQL protokolü üzerinden istemcilerle iletişim kurmak için bağlantı noktası.

**Olası değerler**

Pozitif tamsayı.

Örnek

``` xml
<mysql_port>9004</mysql_port>
```

## tmp_path {#server-settings-tmp_path}

Büyük sorguları işlemek için geçici veri yolu.

!!! note "Not"
    Sondaki eğik çizgi zorunludur.

**Örnek**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#server-settings-tmp-policy}

Politika dan [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) geçici dosyaları saklamak için.
Set değilse [`tmp_path`](#server-settings-tmp_path) kullanılır, aksi takdirde göz ardı edilir.

!!! note "Not"
    - `move_factor` göz ardı edilir
- `keep_free_space_bytes` göz ardı edilir
- `max_data_part_size_bytes` göz ardı edilir
- bu Politikada tam olarak bir cilt olmalı

## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

Tablo motorları tarafından kullanılan sıkıştırılmamış veriler için önbellek boyutu (bayt cinsinden) [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Sunucu için bir paylaşılan önbellek var. Bellek talep üzerine tahsis edilir. Seçenek varsa önbellek kullanılır [use_uncompressed_cache](../settings/settings.md#setting-use_uncompressed_cache) etkindir.

Sıkıştırılmamış önbellek, tek tek durumlarda çok kısa sorgular için avantajlıdır.

**Örnek**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

Kullanıcı dosyaları ile dizin. Tablo işlevinde kullanılır [Dosya()](../../sql-reference/table-functions/file.md).

**Örnek**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users_config {#users-config}

İçeren dosyanın yolu:

-   Kullanıcı yapılandırmaları.
-   Erişim hakları.
-   Ayarlar profilleri.
-   Kota ayarları.

**Örnek**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

ClickHouse ile etkileşim sağlayan ayarları içerir [ZooKeeper](http://zookeeper.apache.org/) küme.

ClickHouse, çoğaltılmış tabloları kullanırken kopyaların meta verilerini depolamak için ZooKeeper kullanır. Çoğaltılmış tablolar kullanılmazsa, parametrelerin bu bölümü atlanabilir.

Bu bölüm aşağıdaki parametreleri içerir:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    Mesela:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) bu, ClickHouse sunucusu tarafından kullanılan znodes için kök olarak kullanılır. İsteğe bağlı.
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**Örnek yapılandırma**

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
</zookeeper>
```

**Ayrıca Bakınız**

-   [Çoğalma](../../engines/table-engines/mergetree-family/replication.md)
-   [ZooKeeper programcı Kılavuzu](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

ZooKeeper veri parçası başlıkları için depolama yöntemi.

Bu ayar yalnızca `MergeTree` aile. Belirt specifiedilebilir:

-   Küresel olarak [merge_tree](#server_configuration_parameters-merge_tree) bu bölüm `config.xml` Dosya.

    ClickHouse sunucudaki tüm tablolar için ayarı kullanır. Ayarı istediğiniz zaman değiştirebilirsiniz. Mevcut tablolar, ayar değiştiğinde davranışlarını değiştirir.

-   Her tablo için.

    Bir tablo oluştururken, karşılık gelen [motor ayarı](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). Genel ayar değişse bile, bu ayara sahip varolan bir tablonun davranışı değişmez.

**Olası değerler**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

Eğer `use_minimalistic_part_header_in_zookeeper = 1`, sonraları [çoğaltıyordu](../../engines/table-engines/mergetree-family/replication.md) tablolar, veri parçalarının başlıklarını tek bir `znode`. Tablo çok sayıda sütun içeriyorsa, bu depolama yöntemi Zookeeper'da depolanan verilerin hacmini önemli ölçüde azaltır.

!!! attention "Dikkat"
    Uyguladıktan sonra `use_minimalistic_part_header_in_zookeeper = 1`, ClickHouse sunucusunu bu ayarı desteklemeyen bir sürüme düşüremezsiniz. Bir kümedeki sunucularda ClickHouse yükseltirken dikkatli olun. Tüm sunucuları bir kerede yükseltmeyin. Clickhouse'un yeni sürümlerini bir test ortamında veya bir kümenin yalnızca birkaç sunucusunda test etmek daha güvenlidir.

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**Varsayılan değer:** 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

İç DNS önbelleğini devre dışı bırakır. Sistemlerinde ClickHouse işletim için tavsiye
Kubernetes gibi sık sık değişen altyapı ile.

**Varsayılan değer:** 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

ClickHouse iç DNS önbelleğinde saklanan IP adreslerini güncelleme süresi (saniye cinsinden).
Güncelleştirme, ayrı bir sistem iş parçacığında zaman uyumsuz olarak gerçekleştirilir.

**Varsayılan değer**: 15.

## access_control_path {#access_control_path}

ClickHouse sunucusunun SQL komutları tarafından oluşturulan kullanıcı ve rol yapılandırmalarını depoladığı bir klasörün yolu.

Varsayılan değer: `/var/lib/clickhouse/access/`.

**Ayrıca bakınız**

-   [Erişim Kontrolü ve hesap yönetimi](../access-rights.md#access-control)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
