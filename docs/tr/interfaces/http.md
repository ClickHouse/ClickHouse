---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 19
toc_title: "HTTP aray\xFCz\xFC"
---

# HTTP arayüzü {#http-interface}

HTTP arayüzü, herhangi bir programlama dilinden herhangi bir platformda Clickhouse'u kullanmanızı sağlar. Java ve Perl'den ve kabuk komut dosyalarından çalışmak için kullanıyoruz. Diğer bölümlerde, HTTP arayüzü Perl, Python ve Go'dan kullanılır. HTTP arabirimi yerel arabirimden daha sınırlıdır, ancak daha iyi uyumluluğa sahiptir.

Varsayılan olarak, clickhouse-server, 8123 numaralı bağlantı noktasında HTTP dinler (bu, yapılandırmada değiştirilebilir).

Parametreler olmadan bir GET / request yaparsanız, 200 yanıt kodunu ve tanımlanan dizeyi döndürür [http\_server\_default\_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) varsayılan değer “Ok.” (sonunda bir çizgi besleme ile)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

Sağlık kontrol komut GET / ping isteği kullanın. Bu işleyici her zaman döner “Ok.” (sonunda bir çizgi besleme ile). 18.12.13 sürümünden edinilebilir.

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

İsteği URL olarak gönder ‘query’ parametre veya bir POST olarak. Veya sorgunun başlangıcını gönder ‘query’ parametre ve postadaki geri kalanı (bunun neden gerekli olduğunu daha sonra açıklayacağız). URL'nin boyutu 16 KB ile sınırlıdır, bu nedenle büyük sorgular gönderirken bunu aklınızda bulundurun.

Başarılı olursa, 200 yanıt Kodu ve yanıt gövdesinde sonucu alırsınız.
Bir hata oluşursa, 500 yanıt Kodu ve yanıt gövdesinde bir hata açıklaması metni alırsınız.

GET yöntemini kullanırken, ‘readonly’ ayar .lanmıştır. Başka bir deyişle, verileri değiştiren sorgular için yalnızca POST yöntemini kullanabilirsiniz. Sorgunun kendisini POST gövdesinde veya URL parametresinde gönderebilirsiniz.

Örnekler:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

Gördüğünüz gibi, curl, boşlukların URL'den kaçması gerektiği konusunda biraz rahatsız edici.
Her ne kadar wget her şeyden kaçsa da, onu kullanmanızı önermiyoruz çünkü keep-alive ve Transfer-Encoding: chunked kullanırken HTTP 1.1 üzerinde iyi çalışmıyor.

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

Sorgunun bir parçası parametrede gönderilirse ve gönderinin bir parçası ise, bu iki veri parçası arasına bir satır akışı eklenir.
Örnek (bu işe yaramaz):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

Varsayılan olarak, veri TabSeparated biçiminde döndürülür (daha fazla bilgi için bkz: “Formats” bölme).
Başka bir biçim istemek için sorgunun biçim yan tümcesi kullanın.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

Ekleme sorguları için veri iletmenin POST yöntemi gereklidir. Bu durumda, URL parametresinde sorgunun başlangıcını yazabilir ve eklemek için verileri iletmek için POST'u kullanabilirsiniz. Eklenecek veriler, örneğin Mysql'den sekmeyle ayrılmış bir döküm olabilir. Bu şekilde, INSERT sorgusu MYSQL'DEN load DATA LOCAL INFİLE'IN yerini alır.

Örnekler: tablo oluşturma:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

Veri ekleme için tanıdık ekleme sorgusunu kullanma:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

Veriler sorgudan ayrı olarak gönderilebilir:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

Herhangi bir veri biçimini belirtebilirsiniz. Bu ‘Values’ biçim, T değerlerine INSERT yazarken kullanılanla aynıdır:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

Sekmeyle ayrılmış bir dökümden veri eklemek için ilgili biçimi belirtin:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

Tablo içeriğini okuma. Paralel sorgu işleme nedeniyle veriler rastgele sırayla çıktılanır:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

Tabloyu silme.

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

Veri tablosu döndürmeyen başarılı istekler için boş bir yanıt gövdesi döndürülür.

Veri iletirken dahili ClickHouse sıkıştırma formatını kullanabilirsiniz. Sıkıştırılmış veriler standart olmayan bir biçime sahiptir ve özel `clickhouse-compressor` onunla çalışmak için program (bu ile yüklü `clickhouse-client` paket). Veri ekleme verimliliğini artırmak için, sunucu tarafı sağlama toplamı doğrulamasını kullanarak devre dışı bırakabilirsiniz. [http\_native\_compression\_disable\_checksumming\_on\_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) ayar.

Belirt ift ifiyseniz `compress=1` URL'de, sunucu size gönderdiği verileri sıkıştırır.
Belirt ift ifiyseniz `decompress=1` URL'de, sunucu içinde geçirdiğiniz aynı verileri açar. `POST` yöntem.

Ayrıca kullanmayı seçebilirsiniz [HTTP sıkıştırma](https://en.wikipedia.org/wiki/HTTP_compression). Sıkıştırılmış bir göndermek için `POST` istek, istek başlığını Ekle `Content-Encoding: compression_method`. Clickhouse'un yanıtı sıkıştırması için şunları eklemelisiniz `Accept-Encoding: compression_method`. ClickHouse destekler `gzip`, `br`, ve `deflate` [sıkıştırma yöntemleri](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens). HTTP sıkıştırmasını etkinleştirmek için Clickhouse'u kullanmanız gerekir [enable\_http\_compression](../operations/settings/settings.md#settings-enable_http_compression) ayar. Veri sıkıştırma düzeyini [http\_zlib\_compression\_level](#settings-http_zlib_compression_level) tüm sıkıştırma yöntemleri için ayarlama.

Bunu, büyük miktarda veri iletirken ağ trafiğini azaltmak veya hemen sıkıştırılmış dökümler oluşturmak için kullanabilirsiniz.

Sıkıştırma ile veri gönderme örnekleri:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "Not"
    Bazı HTTP istemcileri varsayılan olarak sunucudan verileri açabilir ( `gzip` ve `deflate`) ve sıkıştırma ayarlarını doğru kullansanız bile sıkıştırılmış veriler alabilirsiniz.

Kullanabilirsiniz ‘database’ Varsayılan veritabanını belirtmek için URL parametresi.

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

Varsayılan olarak, sunucu ayarlarında kayıtlı veritabanı varsayılan veritabanı olarak kullanılır. Varsayılan olarak, bu veritabanı denir ‘default’. Alternatif olarak, her zaman tablo adından önce bir nokta kullanarak veritabanını belirtebilirsiniz.

Kullanıcı adı ve şifre üç yoldan biriyle belirtilebilir:

1.  HTTP temel kimlik doğrulamasını kullanma. Örnek:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  İn the ‘user’ ve ‘password’ URL parametreleri. Örnek:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  Kullanım ‘X-ClickHouse-User’ ve ‘X-ClickHouse-Key’ üstbilgi. Örnek:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

Kullanıcı adı belirtilmemişse, `default` adı kullanılır. Parola belirtilmezse, boş parola kullanılır.
Tek bir sorguyu veya ayarların tüm profillerini işlemek için herhangi bir ayar belirtmek için URL parametrelerini de kullanabilirsiniz. Örnek: http: / / localhost: 8123/?profil = web & max\_rows\_to\_read = 1000000000 & query = seç + 1

Daha fazla bilgi için, bkz: [Ayarlar](../operations/settings/index.md) bölme.

``` bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

Diğer parametreler hakkında bilgi için bölüme bakın “SET”.

Benzer şekilde, http protokolünde ClickHouse oturumlarını kullanabilirsiniz. Bunu yapmak için şunları eklemeniz gerekir: `session_id` İsteğe parametre alın. Oturum kimliği olarak herhangi bir dize kullanabilirsiniz. Varsayılan olarak, oturum 60 saniye hareketsizlik sonra sonlandırılır. Bu zaman aşımını değiştirmek için, `default_session_timeout` sunucu yapılandırmasında ayarlama veya `session_timeout` İsteğe parametre alın. Oturum durumunu kontrol etmek için `session_check=1` parametre. Bir kerede yalnızca bir sorgu, tek bir oturum içinde çalıştırılabilir.

Bir sorgunun ilerleme durumu hakkında bilgi alabilirsiniz `X-ClickHouse-Progress` yanıt başlıkları. Bunu yapmak için etkinleştir [send\_progress\_in\_http\_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). Başlık dizisi örneği:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

Olası başlık alanları:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

Http bağlantısı kaybolursa çalışan istekler otomatik olarak durmaz. Ayrıştırma ve veri biçimlendirme sunucu tarafında gerçekleştirilir ve ağ kullanarak etkisiz olabilir.
Opsiyonel ‘query\_id’ parametre sorgu kimliği (herhangi bir dize) geçirilebilir. Daha fazla bilgi için bölüme bakın “Settings, replace\_running\_query”.

Opsiyonel ‘quota\_key’ parametre kota anahtarı (herhangi bir dize) olarak geçirilebilir. Daha fazla bilgi için bölüme bakın “Quotas”.

HTTP arabirimi, sorgulamak için dış verileri (dış geçici tablolar) geçirmenize izin verir. Daha fazla bilgi için bölüme bakın “External data for query processing”.

## Yanıt Tamponlama {#response-buffering}

Sunucu tarafında yanıt arabelleği etkinleştirebilirsiniz. Bu `buffer_size` ve `wait_end_of_query` Bu amaçla URL parametreleri sağlanmıştır.

`buffer_size` sunucu belleğinde arabellek sonucu bayt sayısını belirler. Sonuç gövdesi bu eşikten büyükse, arabellek HTTP kanalına yazılır ve kalan veriler doğrudan HTTP kanalına gönderilir.

Tüm yanıtın arabelleğe alındığından emin olmak için `wait_end_of_query=1`. Bu durumda, bellekte depolanan veriler geçici bir sunucu dosyasında arabelleğe alınır.

Örnek:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

Yanıt Kodu ve HTTP üstbilgileri istemciye gönderildikten sonra bir sorgu işleme hatası oluştu durumları önlemek için arabelleğe alma kullanın. Bu durumda, yanıt gövdesinin sonunda bir hata iletisi yazılır ve istemci tarafında hata yalnızca ayrıştırma aşamasında algılanabilir.

### Parametrelerle sorgular {#cli-queries-with-parameters}

Parametrelerle bir sorgu oluşturabilir ve karşılık gelen HTTP istek parametrelerinden onlar için değerler geçirebilirsiniz. Daha fazla bilgi için, bkz. [CLI için parametrelerle sorgular](cli.md#cli-queries-with-parameters).

### Örnek {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## Önceden tanımlanmış HTTP arabirimi {#predefined_http_interface}

ClickHouse HTTP arabirimi üzerinden belirli sorguları destekler. Örneğin, bir tabloya aşağıdaki gibi veri yazabilirsiniz:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse ayrıca gibi üçüncü parti araçları ile daha kolay entegrasyon yardımcı olabilir önceden tanımlanmış HTTP arayüzünü destekler [PROMETHEUS ihracatçı](https://github.com/percona-lab/clickhouse_exporter).

Örnek:

-   Her şeyden önce, bu bölümü sunucu yapılandırma dosyasına ekleyin:

<!-- -->

``` xml
<http_handlers>
    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </handler>
    </rule>
    <rule>...</rule>
    <rule>...</rule>
</http_handlers>
```

-   Artık PROMETHEUS formatında veriler için doğrudan url talep edebilirsiniz:

<!-- -->

``` bash
$ curl -v 'http://localhost:8123/predefined_query'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /predefined_query HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Tue, 28 Apr 2020 08:52:56 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-mloy5trc
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: 96fe0052-01e6-43ce-b12a-6b7370de6e8a
< X-ClickHouse-Format: Template
< X-ClickHouse-Timezone: Asia/Shanghai
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact


* Connection #0 to host localhost left intact
```

Örnekten görebileceğiniz gibi, Eğer `<http_handlers>` yapılandırmada yapılandırılır.xml dosyası ve `<http_handlers>` birçok içerebilir `<rule>s`. ClickHouse, önceden tanımlanmış türe alınan HTTP istekleriyle eşleşir `<rule>` ve ilk eşleşen işleyiciyi çalıştırır. Ardından, Maç başarılı olursa, ClickHouse ilgili önceden tanımlanmış sorguyu yürütecektir.

> Şimdi `<rule>` Yapılandır configureılabilir `<method>`, `<headers>`, `<url>`,`<handler>`:
> `<method>` HTTP isteğinin yöntem bölümünü eşleştirmekten sorumludur. `<method>` tam tanımına uygundur [yöntem](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) HTTP protokolünde. İsteğe bağlı bir yapılandırmadır. Yapılandırma dosyasında tanımlanmamışsa, HTTP isteğinin yöntem kısmıyla eşleşmez.
>
> `<url>` HTTP isteğinin url bölümünü eşleştirmekten sorumludur. İle uyumludur [RE2](https://github.com/google/re2)'In düzenli ifadeleri. İsteğe bağlı bir yapılandırmadır. Yapılandırma dosyasında tanımlanmamışsa, HTTP isteğinin url kısmıyla eşleşmez.
>
> `<headers>` HTTP isteğinin başlık kısmını eşleştirmekten sorumludur. Bu re2 düzenli ifadeler ile uyumludur. İsteğe bağlı bir yapılandırmadır. Yapılandırma dosyasında tanımlanmamışsa, HTTP isteğinin üstbilgi bölümü eşleşmiyor.
>
> `<handler>` ana işleme bölümünü içerir. Şimdi `<handler>` Yapılandır configureılabilir `<type>`, `<status>`, `<content_type>`, `<response_content>`, `<query>`, `<query_param_name>`.
> \> `<type>` şu anda üç tip destekler: **predefined\_query\_handler**, **dynamic\_query\_handler**, **sabit**.
> \>
> \> `<query>` - işleyici çağrıldığında predefined\_query\_handler türü ile kullanın, sorgu yürütür.
> \>
> \> `<query_param_name>` - dynamic\_query\_handler tipi ile kullanın, özler ve karşılık gelen değeri yürütür `<query_param_name>` HTTP isteği params değeri.
> \>
> \> `<status>` - statik tip, yanıt durum kodu ile kullanın.
> \>
> \> `<content_type>` - statik tip, tepki ile kullanın [içerik türü](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).
> \>
> \> `<response_content>` - önek kullanırken, istemciye gönderilen statik tip, yanıt içeriği ile kullanın ‘file://’ veya ‘config://’, dosyadan içeriği bulun veya yapılandırma istemciye gönderin.

Sonraki farklı yapılandırma yöntemleri `<type>`.

## predefined\_query\_handler {#predefined_query_handler}

`<predefined_query_handler>` ayar ayarları ve query\_params değerlerini destekler. Yapılandırabilirsiniz `<query>` tip ininde `<predefined_query_handler>`.

`<query>` değer, önceden tanımlanmış bir sorgudur `<predefined_query_handler>`, bir HTTP isteği eşleştirildiğinde ve sorgunun sonucu döndürüldüğünde ClickHouse tarafından yürütülür. Bu bir zorunluluktur yapılandırma.

Aşağıdaki örnek değerleri tanımlar `max_threads` ve `max_alter_threads` ayarlar, ardından bu ayarların başarıyla ayarlanıp ayarlanmadığını kontrol etmek için sistem tablosunu sorgular.

Örnek:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "Dikkat"
    Birinde `<predefined_query_handler>` sadece birini destekler `<query>` bir ekleme türü.

## dynamic\_query\_handler {#dynamic_query_handler}

İçinde `<dynamic_query_handler>`, sorgu HTTP isteğinin param şeklinde yazılır. Fark şu ki `<predefined_query_handler>`, sorgu yapılandırma dosyasında yazılır. Yapılandırabilirsiniz `<query_param_name>` içinde `<dynamic_query_handler>`.

ClickHouse ayıklar ve karşılık gelen değeri yürütür `<query_param_name>` HTTP isteğinin url'sindeki değer. Varsayılan değer `<query_param_name>` oluyor `/query` . İsteğe bağlı bir yapılandırmadır. Yapılandırma dosyasında tanım yoksa, param iletilmez.

Bu işlevselliği denemek için örnek max\_threads ve max\_alter\_threads değerlerini tanımlar ve ayarların başarıyla ayarlanıp ayarlanmadığını sorgular.

Örnek:

``` xml
<http_handlers>
    <rule>
    <headers>
        <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>    </headers>
    <handler>
        <type>dynamic_query_handler</type>
        <query_param_name>query_param</query_param_name>
    </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_alter_threads=2&param_name_1=max_threads&param_name_2=max_alter_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_alter_threads   2
```

## sabit {#static}

`<static>` dön canebilir [content\_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [durum](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) ve response\_content. response\_content belirtilen içeriği döndürebilir

Örnek:

Mesajı dönmektedir.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
<http_handlers>
```

``` bash
$ curl -vv  -H 'XXX:xxx' 'http://localhost:8123/hi'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /hi HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 402 Payment Required
< Date: Wed, 29 Apr 2020 03:51:26 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

Yapılandırmadan içeriği bulun istemciye gönder.

``` xml
<get_config_static_handler><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></get_config_static_handler>

<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_config_static_handler</url>
            <handler>
                <type>static</type>
                <response_content>config://get_config_static_handler</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ curl -v  -H 'XXX:xxx' 'http://localhost:8123/get_config_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_config_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:01:24 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

İstemciye gönder dosyadan içeriği bulun.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file:///absolute_path_file.html</response_content>
            </handler>
        </rule>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_relative_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file://./relative_path_file.html</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ user_files_path='/var/lib/clickhouse/user_files'
$ sudo echo "<html><body>Relative Path File</body></html>" > $user_files_path/relative_path_file.html
$ sudo echo "<html><body>Absolute Path File</body></html>" > $user_files_path/absolute_path_file.html
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_absolute_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_absolute_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:16 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Absolute Path File</body></html>
* Connection #0 to host localhost left intact
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_relative_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_relative_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:31 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/http_interface/) <!--hide-->
