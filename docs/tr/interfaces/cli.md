---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 17
toc_title: "Komut Sat\u0131r\u0131 \u0130stemcisi"
---

# Komut satırı istemcisi {#command-line-client}

ClickHouse yerel bir komut satırı istemcisi sağlar: `clickhouse-client`. İstemci komut satırı seçeneklerini ve yapılandırma dosyalarını destekler. Daha fazla bilgi için, bkz. [Yapılandırma](#interfaces_cli_configuration).

[Yüklemek](../getting-started/index.md) ıt from the `clickhouse-client` paketleyin ve komutla çalıştırın `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

Farklı istemci ve sunucu sürümleri birbiriyle uyumludur, ancak bazı özellikler eski istemcilerde kullanılamayabilir. Biz sunucu uygulaması olarak istemci aynı sürümünü kullanmanızı öneririz. Eski sürümün bir istemcisini kullanmaya çalıştığınızda, daha sonra sunucu, `clickhouse-client` mesajı görüntüler:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## Kullanma {#cli_usage}

İstemci etkileşimli ve etkileşimli olmayan (toplu iş) modunda kullanılabilir. Toplu iş modunu kullanmak için ‘query’ parametre veya veri göndermek ‘stdin’ (bunu doğrular ‘stdin’ bir terminal değildir) veya her ikisi de. HTTP arayüzüne benzer, kullanırken ‘query’ parametre ve veri gönderme ‘stdin’, istek bir birleştirme olduğunu ‘query’ parametre, bir satır besleme ve veri ‘stdin’. Bu, büyük ekleme sorguları için uygundur.

Veri eklemek için istemci kullanma örneği:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

Toplu iş modunda, varsayılan veri biçimi TabSeparated. Sorgunun biçim yan tümcesinde biçimi ayarlayabilirsiniz.

Varsayılan olarak, yalnızca tek bir sorguyu toplu iş modunda işleyebilirsiniz. Birden çok sorgu yapmak için bir “script,” kullan... `--multiquery` parametre. Bu, INSERT dışındaki tüm sorgular için çalışır. Sorgu sonuçları, ek ayırıcılar olmadan ardışık olarak çıktılanır. Benzer şekilde, çok sayıda sorgu işlemek için, çalıştırabilirsiniz ‘clickhouse-client’ her sorgu için. Başlatmak için onlarca milisaniye sürebilir unutmayın ‘clickhouse-client’ program.

Etkileşimli modda, sorguları girebileceğiniz bir komut satırı alırsınız.

Eğer ‘multiline’ belirtilmemiş (varsayılan): sorguyu çalıştırmak için Enter tuşuna basın. Noktalı virgül, sorgunun sonunda gerekli değildir. Çok satırlı bir sorgu girmek için ters eğik çizgi girin `\` hat beslemeden önce. Enter tuşuna bastıktan sonra, sorgunun sonraki satırını girmeniz istenecektir.

Çok satırlı belirtilirse: bir sorguyu çalıştırmak için, noktalı virgülle sonlandırın ve Enter tuşuna basın. Noktalı virgül, girilen satırın sonunda atlandıysa, sorgunun bir sonraki satırını girmeniz istenecektir.

Yalnızca tek bir sorgu çalıştırılır, bu nedenle noktalı virgülden sonra her şey göz ardı edilir.

Belirtebilirsiniz `\G` noktalı virgül yerine veya sonra. Bu dikey biçimi gösterir. Bu formatta, her değer geniş tablolar için uygun olan ayrı bir satıra yazdırılır. Bu sıradışı özellik MySQL CLI ile uyumluluk için eklendi.

Komut satırı dayanmaktadır ‘replxx’ (benzer ‘readline’). Başka bir deyişle, tanıdık klavye kısayollarını kullanır ve bir geçmişi tutar. Tarih yazılır `~/.clickhouse-client-history`.

Varsayılan olarak, kullanılan biçim PrettyCompact. Sorgunun biçim yan tümcesinde veya belirterek biçimi değiştirebilirsiniz `\G` sorgunun sonunda, `--format` veya `--vertical` komut satırında veya istemci yapılandırma dosyasını kullanarak bağımsız değişken.

İstemciden çıkmak için Ctrl+D (veya Ctrl+C) tuşlarına basın veya bir sorgu yerine aşağıdakilerden birini girin: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

Bir sorguyu işlerken, istemci şunları gösterir:

1.  Saniyede en fazla 10 kez güncellenen ilerleme (varsayılan olarak). Hızlı sorgular için ilerleme görüntülenecek zaman olmayabilir.
2.  Hata ayıklama için ayrıştırmadan sonra biçimlendirilmiş sorgu.
3.  Belirtilen biçimde sonuç.
4.  Sonuçtaki satır sayısı, geçen süre ve sorgu işlemenin ortalama hızı.

Ctrl + C tuşlarına basarak uzun bir sorguyu iptal edebilirsiniz. ancak, sunucunun isteği iptal etmesi için biraz beklemeniz gerekir. Belirli aşamalarda bir sorguyu iptal etmek mümkün değildir. Beklemezseniz ve ikinci kez Ctrl + C tuşlarına basarsanız, istemci çıkacaktır.

Komut satırı istemcisi, sorgulamak için dış verileri (dış geçici tablolar) geçirmenize izin verir. Daha fazla bilgi için bölüme bakın “External data for query processing”.

### Parametrelerle sorgular {#cli-queries-with-parameters}

Parametrelerle bir sorgu oluşturabilir ve istemci uygulamasından onlara değerler aktarabilirsiniz. Bu, istemci tarafında belirli dinamik değerlerle biçimlendirme sorgusunu önlemeye izin verir. Mesela:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### Sorgu Sözdizimi {#cli-queries-with-parameters-syntax}

Bir sorguyu her zamanki gibi biçimlendirin, ardından uygulama parametrelerinden sorguya geçirmek istediğiniz değerleri parantez içinde aşağıdaki biçimde yerleştirin:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [Veri türü](../sql-reference/data-types/index.md) app parametre değeri. Örneğin, aşağıdaki gibi bir veri yapısı `(integer, ('string', integer))` olabilir var `Tuple(UInt8, Tuple(String, UInt8))` veri türü (başka birini de kullanabilirsiniz [tamsayı](../sql-reference/data-types/int-uint.md) türler).

#### Örnek {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## Yapılandırma {#interfaces_cli_configuration}

Parametreleri iletebilirsiniz `clickhouse-client` (tüm parametrelerin varsayılan değeri vardır) :

-   Komut satır fromından

    Komut satırı seçenekleri, yapılandırma dosyalarındaki varsayılan değerleri ve ayarları geçersiz kılar.

-   Yapılandırma dosyaları.

    Yapılandırma dosyalarındaki ayarlar varsayılan değerleri geçersiz kılar.

### Komut Satırı Seçenekleri {#command-line-options}

-   `--host, -h` -– The server name, ‘localhost’ varsayılan olarak. Adı veya IPv4 veya IPv6 adresini kullanabilirsiniz.
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ varsayılan) tarafından.
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as ‘–format=Vertical’. Bu biçimde, her bir değer, geniş tabloları görüntülerken yardımcı olan ayrı bir satıra yazdırılır.
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ etkileşimli olmayan modda.
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection.
-   `--param_<name>` — Value for a [parametrelerle sorgu](#cli-queries-with-parameters).

### Yapılandırma Dosyaları {#configuration_files}

`clickhouse-client` aşağıdaki ilk varolan dosyayı kullanır:

-   Tanımlanan `--config-file` parametre.
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

Bir yapılandırma dosyası örneği:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/cli/) <!--hide-->
