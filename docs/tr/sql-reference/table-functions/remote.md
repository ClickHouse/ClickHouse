---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: uzak
---

# uzaktan, remoteSecure {#remote-remotesecure}

Oluşturmadan uzak sunuculara erişmenizi sağlar. `Distributed` Tablo.

İmzalar:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port` ya da sadece `host`. Ana bilgisayar sunucu adı veya IPv4 veya IPv6 adresi olarak belirtilebilir. Köşeli parantez içinde bir IPv6 adresi belirtilir. Bağlantı noktası, uzak sunucudaki TCP bağlantı noktasıdır. Bağlantı noktası atlanırsa, kullanır `tcp_port` sunucunun yapılandırma dosyasından (varsayılan olarak, 9000).

!!! important "Önemli"
    Bir IPv6 adresi için bağlantı noktası gereklidir.

Örnekler:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Birden çok Adres virgülle ayrılmış olabilir. Bu durumda, ClickHouse dağıtılmış işleme kullanır, bu nedenle sorguyu belirtilen tüm adreslere gönderir (farklı verilerle kırıklar gibi).

Örnek:

``` text
example01-01-1,example01-02-1
```

İfadenin bir kısmı kıvırcık parantez içinde belirtilebilir. Önceki örnek aşağıdaki gibi yazılabilir:

``` text
example01-0{1,2}-1
```

Kıvırcık parantez iki nokta (negatif olmayan tamsayılar) ile ayrılmış bir sayı aralığı içerebilir. Bu durumda, Aralık, shard adresleri üreten bir değer kümesine genişletilir. İlk sayı sıfır ile başlarsa, değerler aynı sıfır hizalamasıyla oluşturulur. Önceki örnek aşağıdaki gibi yazılabilir:

``` text
example01-{01..02}-1
```

Birden fazla kıvırcık parantez çiftiniz varsa, ilgili kümelerin doğrudan ürününü oluşturur.

Adresler ve kıvırcık parantez içindeki adreslerin parçaları boru sembolü (\|) ile ayrılabilir. Bu durumda, karşılık gelen Adres kümeleri yinelemeler olarak yorumlanır ve sorgu ilk sağlıklı yinelemeye gönderilir. Ancak, yinelemeler şu anda ayarlanmış sırayla yinelenir [dengeleme](../../operations/settings/settings.md) ayar.

Örnek:

``` text
example01-{01..02}-{1|2}
```

Bu örnek, her birinin iki kopyası olan iki parçayı belirtir.

Oluşturulan Adres sayısı bir sabit tarafından sınırlıdır. Şu anda bu 1000 Adres.

Kullanarak `remote` tablo işlevi, bir `Distributed` tablo, çünkü bu durumda, her istek için sunucu bağlantısı yeniden kurulur. Buna ek olarak, ana bilgisayar adları ayarlanmışsa, adlar giderilir ve çeşitli yinelemelerle çalışırken hatalar sayılmaz. Çok sayıda sorgu işlerken, her zaman `Distributed` masa vaktinden önce, ve kullanmayın `remote` tablo işlevi.

Bu `remote` tablo işlevi aşağıdaki durumlarda yararlı olabilir:

-   Veri karşılaştırma, hata ayıklama ve sınama için belirli bir sunucuya erişme.
-   Araştırma amaçlı çeşitli ClickHouse kümeleri arasındaki sorgular.
-   El ile yapılan seyrek dağıtılmış istekler.
-   Sunucu kümesinin her seferinde yeniden tanımlandığı dağıtılmış istekler.

Kullanıcı belirtilmemişse, `default` kullanılır.
Parola belirtilmezse, boş bir parola kullanılır.

`remoteSecure` - aynı `remote` but with secured connection. Default port — [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) yapılandırma veya 9440'ten.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
