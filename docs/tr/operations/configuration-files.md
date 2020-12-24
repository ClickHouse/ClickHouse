---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: "Yap\u0131land\u0131rma Dosyalar\u0131"
---

# Yapılandırma Dosyaları {#configuration_files}

ClickHouse Çoklu dosya yapılandırma yönetimini destekler. Ana sunucu yapılandırma dosyası `/etc/clickhouse-server/config.xml`. Diğer dosyalar içinde olmalıdır `/etc/clickhouse-server/config.d` dizin.

!!! note "Not"
    Tüm yapılandırma dosyaları XML biçiminde olmalıdır. Ayrıca, genellikle aynı kök öğeye sahip olmalıdırlar `<yandex>`.

Ana yapılandırma dosyasında belirtilen bazı ayarlar diğer yapılandırma dosyalarında geçersiz kılınabilir. Bu `replace` veya `remove` bu yapılandırma dosyalarının öğeleri için öznitelikler belirtilebilir.

Her ikisi de belirtilmezse, yinelenen çocukların değerlerini değiştirerek öğelerin içeriğini yinelemeli olarak birleştirir.

Eğer `replace` belirtilen, tüm öğeyi belirtilen ile değiştirir.

Eğer `remove` belirt .ilirse, öğeyi siler.

Yapılandırma ayrıca tanımlayabilir “substitutions”. Bir öğe varsa `incl` öznitelik, dosyadan karşılık gelen ikame değeri olarak kullanılacaktır. Varsayılan olarak, değiştirmeler ile dosyanın yolu `/etc/metrika.xml`. Bu değiştirilebilir [include_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) sunucu yapılandırmasında öğe. İkame değerleri belirtilen `/yandex/substitution_name` bu dosyadaki öğeler. Belirtilen bir ika Ame halinde `incl` yok, günlüğe kaydedilir. Clickhouse'un eksik değiştirmelerin günlüğe kaydedilmesini önlemek için `optional="true"` öznitelik (örneğin, ayarlar [makrolar](server-configuration-parameters/settings.md)).

İkame da ZooKeeper yapılabilir. Bunu yapmak için özniteliği belirtin `from_zk = "/path/to/node"`. Eleman değeri, düğümün içeriği ile değiştirilir `/path/to/node` ZooKeeper. Ayrıca ZooKeeper düğümünde bir XML alt ağacının tamamını koyabilirsiniz ve kaynak öğeye tamamen eklenecektir.

Bu `config.xml` dosya kullanıcı ayarları, profiller ve kotalar ile ayrı bir yapılandırma belirtebilirsiniz. Bu yapılandırmanın göreli yolu, `users_config` öğe. Varsayılan olarak, bu `users.xml`. Eğer `users_config` atlanır, kullanıcı ayarları, profiller ve kotalar doğrudan belirtilir `config.xml`.

Kullanıcılar yapılandırma benzer ayrı dosyaları içine bölünmüş olabilir `config.xml` ve `config.d/`.
Dizin adı olarak tanımlanır `users_config` olmadan ayarı `.xml` postfix ile birleştirilmiş `.d`.
Dizin `users.d` varsayılan olarak kullanılır, gibi `users_config` varsayılan olarak `users.xml`.
Örneğin, bu gibi her kullanıcı için ayrı yapılandırma dosyasına sahip olabilirsiniz:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<yandex>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</yandex>
```

Her yapılandırma dosyası için sunucu da üretir `file-preprocessed.xml` başlatırken dosyalar. Bu dosyalar, tamamlanmış tüm değiştirmeleri ve geçersiz kılmaları içerir ve bunlar bilgi amaçlı kullanım içindir. Zookeeper değiştirmelerin yapılandırma dosyalarında kullanılan ancak ZooKeeper sunucu başlangıcında kullanılabilir değilse, sunucu yapılandırmayı önceden işlenmiş dosyadan yükler.

Sunucu, yapılandırma dosyalarındaki değişikliklerin yanı sıra, değiştirmeleri ve geçersiz kılmaları gerçekleştirirken kullanılan dosya ve ZooKeeper düğümlerini izler ve anında kullanıcılar ve kümeler için ayarları yeniden yükler. Bu, sunucuyu yeniden başlatmadan kümeyi, kullanıcıları ve ayarlarını değiştirebileceğiniz anlamına gelir.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
