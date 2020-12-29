---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "Kullan\u0131c\u0131 Ayarlar\u0131"
---

# Kullanıcı Ayarları {#user-settings}

Bu `users` bu bölüm `user.xml` yapılandırma dosyası kullanıcı ayarlarını içerir.

!!! note "Bilgi"
    ClickHouse da destekler [SQL tabanlı iş akışı](../access-rights.md#access-control) kullanıcıları yönetmek için. Bunu kullanmanızı öneririz.

Bu yapı `users` bölme:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
    <!-- Other users settings -->
</users>
```

### home/şifre {#user-namepassword}

Şifre düz metin veya SHA256 (hex formatında) belirtilebilir.

-   Düz metin içinde bir şifre atamak için (**tavsiye edilmez**bir koyun `password` öğe.

    Mesela, `<password>qwerty</password>`. Şifre boş bırakılabilir.

<a id="password_sha256_hex"></a>

-   SHA256 karmasını kullanarak bir şifre atamak için, bir `password_sha256_hex` öğe.

    Mesela, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    Kabuktan bir parola oluşturma örneği:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    Sonucun ilk satırı şifredir. İkinci satır karşılık gelen SHA256 karmasıdır.

<a id="password_double_sha1_hex"></a>

-   MySQL istemcileri ile uyumluluk için, şifre çift SHA1 karma belirtilebilir. İçine yerleştirin `password_double_sha1_hex` öğe.

    Mesela, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    Kabuktan bir parola oluşturma örneği:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    Sonucun ilk satırı şifredir. İkinci satır karşılık gelen çift SHA1 karmasıdır.

### access\_management {#access_management-user-setting}

Bu ayar, SQL-driven kullanarak devre dışı bırakır sağlar [erişim kontrolü ve hesap yönetimi](../access-rights.md#access-control) kullanıcı için.

Olası değerler:

-   0 — Disabled.
-   1 — Enabled.

Varsayılan değer: 0.

### kullanıcı\_adı / ağlar {#user-namenetworks}

Kullanıcının ClickHouse sunucusuna bağlanabileceği ağların listesi.

Listenin her öğesi aşağıdaki formlardan birine sahip olabilir:

-   `<ip>` — IP address or network mask.

    Örnekler: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    Örnek: `example01.host.ru`.

    Erişimi denetlemek için bir DNS sorgusu gerçekleştirilir ve döndürülen tüm IP adresleri eş adresiyle karşılaştırılır.

-   `<host_regexp>` — Regular expression for hostnames.

    Örnek, `^example\d\d-\d\d-\d\.host\.ru$`

    Erişimi kontrol etmek için, bir [DNS ptr sorgusu](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) eş adresi için gerçekleştirilir ve sonra belirtilen regexp uygulanır. Daha sonra PTR sorgusunun sonuçları için başka bir DNS sorgusu gerçekleştirilir ve alınan tüm adresler eş adresine karşılaştırılır. Regexp'nin $ile bitmesini şiddetle tavsiye ederiz.

Sunucu yeniden başlatılıncaya kadar DNS isteklerinin tüm sonuçları önbelleğe alınır.

**Örnekler**

Herhangi bir ağdan kullanıcı için erişimi açmak için şunları belirtin:

``` xml
<ip>::/0</ip>
```

!!! warning "Uyarıcı"
    Düzgün yapılandırılmış bir güvenlik duvarınız yoksa veya sunucu doğrudan internete bağlı değilse, herhangi bir ağdan erişimi açmak güvensizdir.

Erişimi yalnızca localhost'tan açmak için şunları belirtin:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### kullanıcı\_adı / profil {#user-nameprofile}

Kullanıcı için bir ayarlar profili atayabilirsiniz. Ayarlar profilleri ayrı bir bölümde yapılandırılır `users.xml` Dosya. Daha fazla bilgi için, bkz. [Ayarların profilleri](settings-profiles.md).

### user\_name / kota {#user-namequota}

Kotalar, belirli bir süre boyunca kaynak kullanımını izlemenize veya sınırlamanıza izin verir. Kotalar yapılandırılır `quotas`
bu bölüm `users.xml` yapılandırma dosyası.

Kullanıcı için ayarlanmış bir kotalar atayabilirsiniz. Kotalar yapılandırmasının ayrıntılı bir açıklaması için bkz. [Kotalar](../quotas.md#quotas).

### user\_name / veritabanları {#user-namedatabases}

Bu bölümde, ClickHouse tarafından döndürülen satırları sınırlayabilirsiniz `SELECT` geçerli kullanıcı tarafından yapılan sorgular, böylece temel satır düzeyinde güvenlik uygular.

**Örnek**

Aşağıdaki yapılandırma bu kullanıcıyı zorlar `user1` sadece satırları görebilirsiniz `table1` sonucu olarak `SELECT` sorgular, burada değeri `id` alan 1000'dir.

``` xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

Bu `filter` bir sonuç veren herhangi bir ifade olabilir [Uİnt8](../../sql-reference/data-types/int-uint.md)- tip değeri. Genellikle karşılaştırmalar ve mantıksal operatörler içerir. Satır fromlardan `database_name.table1` burada filtre sonuçları 0 için bu kullanıcı için döndürülür. Filtreleme ile uyumsuz `PREWHERE` işlemler ve devre dışı bırakır `WHERE→PREWHERE` optimizasyon.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
