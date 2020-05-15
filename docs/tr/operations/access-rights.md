---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 48
toc_title: "Eri\u015Fim Haklar\u0131"
---

# Erişim Hakları {#access-rights}

Kullanıcılar ve erişim hakları kullanıcı yapılandırmasında ayarlanır. Bu genellikle `users.xml`.

Kullanıcılar kaydedilir `users` bölme. İşte bir parçası `users.xml` Dosya:

``` xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify password in plaintext (not recommended), place it in 'password' element.
             Example: <password>qwerty</password>.
             Password could be empty.

             If you want to specify SHA256, place it in 'password_sha256_hex' element.
             Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>

        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip> The IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.host\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.host\.ru$.

            If you are installing ClickHouse yourself, specify here:
                <networks>
                        <ip>::/0</ip>
                </networks>
        -->
        <networks incl="networks" />

        <!-- Settings profile for the user. -->
        <profile>default</profile>

        <!-- Quota for the user. -->
        <quota>default</quota>
    </default>

    <!-- For requests from the Yandex.Metrica user interface via the API for data on specific counters. -->
    <web>
        <password></password>
        <networks incl="networks" />
        <profile>web</profile>
        <quota>default</quota>
        <allow_databases>
           <database>test</database>
        </allow_databases>
        <allow_dictionaries>
           <dictionary>test</dictionary>
        </allow_dictionaries>
    </web>
</users>
```

İki kullanıcıdan bir bildirim görebilirsiniz: `default`ve`web`. Ek weledik `web` kullanıcı ayrı ayrı.

Bu `default` kullanıcı adı geçilmez durumlarda kullanıcı seçilir. Bu `default` kullanıcı, sunucu veya kümenin yapılandırması, sunucu veya kümenin yapılandırılmasını belirtmezse, dağıtılmış sorgu işleme için de kullanılır. `user` ve `password` (on bölümüne bakın [Dağılı](../engines/table-engines/special/distributed.md) motor).

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

Parola, açık metin (önerilmez) veya SHA-256’da belirtilir. Haşhaş tuzlu değil. Bu bağlamda, bu şifreleri potansiyel kötü amaçlı saldırılara karşı güvenlik sağlamak olarak düşünmemelisiniz. Aksine, çalışanlardan korunmak için gereklidir.

Erişime izin verilen ağların listesi belirtilir. Bu örnekte, her iki kullanıcı için ağ listesi ayrı bir dosyadan yüklenir (`/etc/metrika.xml`) içeren `networks` ikame. İşte bunun bir parçası:

``` xml
<yandex>
    ...
    <networks>
        <ip>::/64</ip>
        <ip>203.0.113.0/24</ip>
        <ip>2001:DB8::/32</ip>
        ...
    </networks>
</yandex>
```

Bu ağ listesini doğrudan tanımlayabilirsiniz `users.xml` veya bir dosyada `users.d` dizin (daha fazla bilgi için bölüme bakın “[Yapılandırma dosyaları](configuration-files.md#configuration_files)”).

Yapılandırma, her yerden erişimin nasıl açılacağını açıklayan yorumları içerir.

Üretimde kullanım için, sadece belirtin `ip` elemanları (IP adresleri ve maskeleri), kullanıl ,dığından beri `host` ve `hoost_regexp` ekstra gecikmeye neden olabilir.

Daha sonra kullanıcı ayarları profili belirtilir (bölüme bakın “[Ayarlar profilleri](settings/settings-profiles.md)”. Varsayılan profili belirtebilirsiniz, `default'`. Profilin herhangi bir adı olabilir. Farklı kullanıcılar için aynı profili belirtebilirsiniz. Ayarlar profilinde yazabileceğiniz en önemli şey `readonly=1` sağlar okumak-sadece erişim. Ardından kullanılacak kotayı belirtin (bölüme bakın “[Kotalar](quotas.md#quotas)”). Varsayılan kotayı belirtebilirsiniz: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

İsteğe bağlı `<allow_databases>` bölümünde, kullanıcının erişebileceği veritabanlarının bir listesini de belirtebilirsiniz. Varsayılan olarak, tüm veritabanları kullanıcı tarafından kullanılabilir. Belirtebilirsiniz `default` veritabanı. Bu durumda, kullanıcı varsayılan olarak veritabanına erişim alır.

İsteğe bağlı `<allow_dictionaries>` bölümünde, kullanıcının erişebileceği sözlüklerin bir listesini de belirtebilirsiniz. Varsayılan olarak, tüm sözlükler kullanıcı tarafından kullanılabilir.

Erişim `system` veritabanı her zaman izin verilir (bu veritabanı sorguları işlemek için kullanıldığından).

Kullanıcı kullanarak onları tüm veritabanları ve tabloların bir listesini alabilirsiniz `SHOW` tek tek veritabanlarına erişime izin verilmese bile, sorgular veya sistem tabloları.

Veritabanı erişimi ile ilgili değildir [readonly](settings/permissions-for-queries.md#settings_readonly) ayar. Bir veritabanına tam erişim izni veremezsiniz ve `readonly` başka birine erişim.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
