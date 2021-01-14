---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 20
toc_title: "MySQL Aray\xFCz\xFC"
---

# MySQL Arayüzü {#mysql-interface}

ClickHouse MySQL Tel protokolünü destekler. Tarafından etkinleştir canilebilir [mysql\_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) yapılandırma dosyasında ayarlama:

``` xml
<mysql_port>9004</mysql_port>
```

Komut satırı aracını kullanarak bağlanma örneği `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

Bir bağlantı başarılı olursa çıktı:

``` text
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 20.2.1.1-ClickHouse

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

Tüm MySQL istemcileri ile uyumluluk için, kullanıcı parolasını belirtmeniz önerilir [çift SHA1](../operations/settings/settings-users.md#password_double_sha1_hex) yapılandırma dosyasında.
Kullanarak kullanıcı şifresi belirt ifilirse [SHA256](../operations/settings/settings-users.md#password_sha256_hex), bazı istemciler (mysqljs ve komut satırı aracı mysql eski sürümleri) kimlik doğrulaması mümkün olmayacaktır.

Kısıtlama:

-   hazırlanan sorgular desteklenmiyor

-   bazı veri türleri dizeleri olarak gönderilir

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/mysql/) <!--hide-->
