---
description: 'Настройки для настройки пользователей и ролей.'
sidebar_label: 'Настройки пользователя'
sidebar_position: 63
slug: /operations/settings/settings-users
title: 'Настройки пользователей и ролей'
doc_type: 'reference'
---

Раздел `users` конфигурационного файла `users.xml` содержит настройки пользователей.

:::note
ClickHouse также поддерживает [управление пользователями через SQL](/ru/operations/access-rights#access-control-usage). Рекомендуем использовать именно его.
:::

Структура раздела `users`:

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

Пароль можно указать в открытом виде или в виде хэша SHA256 (в шестнадцатеричном формате).

* Чтобы задать пароль в открытом виде (**не рекомендуется**), поместите его в элемент `password`.

  Например: `<password>qwerty</password>`. Пароль можно оставить пустым.

<a id="password_sha256_hex" />

* Чтобы задать пароль с помощью хэша SHA256, поместите его в элемент `password_sha256_hex`.

  Например: `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

  Пример генерации пароля в оболочке:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  Первая строка результата — пароль. Вторая строка — соответствующий хэш SHA256.

<a id="password_double_sha1_hex" />

* Для совместимости с клиентами MySQL пароль можно указать в виде двойного хэша SHA1. Поместите его в элемент `password_double_sha1_hex`.

  Например: `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

  Пример генерации пароля в оболочке:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  Первая строка результата — пароль. Вторая строка — соответствующий двойной хэш SHA1.

<div id="totp-authentication-configuration">
  ### Конфигурация TOTP-аутентификации
</div>

TOTP (одноразовый пароль на основе времени) можно использовать для аутентификации пользователей ClickHouse, генерируя временные коды доступа, действующие в течение ограниченного времени.
Этот метод аутентификации TOTP соответствует стандарту [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238), что делает его совместимым с популярными TOTP-приложениями, такими как Google Authenticator, 1Password и аналогичными инструментами.
Его можно настроить через файл конфигурации `users.xml` в дополнение к аутентификации по паролю.
В SQL-driven системе управления доступом он пока не поддерживается.

Для аутентификации с помощью TOTP пользователи должны указать основной пароль вместе с одноразовым паролем, сгенерированным их TOTP-приложением, через параметр командной строки `--one-time-password` или добавить его к основному паролю через символ &#39;+&#39;.
Например, если основной пароль — `some_password`, а сгенерированный TOTP-код — `345123`, пользователь может указать `--password some_password+345123` или `--password some_password --one-time-password 345123` при подключении к ClickHouse. Если пароль не указан, `clickhouse-client` запросит его в интерактивном режиме.

Чтобы включить TOTP-аутентификацию для пользователя, настройте раздел `time_based_one_time_password` в `users.xml`. Этот раздел определяет параметры TOTP, такие как секрет, период действия, количество цифр и алгоритм хеширования.

**Пример**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
````

Эта команда создаст секрет, закодированный в base32, который можно добавить в поле secret в users.xml.

Чтобы включить TOTP для конкретного пользователя, добавьте в любое существующее поле с паролем (например, `password` или `password_sha256_hex`) ещё один раздел `time_based_one_time_password`.

Для генерации QR-кода для секрета TOTP можно использовать утилиту [qrencode](https://linux.die.net/man/1/qrencode).

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

После настройки TOTP для пользователя одноразовый пароль можно использовать в рамках процесса аутентификации, как описано выше.

### username/ssh-key

Этот параметр позволяет проходить аутентификацию с помощью SSH-ключей.

Имея SSH-ключ (например, сгенерированный с помощью `ssh-keygen`),

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

Элемент `ssh_key` должен быть

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

Замените `ssh-ed25519` на `ssh-rsa` или `ecdsa-sha2-nistp256`, чтобы использовать другие поддерживаемые алгоритмы.

### Несколько методов аутентификации

Для одного пользователя можно настроить несколько методов аутентификации с помощью элемента `<auth_methods>`. Это позволяет пользователю проходить аутентификацию с использованием любого из перечисленных методов — например, у пользователя могут быть одновременно пароль и учетные данные LDAP, и вход с любым из них будет успешным.

Каждый дочерний элемент `<auth_methods>` представляет собой произвольно именованный wrapper, содержащий ровно один тип аутентификации. Имя wrapper’а (например, `<method1>`, `<primary>`, `<a1>`) не имеет значения; используется только вложенный элемент аутентификации.

**Пример: несколько паролей**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**Пример: смешанные способы аутентификации**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

Внутри `<auth_methods>` поддерживаются следующие типы аутентификации:

* **`password`** — пароль в открытом виде
* **`password_sha256_hex`** — хэш пароля SHA256
* **`password_scram_sha256_hex`** — хэш пароля SCRAM-SHA-256
* **`password_double_sha1_hex`** — двойной хэш SHA1 пароля
* **`ldap`** — аутентификация через LDAP-сервер
* **`kerberos`** — аутентификация Kerberos
* **`ssl_certificates`** — аутентификация по SSL-сертификату
* **`ssh_keys`** — аутентификация по SSH-ключу
* **`http_authentication`** — HTTP-аутентификация

**Правила и ограничения:**

* `<auth_methods>` **нельзя** использовать вместе с методами аутентификации, заданными на уровне пользователя. Используйте либо один вариант, либо другой, но не оба одновременно.
* `<auth_methods>` должен содержать как минимум один метод аутентификации.
* Каждый элемент-обёртка внутри `<auth_methods>` должен содержать ровно один тип аутентификации (за исключением `<ssh_keys>`, который для обратной совместимости может содержать несколько).
* TOTP (`<time_based_one_time_password>`) указывается на уровне пользователя (вне `<auth_methods>`) и применяется ко всем методам на основе пароля в списке. Если TOTP включён, требуется как минимум один метод на основе пароля.

**Пример: `auth_methods` с TOTP**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

В этом примере проверка TOTP применяется к методу аутентификации по паролю (`<password>`), тогда как метод LDAP независимо выполняет аутентификацию через внешний сервер.

### access_management

Этот параметр включает или отключает использование [системы управления доступом и учётными записями](/ru/operations/access-rights#access-control-usage), управляемой через SQL, для пользователя.

Возможные значения:

* 0 — Отключено.
* 1 — Включено.

Значение по умолчанию: 0.

### grants

Эта настройка позволяет назначать выбранному пользователю любые привилегии.
Каждый элемент списка должен представлять собой запрос `GRANT` без указания получателей привилегий.

Пример:

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

Этот параметр нельзя указывать одновременно с параметрами
`dictionaries`, `access_management`, `named_collection_control`, `show_named_collections_secrets`
и `allow_databases`.

### user_name/networks

Список сетей, из которых пользователь может подключаться к серверу ClickHouse.

Каждый элемент списка может иметь одну из следующих форм:

* `<ip>` — IP-адрес или маска сети.

  Примеры: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — имя хоста.

  Пример: `example01.host.ru`.

  Для проверки доступа выполняется DNS-запрос, и все возвращённые IP-адреса сравниваются с адресом узла, с которого выполняется подключение.

* `<host_regexp>` — регулярное выражение для имён хостов.

  Пример: `^example\d\d-\d\d-\d\.host\.ru$`

  Для проверки доступа для адреса узла, с которого выполняется подключение, выполняется [DNS PTR-запрос](https://en.wikipedia.org/wiki/Reverse_DNS_lookup), после чего к результату применяется указанное регулярное выражение. Затем для результатов PTR-запроса выполняется ещё один DNS-запрос, и все полученные адреса сравниваются с адресом этого узла. Мы настоятельно рекомендуем, чтобы регулярное выражение оканчивалось символом $.

Все результаты DNS-запросов кэшируются до перезапуска сервера.

**Примеры**

Чтобы открыть пользователю доступ из любой сети, укажите:

```xml
<ip>::/0</ip>
```

:::note
Открывать доступ из любой сети небезопасно, если только у вас не настроен должным образом межсетевой экран или сервер не подключён к интернету напрямую.
:::

Чтобы открыть доступ только с localhost, укажите:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

Пользователю можно назначить профиль настроек. Профили настроек настраиваются в отдельном разделе файла `users.xml`. Подробнее см. в разделе [Профили настроек](../../operations/settings/settings-profiles.md).

### user_name/quota

Квоты позволяют отслеживать и ограничивать использование ресурсов за определённый период времени. Квоты настраиваются в разделе `quotas`
конфигурационного файла `users.xml`.

Вы можете назначить пользователю набор квот. Подробное описание настройки квот см. в разделе [Квоты](/ru/operations/quotas).

### user_name/databases

В этом разделе можно ограничить строки, которые ClickHouse возвращает для запросов `SELECT`, выполняемых текущим пользователем, тем самым реализуя базовую защиту на уровне строки.

**Пример**

Следующая конфигурация задаёт, что пользователь `user1` может видеть в результатах запросов `SELECT` только строки таблицы `table1`, в которых значение поля `id` равно 1000.

```xml
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

`filter` может быть любым выражением, результат которого имеет тип [UInt8](../../sql-reference/data-types/int-uint.md). Обычно оно содержит сравнения и логические операторы. Строки из `database_name.table1`, для которых `filter` возвращает 0, этому пользователю не возвращаются. Фильтрация несовместима с операциями `PREWHERE` и отключает оптимизацию `WHERE→PREWHERE`.

## Роли

С помощью раздела `roles` в конфигурационном файле `user.xml` можно создать любые предопределённые роли.

Структура раздела `roles`:

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

Эти роли также можно выдать пользователям в разделе `users`:

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```