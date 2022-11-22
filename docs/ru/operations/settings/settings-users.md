---
sidebar_position: 63
sidebar_label: "Настройки пользователей"
---

# Настройки пользователей {#nastroiki-polzovatelei}

Раздел `users` конфигурационного файла `user.xml` содержит настройки для пользователей.

    :::note "Информация"
    Для управления пользователями рекомендуется использовать [SQL-ориентированный воркфлоу](../access-rights.md#access-control), который также поддерживается в ClickHouse.
    :::
Структура раздела `users`:

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
        <default_database>default<default_database>
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

### user_name/password {#user-namepassword}

Пароль можно указать в текстовом виде или в виде SHA256 (шестнадцатеричный формат).

-   Чтобы назначить пароль в текстовом виде (**не рекомендуем**), поместите его в элемент `password`.

        Например, `<password>qwerty</password>`. Пароль можно оставить пустым.

<a id="password_sha256_hex"></a>

-   Чтобы назначить пароль в виде SHA256, поместите хэш в элемент `password_sha256_hex`.

        Например, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

        Пример создания пароля в командной строке:

        ```
        PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
        ```

        Первая строка результата — пароль. Вторая строка — соответствующий ему хэш SHA256.

<a id="password_double_sha1_hex"></a>

-   Для совместимости с клиентами MySQL, пароль можно задать с помощью двойного хэша SHA1, поместив его в элемент `password_double_sha1_hex`.

        Например, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

        Пример создания пароля в командной строке:

        ```
        PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
        ```

        Первая строка результата — пароль. Вторая строка — соответствующий ему двойной хэш SHA1.

### access_management {#access_management-user-setting}

Включает или выключает SQL-ориентированное [управление доступом](../access-rights.md#access-control) для пользователя.

Возможные значения:

- 0 — Выключено.
- 1 — Включено.

Значение по умолчанию: 0.

### user_name/networks {#user-namenetworks}

Список сетей, из которых пользователь может подключиться к серверу ClickHouse.

Каждый элемент списка имеет одну из следующих форм:

-   `<ip>` — IP-адрес или маска подсети.

        Примеры: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Имя хоста.

        Пример: `example01.host.ru`.

        Для проверки доступа выполняется DNS-запрос, и все возвращенные IP-адреса сравниваются с адресом клиента.

-   `<host_regexp>` — Регулярное выражение для имен хостов.

        Пример, `^example\d\d-\d\d-\d\.host\.ru$`

        Для проверки доступа выполняется [DNS запрос PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) для адреса клиента, а затем применяется заданное регулярное выражение. Затем, для результатов запроса PTR выполняется другой DNS-запрос и все полученные адреса сравниваются с адресом клиента. Рекомендуем завершать регулярное выражение символом $.

Все результаты DNS-запросов кэшируются до перезапуска сервера.

**Примеры**

Чтобы открыть доступ пользователю из любой сети, укажите:

``` xml
<ip>::/0</ip>
```

:::danger "Внимание"
    Открывать доступ из любой сети небезопасно, если у вас нет правильно настроенного брандмауэра или сервер не отключен от интернета.

Чтобы открыть только локальный доступ, укажите:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile {#user-nameprofile}

Пользователю можно назначить профиль настроек. Профили настроек конфигурируются в отдельной секции файла `users.xml`. Подробнее читайте в разделе [Профили настроек](settings-profiles.md).

### user_name/quota {#user-namequota}

Квотирование позволяет отслеживать или ограничивать использование ресурсов в течение определённого периода времени. Квоты настраиваются в разделе `quotas` конфигурационного файла `users.xml`.

Пользователю можно назначить квоты. Подробное описание настройки квот смотрите в разделе [Квоты](../quotas.md#quotas).

### user_name/databases {#user-namedatabases}

В этом разделе вы можете ограничить выдачу ClickHouse запросами `SELECT` для конкретного пользователя, таким образом реализуя базовую защиту на уровне строк.

**Пример**

Следующая конфигурация задаёт, что пользователь `user1` в результате запросов `SELECT` может получать только те строки `table1`, в которых значение поля `id` равно 1000.

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

Элемент `filter` содержать любое выражение, возвращающее значение типа [UInt8](../../sql-reference/data-types/int-uint.md). Обычно он содержит сравнения и логические операторы. Строки `database_name.table1`, для которых фильтр возвращает 0 не выдаются пользователю. Фильтрация несовместима с операциями `PREWHERE` и отключает оптимизацию `WHERE→PREWHERE`.

