# LDAP {#external-authenticators-ldap} 

Для аутентификации пользователей ClickHouse можно использовать сервер LDAP. Можно использовать два подхода:

- Использовать LDAP как внешний аутентификатор для существующих пользователей, которые определены в `users.xml` или в локальных путях управления контролем.
- Использовать LDAP как внешний пользовательский каталог и разрешить аутентификацию локально неопределенных пользователей, если они есть на LDAP сервере.

Для обоих подходов необходимо определить в конфиге ClickHouse внутренне названный LDAP сервер, чтобы другие части конфига могли ссылаться на него.

## Определение LDAP сервера {#ldap-server-definition}

Чтобы определить LDAP сервер, необходимо добавить секцию `ldap_servers` в `config.xml`. Например:

```xml
<yandex>
    <!- ... -->
    <ldap_servers>
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>
    </ldap_servers>
</yandex>
```

Обратите внимание, что можно определить несколько LDAP серверов внутри секции `ldap_servers` используя различные имена.

**Параметры**

- `host` — имя хоста сервера LDAP или его IP. Этот параметр обязательный и не может быть пустым.
- `port` — порт сервера LDAP. По-умолчанию: при значении `true` настройки `enable_tls` — `636`, иначе `389`.
- `bind_dn` — шаблон для создания DN для привязки.
    - конечный DN будет создан заменой всех подстрок `{user_name}` шаблона на фактическое имя пользователя при каждой попытке аутентификации.
- `verification_cooldown` — промежуток времени (в секундах) после успешной попытки привязки, в течение которого пользователь будет считаться успешно аутентифицированным без с сервером LDAP для всех последующих запросов.
    - Укажите `0` (по-умолчанию), чтобы отключить кеширования и заставить связываться с сервером LDAP для каждого запроса аутетификации. 
- `enable_tls` — флаг, включающий использование защищенного соединения с сервером LDAP.
    - Укажите `no` для текстового `ldap://` протокола (не рекомендовано).
    - Укажите `yes` для LDAP через SSL/TLS `ldaps://` протокола (рекомендовано, используется по-умолчанию).
    - Укажите `starttls` для устаревшего StartTLS протокола (текстовый `ldap://` протокол, модернизированный до TLS).
- `tls_minimum_protocol_version` — минимальная версия протокола SSL/TLS.
    - Принимаемые значения: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (по-умолчанию).
- `tls_require_cert` —  поведение при проверке сертификата SSL/TLS.
    - Принимаемые значения: `never`, `allow`, `try`, `demand` (по-умолчанию).
- `tls_cert_file` — путь до файла сертификата.
- `tls_key_file` — путь к файлу ключа сертификата.
- `tls_ca_cert_file` — путь к файлу ЦС сертификата.
- `tls_ca_cert_dir` — путь к каталогу, содержащая сертификаты ЦС. 
- `tls_cipher_suite` — разрешить набор шифров (в нотации OpenSSL).

## LDAP внешний аутентификатор {#ldap-external-authenticator}

Удаленный сервер LDAP можно использовать как метод верификации паролей локально определенных пользователей (пользователей, которые определены в `users.xml` или в локальных путях управления контролем). Для этого укажите имя определенного до этого сервера LDAP вместо `password` или другой похожей секции в определении пользователя.

При каждой попытке авторизации, ClickHouse пытается "привязаться" к DN, указанному в [определение LDAP сервера](#ldap-server-definition) параметром `bind_dn`, используя предоставленные реквизиты для входа. Если попытка оказалась успешной, пользователь считается аутентифицированным. Обычно это называют методом "простой привязки".

**Например**

```xml
<yandex>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</yandex>
```

Обратите внимание, что пользователь `my_user` ссылается на `my_ldap_server`. Этот LDAP сервер должен быть настроен в основном файле `config.xml`, как это было описано ранее.

При включенном SQL-ориентированным [Управлением доступом](../access-rights.md#access-control) пользователи, аутентифицированные LDAP серверами, могут также быть созданы выражением [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement).

Запрос:

```sql
CREATE USER my_user IDENTIFIED WITH ldap_server BY 'my_ldap_server';
```

## Внешний пользовательский каталог LDAP {#ldap-external-user-directory}

В добавок к локально определенным пользователям, удаленный LDAP сервер может быть использован как источник определения пользователей. Для этого укажите имя определенного до этого сервера LDAP (см. [Определение LDAP сервера](#ldap-server-definition)) в секции `ldap` внутри секции `users_directories` файла `config.xml`.

При каждой попытке авторизации, ClicHouse пытается локально найти определение пользователя и авторизовать его как обычно. Если определение не будет найдено, ClickHouse предполагает, что оно находится во внешнем LDAP каталоге, и попытается "привязаться" к DN, указанному на LDAP сервере, используя предоставленные реквизиты для входа. Если попытка оказалась успешной, пользователь считается существующим и  аутентифицированным. Пользователю будут присвоены роли из списка, указанного в секции `roles`. Кроме того, может быть выполнен LDAP поиск, а его результаты могут быть преобразованы в имена ролей и присвоены пользователям, если была настроена секция `role_mapping`. Все это работает при условии, что SQL-ориентированное [Управлением доступом](../access-rights.md#access-control) включено, а роли созданы выражением[CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement).

**Пример**

В `config.xml`.

```xml
<yandex>
    <!- ... -->
    <user_directories>
        <!- ... -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</yandex>
```

Обратите внимание, что `my_ldap_server`, указанный в секции `ldap` внутри секции `user_directories`, должен быть настроен в файле `config.xml`, как это было описано ранее. (см. [Определение LDAP сервера](#ldap-server-definition)).

**Параметры**

- `server` — One of LDAP server names defined in the `ldap_servers` config section above.
  This parameter is mandatory and cannot be empty.
- `roles` — Section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server.
    - If no roles are specified here or assigned during role mapping (below), user will not be able to perform any actions after authentication.
- `role_mapping` — Section with LDAP search parameters and mapping rules.
    - When a user authenticates, while still bound to LDAP, an LDAP search is performed using `search_filter` and the name of the logged in user. For each entry found during that search, the value of the specified attribute is extracted. For each attribute value that has the specified prefix, the prefix is removed, and the rest of the value becomes the name of a local role defined in ClickHouse, which is expected to be created beforehand by the [CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement) statement.
    - There can be multiple `role_mapping` sections defined inside the same `ldap` section. All of them will be applied.
        - `base_dn` — Template used to construct the base DN for the LDAP search.
           - The resulting DN will be constructed by replacing all `{user_name}` and `{bind_dn}` substrings of the template with the actual user name and bind DN during each LDAP search.
        - `scope` — Scope of the LDAP search.
            - Accepted values are: `base`, `one_level`, `children`, `subtree` (the default).
        - `search_filter` — Template used to construct the search filter for the LDAP search.
            - The resulting filter will be constructed by replacing all `{user_name}`, `{bind_dn}`, and `{base_dn}` substrings of the template with the actual user name, bind DN, and base DN during each LDAP search.
            - Note, that the special characters must be escaped properly in XML.
        - `attribute` — Attribute name whose values will be returned by the LDAP search.
        - `prefix` — Prefix, that will be expected to be in front of each string in the original list of strings returned by the LDAP search. Prefix will be removed from the original strings and resulting strings will be treated as local role names. Empty, by default.


[Original article](https://clickhouse.tech/docs/en/operations/external-authenticators/ldap.md) <!--hide-->
