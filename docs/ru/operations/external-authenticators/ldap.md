# LDAP {#external-authenticators-ldap}

Пользователи ClickHouse могут использовать сервер LDAP для аутентификации. Есть два варианта, как это можно осуществлять:

- использовать LDAP как внешний аутентификатор для уже существующих пользователей, определённых в `users.xml` или через SQL-воркфлоу.
- использовать LDAP как внешний каталог пользователей и разрешать аутентификацию для пользователей, не определённых локально в ClickHouse, но существующих на LDAP-сервере.

Для любого из этих подходов необходимо прописать в конфигурационных файлах ClickHouse сервер LDAP.

## Определение LDAP-сервера {#ldap-server-definition}

Для определения LDAP-сервера необходимо добавить секцию `ldap_servers` в конфигурационный файл `config.xml`, например:

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

Используя различные имена, можно определить несколько LDAP-серверов внутри секции `ldap_servers`.

Параметры:

- `host` - IP или имя хоста LDAP-сервера; этот параметр указывать обязательно, и он не может быть пустым.
- `port` - порт LDAP-сервера. Если `enable_tls` имеет значение `true`, то по умолчанию этот параметр равен `636`, а в противном случае - `389`.
- `bind_dn` - шаблон для построения DN (Distinguished Name, или Уникальное Имя).
    - Конечное DN получается заменой всех подстрок шаблона вида `{user_name}` актуальным именем пользователя во время каждой попытки аутентификации.
- `verification_cooldown` - временной интервал (в секундах) после успешной попытки подключения, в течение которого пользователь будет считаться аутентифицированным для всех последующих запросов, в это время повторное соединение с LDAP-сервером соверщаться не будет.
    - Укажите `0` (значение по умолчанию), чтобы отключить кэширование и принудить связываться с LDAP-сервером при каждой попытке аутентификации.
- `enable_tls` - флаг для указания необходимости использования защищённого соедниения с LDAP-сервером.
    - Укажите `no` для использования незашифрованного текстового протокола `ldap://` (не рекомендуется).
    - Укажите `yes` для использования LDAP через протокол SSL/TLS `ldaps://` (значение по умолчанию, рекомендуется).
    - Укажите `starttls` для использования устаревшего протокола StartTLS (незашифрованный `ldap://` с добавлением TLS).
- `tls_minimum_protocol_version` - требование к минимальной версии SSL/TLS.
    - Возможные значения: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (по умолчанию).
- `tls_require_cert` - проверка сертификата SSL/TLS.
    - Возможные значения: `never`, `allow`, `try`, `demand` (по умолчанию).
- `tls_cert_file` - путь к файлу сертификата.
- `tls_key_file` - путь к файлу ключа сертификата.
- `tls_ca_cert_file` - путь к файлу корневого сертификата (CA).
- `tls_ca_cert_dir` - путь к директории, содержащей корневые (CA) сертификаты.
- `tls_cipher_suite` - разрешённый набор шифров (в обозначении OpenSSL).

## LDAP как внешний аутентификатор {#ldap-external-authenticator}

Удалённый LDAP-сервер можно использовать для проверки паролей пользователей, определённых локально, в ClickHouse (в конфигурационных файлах или через SQL-воркфлоу). Для этого при определении пользователя укажите имя предварительно описанного LDAP-сервера вместо секции `password` или ей аналогичной.

При каждой попытке логина ClickHouse будет пытаться подключиться как DN, указанному в параметре `bind_dn` в [Определении сервера LDAP](#ldap-server-definition)
At each login attempt, ClickHouse will try to "bind" to the specified DN defined by the `bind_dn` parameter in the [LDAP server definition](#ldap-server-definition) using the provided credentials, and if successful, the user will be considered authenticated. This is often called a "simple bind" method.

For example,

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

Note, that user `my_user` refers to `my_ldap_server`. This LDAP server must be configured in the main `config.xml` file as described previously.

When SQL-driven [Access Control and Account Management](../access-rights.md#access-control) is enabled in ClickHouse, users that are authenticated by LDAP servers can also be created using the [CRATE USER](../../sql-reference/statements/create/user.md#create-user-statement) statement.


```sql
CREATE USER my_user IDENTIFIED WITH ldap_server BY 'my_ldap_server'
```

## LDAP Exernal User Directory {#ldap-external-user-directory}

In addition to the locally defined users, a remote LDAP server can be used as a source of user definitions. In order to achieve this, specify previously defined LDAP server name (see [LDAP Server Definition](#ldap-server-definition)) in the `ldap` section inside the `users_directories` section of the `config.xml` file.

At each login attempt, ClickHouse will try to find the user definition locally and authenticate it as usual, but if the user is not defined, ClickHouse will assume it exists in the external LDAP directory, and will try to "bind" to the specified DN at the LDAP server using the provided credentials. If successful, the user will be considered existing and authenticated. The user will be assigned roles from the list specified in the `roles` section. Additionally, LDAP "search" can be performed and results can be transformed and treated as role names and then be assigned to the user if the `role_mapping` section is also configured. All this implies that the SQL-driven [Access Control and Account Management](../access-rights.md#access-control) is enabled and roles are created using the [CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement) statement.

Example (goes into `config.xml`):

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

Note that `my_ldap_server` referred in the `ldap` section inside the `user_directories` section must be a previously
defined LDAP server that is configured in the `config.xml` (see [LDAP Server Definition](#ldap-server-definition)).

Parameters:

- `server` - one of LDAP server names defined in the `ldap_servers` config section above.
  This parameter is mandatory and cannot be empty.
- `roles` - section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server.
    - If no roles are specified here or assigned during role mapping (below), user will not be able
      to perform any actions after authentication.
- `role_mapping` - section with LDAP search parameters and mapping rules.
    - When a user authenticates, while still bound to LDAP, an LDAP search is performed using `search_filter`
      and the name of the logged in user. For each entry found during that search, the value of the specified
      attribute is extracted. For each attribute value that has the specified prefix, the prefix is removed,
      and the rest of the value becomes the name of a local role defined in ClickHouse,
      which is expected to be created beforehand by the [CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement) statement.
    - There can be multiple `role_mapping` sections defined inside the same `ldap` section. All of them will be applied.
        - `base_dn` - template used to construct the base DN for the LDAP search.
           - The resulting DN will be constructed by replacing all `{user_name}` and `{bind_dn}`
             substrings of the template with the actual user name and bind DN during each LDAP search.
        - `scope` - scope of the LDAP search.
            - Accepted values are: `base`, `one_level`, `children`, `subtree` (the default).
        - `search_filter` - template used to construct the search filter for the LDAP search.
            - The resulting filter will be constructed by replacing all `{user_name}`, `{bind_dn}`, and `{base_dn}`
              substrings of the template with the actual user name, bind DN, and base DN during each LDAP search.
            - Note, that the special characters must be escaped properly in XML.
        - `attribute` - attribute name whose values will be returned by the LDAP search.
        - `prefix` - prefix, that will be expected to be in front of each string in the original
          list of strings returned by the LDAP search. Prefix will be removed from the original
          strings and resulting strings will be treated as local role names. Empty, by default.
