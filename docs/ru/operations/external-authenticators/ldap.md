# LDAP {#external-authenticators-ldap}

Для аутентификации пользователей ClickHouse можно использовать сервер LDAP. Существуют два подхода:

- Использовать LDAP как внешний аутентификатор для существующих пользователей, которые определены в `users.xml`, или в локальных параметрах управления доступом.
- Использовать LDAP как внешний пользовательский каталог и разрешить аутентификацию локально неопределенных пользователей, если они есть на LDAP сервере.

Для обоих подходов необходимо определить внутреннее имя LDAP сервера в конфигурации ClickHouse, чтобы другие параметры конфигурации могли ссылаться на это имя.

## Определение LDAP сервера {#ldap-server-definition}

Чтобы определить LDAP сервер, необходимо добавить секцию `ldap_servers` в `config.xml`.

**Пример**

```xml
<yandex>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
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
		
        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</yandex>
```

Обратите внимание, что можно определить несколько LDAP серверов внутри секции `ldap_servers`, используя различные имена.

**Параметры**

- `host` — имя хоста сервера LDAP или его IP. Этот параметр обязательный и не может быть пустым.
- `port` — порт сервера LDAP. Если настройка `enable_tls` равна `true`, то по умолчанию используется порт `636`, иначе — порт `389`.
- `bind_dn` — шаблон для создания DN подключения.
    - При формировании DN все подстроки `{user_name}` в шаблоне будут заменяться на фактическое имя пользователя при каждой попытке аутентификации.
- `user_dn_detection` — секция с параметрами LDAP поиска для определения фактического значения DN подключенного пользователя.
    - Это в основном используется в фильтрах поиска для дальнейшего сопоставления ролей, когда сервер является Active Directory. Полученный DN пользователя будет использоваться при замене подстрок `{user_dn}` везде, где они разрешены. По умолчанию DN пользователя устанавливается равным DN подключения, но после выполнения поиска он будет обновлен до фактического найденного значения DN пользователя.
        - `base_dn` — шаблон для создания базового DN для LDAP поиска.
            - При формировании DN все подстроки `{user_name}` и `{bind_dn}` в шаблоне будут заменяться на фактическое имя пользователя и DN подключения соответственно при каждом LDAP поиске.
        - `scope` — область LDAP поиска.
            - Возможные значения: `base`, `one_level`, `children`, `subtree` (по умолчанию).
        - `search_filter` — шаблон для создания фильтра для каждого LDAP поиска.
            - При формировании фильтра все подстроки `{user_name}`, `{bind_dn}`, `{user_dn}` и `{base_dn}` в шаблоне будут заменяться на фактическое имя пользователя, DN подключения, DN пользователя и базовый DN соответственно при каждом LDAP поиске.
            - Обратите внимание, что специальные символы должны быть правильно экранированы в XML.
- `verification_cooldown` — промежуток времени (в секундах) после успешной попытки подключения, в течение которого пользователь будет считаться аутентифицированным и сможет выполнять запросы без повторного обращения к серверам LDAP.
    - Чтобы отключить кеширование и заставить обращаться к серверу LDAP для каждого запроса аутентификации, укажите `0` (значение по умолчанию). 
- `enable_tls` — флаг, включающий использование защищенного соединения с сервером LDAP.
    - Укажите `no` для использования текстового протокола `ldap://` (не рекомендовано).
    - Укажите `yes` для обращения к LDAP по протоколу SSL/TLS `ldaps://` (рекомендовано, используется по умолчанию).
    - Укажите `starttls` для использования устаревшего протокола StartTLS (текстовый `ldap://` протокол, модернизированный до TLS).
- `tls_minimum_protocol_version` — минимальная версия протокола SSL/TLS.
    - Возможные значения: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (по-умолчанию).
- `tls_require_cert` —  поведение при проверке сертификата SSL/TLS.
    - Возможные значения: `never`, `allow`, `try`, `demand` (по-умолчанию).
- `tls_cert_file` — путь к файлу сертификата.
- `tls_key_file` — путь к файлу ключа сертификата.
- `tls_ca_cert_file` — путь к файлу ЦС (certification authority) сертификата.
- `tls_ca_cert_dir` — путь к каталогу, содержащему сертификаты ЦС. 
- `tls_cipher_suite` — разрешенный набор шифров (в нотации OpenSSL).

## Внешний аутентификатор LDAP {#ldap-external-authenticator}

Удаленный сервер LDAP можно использовать для верификации паролей локально определенных пользователей (пользователей, которые определены в `users.xml` или в локальных параметрах управления доступом). Для этого укажите имя определенного ранее сервера LDAP вместо `password` или другой аналогичной секции в настройках пользователя.

При каждой попытке авторизации ClickHouse пытается "привязаться" к DN, указанному в [определении LDAP сервера](#ldap-server-definition), используя параметр `bind_dn` и предоставленные реквизиты для входа. Если попытка оказалась успешной, пользователь считается аутентифицированным. Обычно это называют методом "простой привязки".

**Пример**

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

При включенном SQL-ориентированном [управлении доступом](../access-rights.md#access-control) пользователи, аутентифицированные LDAP серверами, могут также быть созданы запросом [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement).

Запрос:

```sql
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

## Внешний пользовательский каталог LDAP {#ldap-external-user-directory}

В дополнение к локально определенным пользователям, удаленный LDAP сервер может служить источником определения пользователей. Для этого укажите имя определенного ранее сервера LDAP (см. [Определение LDAP сервера](#ldap-server-definition)) в секции `ldap` внутри секции `users_directories` файла `config.xml`.

При каждой попытке аутентификации ClickHouse пытается локально найти определение пользователя и аутентифицировать его как обычно. Если пользователь не находится локально, ClickHouse предполагает, что он определяется во внешнем LDAP каталоге и пытается "привязаться" к DN, указанному на LDAP сервере, используя предоставленные реквизиты для входа. Если попытка оказалась успешной, пользователь считается существующим и  аутентифицированным. Пользователю присваиваются роли из списка, указанного в секции `roles`. Кроме того, если настроена секция `role_mapping`, то выполняется LDAP поиск, а его результаты преобразуются в имена ролей и присваиваются пользователям. Все это работает при условии, что SQL-ориентированное [управлением доступом](../access-rights.md#access-control) включено, а роли созданы запросом [CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement).

**Пример**

В `config.xml`.

```xml
<yandex>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
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
		
        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</yandex>
```

Обратите внимание, что `my_ldap_server`, указанный в секции `ldap` внутри секции `user_directories`, должен быть настроен в файле `config.xml`, как это было описано ранее. (см. [Определение LDAP сервера](#ldap-server-definition)).

**Параметры**

- `server` — имя одного из серверов LDAP, определенных в секции `ldap_servers` в файле конфигурации (см.выше). Этот параметр обязательный и не может быть пустым.
- `roles` — секция со списком локально определенных ролей, которые будут присвоены каждому пользователю, полученному от сервера LDAP.
    - Если роли не указаны ни здесь, ни в секции `role_mapping` (см. ниже), пользователь после аутентификации не сможет выполнять никаких действий.
- `role_mapping` — секция c параметрами LDAP поиска и правилами отображения.
    - При аутентификации пользователя, пока еще связанного с LDAP, производится LDAP поиск с помощью `search_filter` и имени этого пользователя. Для каждой записи, найденной в ходе поиска, выделяется значение указанного атрибута. У каждого атрибута, имеющего указанный префикс, этот префикс удаляется, а остальная часть значения становится именем локальной роли, определенной в ClickHouse, причем предполагается, что эта роль была ранее создана запросом [CREATE ROLE](../../sql-reference/statements/create/role.md#create-role-statement) до этого.
    - Внутри одной секции `ldap` может быть несколько секций `role_mapping`. Все они будут применены.
        - `base_dn` — шаблон для создания базового DN для LDAP поиска.
            - При формировании DN все подстроки `{user_name}`, `{bind_dn}` и `{user_dn}` в шаблоне будут заменяться на фактическое имя пользователя, DN подключения и DN пользователя соответственно при каждом LDAP поиске.
        - `scope` — область LDAP поиска.
            - Возможные значения: `base`, `one_level`, `children`, `subtree` (по умолчанию).
        - `search_filter` — шаблон для создания фильтра для каждого LDAP поиска.
            - При формировании фильтра все подстроки `{user_name}`, `{bind_dn}`, `{user_dn}` и `{base_dn}` в шаблоне будут заменяться на фактическое имя пользователя, DN подключения, DN пользователя и базовый DN соответственно при каждом LDAP поиске.
            - Обратите внимание, что специальные символы должны быть правильно экранированы в XML.
        - `attribute` — имя атрибута, значение которого будет возвращаться LDAP поиском. По умолчанию: `cn`.
        - `prefix` — префикс, который, как предполагается, будет находиться перед началом каждой строки в исходном списке строк, возвращаемых LDAP поиском. Префикс будет удален из исходных строк, а сами они будут рассматриваться как имена локальных ролей. По умолчанию: пустая строка. 

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/external-authenticators/ldap) <!--hide-->
