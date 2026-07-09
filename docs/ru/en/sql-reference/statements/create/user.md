---
description: 'Документация по команде USER'
sidebar_label: 'USER'
sidebar_position: 39
slug: /sql-reference/statements/create/user
title: 'CREATE USER'
doc_type: 'reference'
---

Создаёт [учётные записи пользователей](../../../guides/sre/user-management/index.md#user-account-management).

Синтаксис:

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime] 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [ROLE role [,...]]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

Предложение `ON CLUSTER` позволяет создавать пользователей в кластере, см. [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="identification">
  ## Идентификация
</div>

Существует несколько способов идентификации пользователя:

* `IDENTIFIED WITH no_password`
* `IDENTIFIED WITH plaintext_password BY 'qwerty'`
* `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
* `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
* `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
* `IDENTIFIED WITH double_sha1_hash BY 'hash'`
* `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
* `IDENTIFIED WITH bcrypt_hash BY 'hash'`
* `IDENTIFIED WITH ldap SERVER 'server_name'`
* `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`
* `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
* `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
* `IDENTIFIED WITH http SERVER 'http_server'` or `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
* `IDENTIFIED BY 'qwerty'`

Требования к сложности паролей можно изменить в [config.xml](/ru/operations/configuration-files). Ниже приведён пример конфигурации, в которой пароли должны быть длиной не менее 12 символов и содержать 1 цифру. Для каждого правила сложности пароля необходимо задать регулярное выражение для проверки паролей и описание самого правила.

```xml
<clickhouse>
    <password_complexity>
        <rule>
            <pattern>.{12}</pattern>
            <message>be at least 12 characters long</message>
        </rule>
        <rule>
            <pattern>\p{N}</pattern>
            <message>contain at least 1 numeric character</message>
        </rule>
    </password_complexity>
</clickhouse>
```

:::note
В ClickHouse Cloud пароли по умолчанию должны соответствовать следующим требованиям сложности:

* Быть длиной не менее 12 символов
* Содержать как минимум 1 цифру
* Содержать как минимум 1 заглавную букву
* Содержать как минимум 1 строчную букву
* Содержать как минимум 1 специальный символ
  :::

<div id="examples">
  ## Примеры
</div>

1. Имя пользователя `name1` не требует пароля, что, очевидно, не обеспечивает особой безопасности:

   ```sql
   CREATE USER name1 NOT IDENTIFIED
   ```

2. Чтобы указать пароль в открытом виде:

   ```sql
   CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
   ```

   :::tip
   Пароль хранится в текстовом SQL-файле в `/var/lib/clickhouse/access`, поэтому использовать `plaintext_password` — не лучшая идея. Вместо этого лучше использовать `sha256_password`, как показано далее...
   :::

3. Самый распространенный вариант — использовать пароль, хешированный с помощью SHA-256. ClickHouse сам вычислит хеш пароля, если указать `IDENTIFIED WITH sha256_password`. Например:

   ```sql
   CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
   ```

   Пользователь `name3` теперь может войти с паролем `my_password`, но сам пароль хранится в виде хеша. В `/var/lib/clickhouse/access` создается следующий SQL-файл, который выполняется при запуске сервера:

   ```bash
   /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
   ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
   ```

   :::tip
   Если у вас уже есть готовое хеш-значение и соответствующее значение SALT для имени пользователя, можно использовать `IDENTIFIED WITH sha256_hash BY 'hash'` или `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`. При идентификации с помощью `sha256_hash` с использованием `SALT` хеш должен вычисляться из конкатенации &#39;password&#39; и &#39;salt&#39;.
   :::

4. `double_sha1_password` обычно не нужен, но бывает полезен при работе с клиентами, которым он требуется (например, через интерфейс MySQL):

   ```sql
   CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
   ```

   ClickHouse генерирует и выполняет следующий запрос:

   ```response
   CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
   ```

5. `bcrypt_password` — самый безопасный вариант хранения паролей. Он использует алгоритм [bcrypt](https://en.wikipedia.org/wiki/Bcrypt), устойчивый к атакам перебором, даже если хеш пароля был скомпрометирован.

   ```sql
   CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
   ```

   При использовании этого метода длина пароля ограничена 72 символами.
   Параметр work factor для bcrypt, который определяет объем вычислений и время, необходимые для вычисления хеша и проверки пароля, можно изменить в конфигурации сервера:

   ```xml
   <bcrypt_workfactor>12</bcrypt_workfactor>
   ```

   Значение work factor должно быть от 4 до 31, значение по умолчанию — 12.

   :::warning
   Для приложений с частой аутентификацией
   рассмотрите альтернативные методы аутентификации из-за
   высокой вычислительной нагрузки bcrypt при больших значениях work factor.
   :::

6. Тип пароля также можно не указывать:

   ```sql
   CREATE USER name6 IDENTIFIED BY 'my_password'
   ```

   В этом случае ClickHouse будет использовать тип пароля по умолчанию, указанный в конфигурации сервера:

   ```xml
   <default_password_type>sha256_password</default_password_type>
   ```

   Доступные типы паролей: `plaintext_password`, `sha256_password`, `double_sha1_password`.

7. Можно указать несколько методов аутентификации:

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

Примечания:

1. Более старые версии ClickHouse могут не поддерживать синтаксис с несколькими методами аутентификации. Поэтому, если на сервере ClickHouse есть такие пользователи и версию сервера понизить до версии, которая это не поддерживает, такие пользователи станут недоступны, а некоторые операции, связанные с пользователями, перестанут работать. Чтобы корректно понизить версию, перед этим необходимо настроить для всех пользователей только один метод аутентификации. Либо, если версия сервера уже была понижена без соблюдения нужной процедуры, проблемных пользователей следует удалить.
2. `no_password` не может использоваться вместе с другими методами аутентификации из соображений безопасности. Поэтому указать
   `no_password` можно только в том случае, если это единственный метод аутентификации в запросе.

<div id="user-host">
  ## Хост пользователя
</div>

Хост пользователя — это хост, с которого может быть установлено соединение с сервером ClickHouse. Хост можно указать в секции запроса `HOST` следующими способами:

* `HOST IP 'ip_address_or_subnetwork'` — Пользователь может подключаться к серверу ClickHouse только с указанного IP-адреса или [подсети](https://en.wikipedia.org/wiki/Subnetwork). Примеры: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. Для использования в продакшне указывайте только элементы `HOST IP` (IP-адреса и их маски), так как использование `host` и `host_regexp` может вызывать дополнительную задержку.
* `HOST ANY` — Пользователь может подключаться с любого хоста. Это вариант по умолчанию.
* `HOST LOCAL` — Пользователь может подключаться только локально.
* `HOST NAME 'fqdn'` — Хост пользователя можно указать в виде FQDN. Например, `HOST NAME 'mysite.com'`.
* `HOST REGEXP 'regexp'` — При указании хостов пользователя можно использовать регулярные выражения [pcre](http://www.pcre.org/). Например, `HOST REGEXP '.*\.mysite\.com'`.
* `HOST LIKE 'template'` — Позволяет использовать оператор [LIKE](/ru/sql-reference/functions/string-search-functions#like) для фильтрации хостов пользователя. Например, `HOST LIKE '%'` эквивалентно `HOST ANY`, а `HOST LIKE '%.mysite.com'` фильтрует все хосты в домене `mysite.com`.

Ещё один способ указать хост — использовать синтаксис `@` после имени пользователя. Примеры:

* `CREATE USER mira@'127.0.0.1'` — Эквивалентно синтаксису `HOST IP`.
* `CREATE USER mira@'localhost'` — Эквивалентно синтаксису `HOST LOCAL`.
* `CREATE USER mira@'192.168.%.%'` — Эквивалентно синтаксису `HOST LIKE`.

:::tip
ClickHouse рассматривает `user_name@'address'` как имя пользователя целиком. Таким образом, технически можно создать нескольких пользователей с одинаковым `user_name` и разными конструкциями после `@`. Однако мы не рекомендуем так делать.
:::

<div id="valid-until-clause">
  ## Предложение VALID UNTIL
</div>

Позволяет указать дату истечения срока действия и, при необходимости, время для метода аутентификации. В качестве параметра принимает строку. Для даты и времени рекомендуется использовать формат `YYYY-MM-DD [hh:mm:ss] [timezone]`, где `[timezone]` должен быть числовым смещением, например `+09:00`, или одним из значений `UTC`, `GMT`, `Z`, `MSK`, `MSD`; именованные зоны IANA, такие как `Asia/Tokyo`, не распознаются (см. примечание ниже). По умолчанию этот параметр равен `'infinity'`.
Предложение `VALID UNTIL` можно указывать только вместе с методом аутентификации, за исключением случая, когда в запросе не указан ни один метод аутентификации. В этом случае предложение `VALID UNTIL` применяется ко всем существующим методам аутентификации.

Примеры:

* `CREATE USER name1 VALID UNTIL '2025-01-01'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
* `CREATE USER name1 VALID UNTIL 'infinity'`
* `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`

:::note
Строка даты и времени разбирается функцией `parseDateTimeBestEffort`, которая распознаёт только токены часового пояса `UTC`, `GMT`, `Z`, `MSK`, `MSD` и числовые смещения, такие как `+09:00` или `-05:00`. Именованные часовые пояса IANA, такие как `Asia/Tokyo` или `Europe/London`, не поддерживаются, а фиксированное смещение не эквивалентно зоне IANA для регионов, где действует переход на летнее время, поэтому необходимо вычислить правильное смещение для конкретной даты, которую вы кодируете.
:::

<div id="grantees-clause">
  ## Секция GRANTEES
</div>

Указывает пользователей или роли, которым этот пользователь может выдавать [привилегии](../../../sql-reference/statements/grant.md#privileges), при условии, что ему самому выданы все необходимые права с [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Варианты секции `GRANTEES`:

* `user` — Указывает пользователя, которому этот пользователь может выдавать привилегии.
* `role` — Указывает роль, которой этот пользователь может выдавать привилегии.
* `ANY` — Этот пользователь может выдавать привилегии кому угодно. Это значение используется по умолчанию.
* `NONE` — Этот пользователь не может выдавать привилегии никому.

Любого пользователя или роль можно исключить с помощью выражения `EXCEPT`. Например, `CREATE USER user1 GRANTEES ANY EXCEPT user2`. Это означает, что если `user1` выданы какие-либо привилегии с `GRANT OPTION`, он сможет выдавать эти привилегии кому угодно, кроме `user2`.

<div id="examples">
  ## Примеры
</div>

Создайте учетную запись пользователя `mira`, защищённую паролем `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira` должен запускать клиентское приложение на хосте, где работает сервер ClickHouse.

Создайте учётную запись пользователя `john` и назначьте ей роли:

```sql
CREATE USER john ROLE role1, role2;
```

Создайте учетную запись пользователя `john`, назначьте роли и сделайте некоторые из них ролями по умолчанию:

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE role1;
```

or

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE ALL EXCEPT role2;
```

Создайте учётную запись пользователя `john` и разрешите ему передавать свои привилегии пользователю с аккаунтом `jack`:

```sql
CREATE USER john GRANTEES jack;
```

Создайте учетную запись пользователя `john` с помощью параметра запроса:

```sql
SET param_user=john;
CREATE USER {user:Identifier};
```