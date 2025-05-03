---
slug: /ru/sql-reference/statements/create/user
sidebar_position: 39
sidebar_label: "Пользователь"
---

# CREATE USER {#create-user-statement}

Создает [аккаунты пользователей](../../../operations/access-rights.md#user-account-management).

Синтаксис:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` позволяет создавать пользователей в кластере, см. [Распределенные DDL](../../../sql-reference/distributed-ddl.md).

## Идентификация

Существует несколько способов идентификации пользователя:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`
- `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
- `IDENTIFIED WITH bcrypt_hash BY 'hash'`
- `IDENTIFIED WITH ldap SERVER 'server_name'`
- `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`
- `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
- `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
- `IDENTIFIED BY 'qwerty'`

Для идентификации с sha256_hash используя `SALT` - хэш должен быть вычислен от конкатенации 'password' и 'salt'.

## Пользовательский хост

Пользовательский хост — это хост, с которого можно установить соединение с сервером ClickHouse. Хост задается в секции `HOST` следующими способами:

-   `HOST IP 'ip_address_or_subnetwork'` — Пользователь может подключиться к серверу ClickHouse только с указанного IP-адреса или [подсети](https://ru.wikipedia.org/wiki/Подсеть). Примеры: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. При использовании в эксплуатации указывайте только элементы `HOST IP` (IP-адреса и маски подсети), так как использование `host` и `host_regexp` может привести к дополнительной задержке.
-   `HOST ANY` — Пользователь может подключиться с любого хоста. Используется по умолчанию.
-   `HOST LOCAL` — Пользователь может подключиться только локально.
-   `HOST NAME 'fqdn'` — Хост задается через FQDN. Например, `HOST NAME 'mysite.com'`.
-   `HOST REGEXP 'regexp'` — Позволяет использовать регулярные выражения [pcre](http://www.pcre.org/), чтобы задать хосты. Например, `HOST REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Позволяет использовать оператор [LIKE](../../functions/string-search-functions.md#function-like) для фильтрации хостов. Например, `HOST LIKE '%'` эквивалентен `HOST ANY`; `HOST LIKE '%.mysite.com'` разрешает подключение со всех хостов в домене `mysite.com`.

Также, чтобы задать хост, вы можете использовать `@` вместе с именем пользователя. Примеры:

-   `CREATE USER mira@'127.0.0.1'` — Эквивалентно `HOST IP`.
-   `CREATE USER mira@'localhost'` — Эквивалентно `HOST LOCAL`.
-   `CREATE USER mira@'192.168.%.%'` — Эквивалентно `HOST LIKE`.

:::info Внимание
ClickHouse трактует конструкцию `user_name@'address'` как имя пользователя целиком. То есть технически вы можете создать несколько пользователей с одинаковыми `user_name`, но разными частями конструкции после `@`, но лучше так не делать.
:::

## Секция GRANTEES {#grantees}

Указываются пользователи или роли, которым разрешено получать [привилегии](../../../sql-reference/statements/grant.md#grant-privileges) от создаваемого пользователя при условии, что этому пользователю также предоставлен весь необходимый доступ с использованием [GRANT OPTION](../../../sql-reference/statements/grant.md#grant-privigele-syntax). Параметры секции `GRANTEES`:

-   `user` — указывается пользователь, которому разрешено получать привилегии от создаваемого пользователя.
-   `role` — указывается роль, которой разрешено получать привилегии от создаваемого пользователя.
-   `ANY` — любому пользователю или любой роли разрешено получать привилегии от создаваемого пользователя. Используется по умолчанию.
-   `NONE` — никому не разрешено получать привилегии от создаваемого пользователя.

Вы можете исключить любого пользователя или роль, используя выражение `EXCEPT`. Например, `CREATE USER user1 GRANTEES ANY EXCEPT user2`. Это означает, что если `user1` имеет привилегии, предоставленные с использованием `GRANT OPTION`, он сможет предоставить их любому, кроме `user2`.

## Примеры {#create-user-examples}

Создать аккаунт `mira`, защищенный паролем `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

Пользователь `mira` должен запустить клиентское приложение на хосте, где запущен ClickHouse.

Создать аккаунт `john`, назначить на него роли, сделать данные роли ролями по умолчанию:

``` sql
CREATE USER john DEFAULT ROLE role1, role2;
```

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли:

``` sql
CREATE USER john DEFAULT ROLE ALL;
```

Когда роль будет назначена аккаунту `john`, она автоматически станет ролью по умолчанию.

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли, кроме `role1` и `role2`:

``` sql
CREATE USER john DEFAULT ROLE ALL EXCEPT role1, role2;
```

Создать пользователя с аккаунтом `john` и разрешить ему предоставить свои привилегии пользователю с аккаунтом `jack`:

``` sql
CREATE USER john GRANTEES jack;
```
