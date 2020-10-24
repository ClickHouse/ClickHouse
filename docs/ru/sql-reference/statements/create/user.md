---
toc_priority: 39
toc_title: "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c"
---

# CREATE USER {#create-user-statement}

Создает [аккаунт пользователя](../../../operations/access-rights.md#user-account-management).

### Синтаксис {#create-user-syntax}

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Идентификация

Существует несколько способов идентификации пользователя:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### Пользовательский хост

Пользовательский хост — это хост, с которого можно установить соединение с сервером ClickHouse. Хост задается в секции `HOST` следующими способами:

- `HOST IP 'ip_address_or_subnetwork'` — Пользователь может подключиться к серверу ClickHouse только с указанного IP-адреса или [подсети](https://ru.wikipedia.org/wiki/Подсеть). Примеры: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. При использовании в эксплуатации указывайте только элементы `HOST IP` (IP-адреса и маски подсети), так как использование `host` и `host_regexp` может привести к дополнительной задержке.
- `HOST ANY` — Пользователь может подключиться с любого хоста. Используется по умолчанию.
- `HOST LOCAL` — Пользователь может подключиться только локально.
- `HOST NAME 'fqdn'` — Хост задается через FQDN. Например, `HOST NAME 'mysite.com'`.
- `HOST NAME REGEXP 'regexp'` — Позволяет использовать регулярные выражения [pcre](http://www.pcre.org/), чтобы задать хосты. Например, `HOST NAME REGEXP '.*\.mysite\.com'`.
- `HOST LIKE 'template'` — Позволяет использовать оператор [LIKE](../../functions/string-search-functions.md#function-like) для фильтрации хостов. Например, `HOST LIKE '%'` эквивалентен `HOST ANY`; `HOST LIKE '%.mysite.com'` разрешает подключение со всех хостов в домене `mysite.com`.

Также, чтобы задать хост, вы можете использовать `@` вместе с именем пользователя. Примеры:

- `CREATE USER mira@'127.0.0.1'` — Эквивалентно `HOST IP`.
- `CREATE USER mira@'localhost'` — Эквивалентно `HOST LOCAL`.
- `CREATE USER mira@'192.168.%.%'` — Эквивалентно `HOST LIKE`.

!!! info "Внимание"
    ClickHouse трактует конструкцию `user_name@'address'` как имя пользователя целиком. То есть технически вы можете создать несколько пользователей с одинаковыми `user_name`, но разными частями конструкции после `@`, но лучше так не делать.


### Примеры {#create-user-examples}


Создать аккаунт `mira`, защищенный паролем `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

Пользователь `mira` должен запустить клиентское приложение на хосте, где запущен ClickHouse.

Создать аккаунт `john`, назначить на него роли, сделать данные роли ролями по умолчанию:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Когда роль будет назначена аккаунту `john`, она автоматически станет ролью по умолчанию.

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли, кроме `role1` и `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/user) 
<!--hide-->