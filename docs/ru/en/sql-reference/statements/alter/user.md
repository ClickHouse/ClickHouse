---
description: 'Документация по USER'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'справочник'
---

Изменяет учётные записи пользователей в ClickHouse.

Синтаксис:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

Чтобы использовать `ALTER USER`, у вас должна быть привилегия [ALTER USER](../../../sql-reference/statements/grant.md#access-management).

`SET variable = value` — это псевдоним для `MODIFY SETTING variable = value`: он изменяет один параметр, сохраняя остальные. Предпочтительнее использовать его (или `MODIFY SETTING`) вместо простого предложения `SETTINGS`, которая заменяет весь список настроек, а также удаляет все наследуемые (родительские) профили.

<div id="grantees-clause">
  ## Предложение GRANTEES
</div>

Указывает пользователей или роли, которым разрешено получать [привилегии](../../../sql-reference/statements/grant.md#privileges) от этого пользователя, при условии что ему также выданы все необходимые права доступа с [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Возможные варианты предложения `GRANTEES`:

* `user` — Указывает пользователя, которому этот пользователь может выдавать привилегии.
* `role` — Указывает роль, которой этот пользователь может выдавать привилегии.
* `ANY` — Этот пользователь может выдавать привилегии кому угодно. Это значение по умолчанию.
* `NONE` — Этот пользователь не может выдавать привилегии никому.

С помощью выражения `EXCEPT` можно исключить любого пользователя или роль. Например, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. Это означает, что если пользователю `user1` выданы какие-либо привилегии с `GRANT OPTION`, он сможет выдавать их кому угодно, кроме `user2`.

<div id="examples">
  ## Примеры
</div>

Установите назначенные роли по умолчанию:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

Если пользователю ранее не были назначены роли, ClickHouse генерирует исключение.

Сделайте все назначенные роли ролями по умолчанию:

```sql
ALTER USER user DEFAULT ROLE ALL
```

Если в будущем пользователю будет назначена роль, она автоматически станет ролью по умолчанию.

Сделайте роли по умолчанию из всех назначенных ролей, кроме `role1` и `role2`:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

Позволяет пользователю с аккаунтом `john` предоставлять свои привилегии пользователю с аккаунтом `jack`:

```sql
ALTER USER john GRANTEES jack;
```

Добавляет пользователю новые методы аутентификации, сохраняя при этом существующие:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Примечания:

1. Более старые версии ClickHouse могут не поддерживать синтаксис нескольких методов аутентификации. Поэтому, если на сервере ClickHouse есть такие пользователи, а версию сервера понижают до версии, которая этого не поддерживает, такие пользователи станут непригодными для использования, а некоторые операции, связанные с пользователями, перестанут работать. Чтобы корректно понизить версию, перед этим необходимо настроить для всех пользователей только один метод аутентификации. Если же версия сервера была понижена без соблюдения этой процедуры, проблемных пользователей следует удалить.
2. `no_password` не может использоваться вместе с другими методами аутентификации из соображений безопасности.
   Поэтому добавить метод аутентификации `no_password` с помощью `ADD` невозможно. Приведённый ниже запрос сгенерирует ошибку:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

Если вы хотите сбросить методы аутентификации для пользователя и использовать `no_password`, это нужно указать в приведённой ниже форме замены.

Сбрасывает методы аутентификации и добавляет те, которые указаны в запросе (эффект начального IDENTIFIED без ключевого слова ADD):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Сбросить методы аутентификации и сохранить последний из добавленных:

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## Предложение VALID UNTIL
</div>

Позволяет указать дату истечения срока действия и, при необходимости, время для метода аутентификации. В качестве параметра принимает строку. Для даты и времени рекомендуется использовать формат `YYYY-MM-DD [hh:mm:ss] [timezone]`. По умолчанию значение этого параметра — `'infinity'`.
Предложение `VALID UNTIL` можно указывать только вместе с методом аутентификации, за исключением случая, когда в запросе не указан ни один метод аутентификации. В этом случае предложение `VALID UNTIL` будет применено ко всем существующим методам аутентификации.

Примеры:

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`