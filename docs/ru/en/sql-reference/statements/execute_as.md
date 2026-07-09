---
description: 'Документация по оператору EXECUTE AS'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'Оператор EXECUTE AS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # Оператор EXECUTE AS
</div>

Позволяет выполнять запросы от имени другого пользователя.

<div id="syntax">
  ## Синтаксис
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

Первая форма (без `subquery`) задаёт, что все последующие запросы в текущем сеансе будут выполняться от имени указанного `target_user`.

Вторая форма (с `subquery`) выполняет только указанный `subquery` от имени указанного `target_user`.

Для работы обеих форм требуется, чтобы параметр конфигурации `access_control_improvements.allow_impersonate_user`
был установлен в `1`, а привилегия `IMPERSONATE` — выдана. Например, следующие команды

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

разрешить пользователю `user2` выполнять команды `EXECUTE AS user1 ...`, а также разрешить пользователю `user3` выполнять команды от имени любого пользователя.

При выполнении команд от имени другого пользователя функция [currentUser()](/ru/sql-reference/functions/other-functions#currentUser) возвращает имя этого пользователя,
а функция [authenticatedUser()](/ru/sql-reference/functions/other-functions#authenticatedUser) возвращает имя пользователя, который действительно прошёл аутентификацию.

<div id="examples">
  ## Примеры
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```