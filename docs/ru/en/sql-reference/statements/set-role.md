---
description: 'Документация по SET ROLE'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'Оператор SET ROLE'
doc_type: 'reference'
---

Активирует роли текущего пользователя.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

Устанавливает для пользователя роли по умолчанию.

Роли по умолчанию автоматически активируются при входе пользователя в систему. В качестве ролей по умолчанию можно задать только ранее предоставленные роли. Если роль не выдана пользователю, ClickHouse генерирует исключение.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## Примеры
</div>

Назначьте пользователю несколько ролей по умолчанию:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Назначить пользователю все предоставленные роли ролями по умолчанию:

```sql
SET DEFAULT ROLE ALL TO user
```

Удалите у пользователя роли по умолчанию:

```sql
SET DEFAULT ROLE NONE TO user
```

Установите все предоставленные роли как роли по умолчанию, кроме `role1` и `role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```