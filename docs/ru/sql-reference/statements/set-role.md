---
sidebar_position: 50
sidebar_label: SET ROLE
---

# SET ROLE {#set-role-statement}

Активирует роли для текущего пользователя.

### Синтаксис {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Устанавливает роли по умолчанию для пользователя.

Роли по умолчанию активируются автоматически при входе пользователя. Ролями по умолчанию могут быть установлены только ранее назначенные роли. Если роль не назначена пользователю, ClickHouse выбрасывает исключение.


### Синтаксис {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```


### Примеры {#set-default-role-examples}

Установить несколько ролей по умолчанию для пользователя:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Установить ролями по умолчанию все назначенные пользователю роли:

``` sql
SET DEFAULT ROLE ALL TO user
```

Удалить роли по умолчанию для пользователя:

``` sql
SET DEFAULT ROLE NONE TO user
```

Установить ролями по умолчанию все назначенные пользователю роли за исключением указанных:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```


