---
toc_priority: 46
toc_title: DROP
---

# DROP {#drop}

Запрос имеет два вида: `DROP DATABASE` и `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет таблицу.
Если указано `IF EXISTS` - не выдавать ошибку, если таблица не существует или база данных не существует.

## DROP USER {#drop-user-statement}

Удаляет пользователя.

### Синтаксис {#drop-user-syntax}

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP ROLE {#drop-role-statement}

Удаляет роль.

При удалении роль отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-role-syntax}

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

Удаляет политику доступа к строкам.

При удалении политика отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```


## DROP QUOTA {#drop-quota-statement}

Удаляет квоту.

При удалении квота отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

Удаляет профиль настроек.

При удалении профиль отзывается у всех объектов системы доступа, которым он присвоен.

### Синтаксис {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```



[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/drop/) <!--hide-->