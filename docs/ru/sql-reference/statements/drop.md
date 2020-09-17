---
toc_priority: 46
toc_title: DROP
---

# DROP {#drop}

Удаляет существующий объект. 
Если указано `IF EXISTS` - не выдавать ошибку, если объекта не существует.

## DROP DATABASE {#drop-database}

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Удаляет все таблицы в базе данных db, затем удаляет саму базу данных db.


## DROP TABLE {#drop-table}

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет таблицу.


## DROP DICTIONARY {#drop-dictionary}

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name
```

Удаляет словарь.


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


## DROP VIEW {#drop-view}

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет представление. Представления могут быть удалены и командой `DROP TABLE`, но команда `DROP VIEW` проверяет, что `[db.]name` является представлением.


[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/drop/) <!--hide-->
