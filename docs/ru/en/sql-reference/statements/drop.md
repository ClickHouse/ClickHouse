---
description: 'Документация по командам DROP'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'Команды DROP'
doc_type: 'reference'
---

Удаляет существующий объект. Если указан оператор `IF EXISTS`, запрос не возвращает ошибку, если объект не существует. Если указан модификатор `SYNC`, объект удаляется без задержки.

<div id="drop-database">
  ## DROP DATABASE
</div>

Удаляет все таблицы в базе данных `db`, а затем саму базу данных `db`.

Синтаксис:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

Удаляет одну или несколько таблиц.

:::tip
Чтобы отменить удаление таблицы, см. [UNDROP TABLE](/ru/sql-reference/statements/undrop.md)
:::

Синтаксис:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Ограничения:

* Если указано `IF EMPTY`, сервер проверяет, пуста ли таблица, только на реплике, получившей запрос.
* Удаление нескольких таблиц сразу не является атомарной операцией: если удаление одной из таблиц завершится ошибкой, последующие таблицы удалены не будут.

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

Удаляет словарь.

Синтаксис:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

Удаляет пользователя.

Синтаксис:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

Удаляет роль. После удаления роль отзывается у всех сущностей, которым она была назначена.

Синтаксис:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

Удаляет политику ROW POLICY. Удалённая политика ROW POLICY отзывается у всех сущностей, которым она была назначена.

Синтаксис:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

Удаляет политику маскирования.

Синтаксис:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

Удаляет квоту. Удалённая квота отзывается у всех сущностей, которым она была назначена.

Синтаксис:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

Удаляет профиль настроек. Удалённый профиль настроек будет отозван для всех сущностей, которым он был назначен.

Синтаксис:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

Удаляет представление. Представления также можно удалить командой `DROP TABLE`, но `DROP VIEW` проверяет, что `[db.]name` — это представление.

Синтаксис:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

Удаляет определяемую пользователем функцию, созданную с помощью [CREATE FUNCTION](./create/function.md).
Системные функции удалить нельзя.

**Синтаксис**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Пример**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

Удаляет именованную коллекцию.

**Синтаксис**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Пример**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```