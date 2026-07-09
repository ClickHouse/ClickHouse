---
description: 'Документация по оператору RENAME'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'Оператор RENAME'
doc_type: 'reference'
---

Переименовывает базы данных, таблицы или словари. В одном запросе можно переименовать несколько сущностей.
Обратите внимание, что запрос `RENAME` для нескольких сущностей не является атомарной операцией. Чтобы атомарно поменять местами имена сущностей, используйте оператор [EXCHANGE](./exchange.md).

**Синтаксис**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

Переименовывает базы данных.

**Синтаксис**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

Переименовывает одну или несколько таблиц.

Переименование таблиц — лёгкая операция. Если после `TO` указать другую базу данных, таблица будет перемещена в неё. Однако каталоги баз данных должны находиться в одной и той же файловой системе. В противном случае возвращается ошибка.
Если переименовывать несколько таблиц в одном запросе, операция не является атомарной. Она может быть выполнена частично, и запросы в других сеансах могут получить ошибку `Table ... does not exist ...`.

**Синтаксис**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Пример**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

И можно использовать более простой SQL:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

Переименовывает один или несколько словарей. Этот запрос также можно использовать для перемещения словарей между базами данных.

**Синтаксис**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**См. также**

* [Словари](./create/dictionary/overview.md)