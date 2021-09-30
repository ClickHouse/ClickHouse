---
toc_priority: 48
toc_title: RENAME
---

# RENAME {#misc_operations-rename}

Переименовывает базы данных, таблицы или словари. Несколько сущностей могут быть переименованы в одном запросе.
Обратите внимание, что запрос `RENAME` с несколькими сущностями это неатомарная операция. Чтобы обменять имена атомарно, используйте выражение [EXCHANGE](./exchange.md).

!!! note "Примечание"
    Запрос `RENAME` поддерживается только движком баз данных [Atomic](../../engines/database-engines/atomic.md).

**Синтаксис**

```sql
RENAME DATABASE|TABLE|DICTIONARY name TO new_name [,...] [ON CLUSTER cluster]
```

## RENAME DATABASE {#misc_operations-rename_database}

Переименовывает базы данных.

**Синтаксис**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

## RENAME TABLE {#misc_operations-rename_table}

Переименовывает одну или несколько таблиц.

Переименовывание таблиц является лёгкой операцией. Если вы указали после `TO` другую базу данных, то таблица будет перенесена в эту базу данных. При этом директории с базами данных должны быть расположены в одной файловой системе, иначе возвращается ошибка. Если переименовывается несколько таблиц в одном запросе, то такая операция неатомарная. Она может выполнится частично, и запросы в других сессиях могут получить ошибку `Table ... doesn't exist...`.

**Синтаксис**

``` sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Пример**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

## RENAME DICTIONARY {#rename_dictionary}

Переименовывает один или несколько словарей. Этот запрос можно использовать для перемещения словарей между базами данных.

**Синтаксис**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**Смотрите также**

-   [Словари](../../sql-reference/dictionaries/index.md)
