---
toc_priority: 49
toc_title: EXCHANGE
---

# EXCHANGE {#exchange}

Атомарно обменивает имена двух таблиц или словарей.
Это действие также можно выполнить с помощью запроса [RENAME](./rename.md), используя третье временное имя, но в таком случае действие неатомарно.

!!! note "Примечание"
    Запрос `EXCHANGE` поддерживается только движком баз данных [Atomic](../../engines/database-engines/atomic.md).

**Синтаксис**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B
```

## EXCHANGE TABLES {#exchange_tables}

Обменивает имена двух таблиц.

**Синтаксис**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B
```

## EXCHANGE DICTIONARIES {#exchange_dictionaries}

Обменивает имена двух словарей.

**Синтаксис**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B
```

**Смотрите также**

-   [Словари](../../sql-reference/dictionaries/index.md)
