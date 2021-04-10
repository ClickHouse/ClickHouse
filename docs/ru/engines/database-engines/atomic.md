---
toc_priority: 32
toc_title: Atomic
---


# Atomic {#atomic}

Поддерживает неблокирующие запросы `DROP` и `RENAME TABLE` и запросы `EXCHANGE TABLES t1 AND t2`. Движок `Atomic` используется по умолчанию.

## Создание БД {#creating-a-database}

```sql
CREATE DATABASE test ENGINE = Atomic;
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/engines/database-engines/atomic/) <!--hide-->
