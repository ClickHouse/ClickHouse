---
description: 'Возвращает таблицу, подключенную через JDBC-драйвер.'
sidebar_label: 'jdbc'
sidebar_position: 100
slug: /sql-reference/table-functions/jdbc
title: 'jdbc'
doc_type: 'reference'
---

:::note
clickhouse-jdbc-bridge содержит экспериментальный код и больше не поддерживается. Он может иметь проблемы с надежностью и уязвимости в безопасности. Используйте его на свой страх и риск.
ClickHouse рекомендует использовать встроенные табличные функции ClickHouse, которые являются более подходящей альтернативой для ad-hoc запросов (Postgres, MySQL, MongoDB и т. д.).
:::

Табличная функция JDBC возвращает таблицу, подключенную через JDBC-драйвер.

Для этой табличной функции требуется отдельно запущенная программа [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge).
Она поддерживает типы Nullable (на основе DDL удаленной таблицы, к которой выполняется запрос).

<div id="syntax">
  ## Синтаксис
</div>

```sql
jdbc(datasource, external_database, external_table)
jdbc(datasource, external_table)
jdbc(named_collection)
```

<div id="examples">
  ## Примеры
</div>

Вместо имени внешней базы данных можно указать схему:

```sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

```sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'select * from schema.table')
```

```sql
SELECT * FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT *
FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT a.datasource AS server1, b.datasource AS server2, b.name AS db
FROM jdbc('mysql-dev?datasource_column', 'show databases') a
INNER JOIN jdbc('self?datasource_column', 'show databases') b ON a.Database = b.name
```