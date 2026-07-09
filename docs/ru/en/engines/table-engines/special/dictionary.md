---
description: 'Движок `Dictionary` отображает данные словаря как таблицу ClickHouse.'
sidebar_label: 'Dictionary'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Табличный движок Dictionary'
doc_type: 'reference'
---

Движок `Dictionary` отображает данные [словаря](../../../sql-reference/statements/create/dictionary/overview.md) как таблицу ClickHouse.

<div id="example">
  ## Пример
</div>

Рассмотрим в качестве примера словарь `products` со следующей конфигурацией:

```xml
<dictionaries>
    <dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</dictionaries>
```

Выполните запрос к данным словаря:

```sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

```text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

Вы можете использовать функции [dictGet*](/ru/sql-reference/functions/ext-dict-functions), чтобы получать данные словаря в этом формате.

Это представление не очень удобно, если вам нужны необработанные данные или нужно выполнить операцию `JOIN`. В таких случаях можно использовать движок `Dictionary`, который показывает данные словаря в виде таблицы.

Синтаксис:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Пример использования:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

Хорошо

Взгляните на содержимое таблицы.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**См. также**

* [Функция dictionary](/ru/sql-reference/table-functions/dictionary)