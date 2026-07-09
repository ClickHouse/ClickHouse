---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'Источник словаря Null'
sidebar_position: 14
sidebar_label: 'Null'
description: 'Настройка источника словаря Null (пустого) в ClickHouse для тестирования.'
doc_type: 'reference'
---

Специальный источник, который можно использовать для создания пустых словарей-заглушек.
Такие словари могут быть полезны для тестирования или в конфигурациях с отдельными узлами для хранения данных и выполнения запросов, использующих distributed таблицы.

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```