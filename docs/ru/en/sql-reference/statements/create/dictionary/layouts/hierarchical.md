---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: 'Иерархические словари'
sidebar_label: 'Иерархические'
sidebar_position: 10
description: 'Настройка иерархических словарей с отношениями «родитель-потомок» между ключами.'
doc_type: 'справочник'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## Иерархические словари
</div>

ClickHouse поддерживает иерархические словари с [числовым ключом](../attributes.md#numeric-key).

Рассмотрим следующую иерархическую структуру:

```text
0 (Common parent)
│
├── 1 (United States of America)
│   │
│   └── 2 (California)
│       │
│       └── 3 (San Francisco)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

Эту иерархию можно представить в виде следующей таблицы словаря.

| region&#95;id | parent&#95;region | region&#95;name           |
| ------------- | ----------------- | ------------------------- |
| 1             | 0                 | Соединённые Штаты Америки |
| 2             | 1                 | Калифорния                |
| 3             | 2                 | Сан-Франциско             |
| 4             | 0                 | Великобритания            |
| 5             | 4                 | Лондон                    |

Эта таблица содержит столбец `parent_region`, в котором хранится ключ ближайшего родительского элемента.

ClickHouse поддерживает иерархическое свойство для атрибутов внешних словарей. Это свойство позволяет настроить иерархический словарь так, как описано выше.

Функция [dictGetHierarchy](/ru/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) позволяет получить цепочку родительских элементов.

Для нашего примера структура словаря может быть следующей:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY regions_dict
    (
        region_id UInt64,
        parent_region UInt64 DEFAULT 0 HIERARCHICAL,
        region_name String DEFAULT ''
    )
    PRIMARY KEY region_id
    SOURCE(...)
    LAYOUT(HASHED())
    LIFETIME(3600);
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <dictionary>
        <structure>
            <id>
                <name>region_id</name>
            </id>

            <attribute>
                <name>parent_region</name>
                <type>UInt64</type>
                <null_value>0</null_value>
                <hierarchical>true</hierarchical>
            </attribute>

            <attribute>
                <name>region_name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

        </structure>
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />