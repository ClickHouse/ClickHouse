---
description: 'Документация по созданию и настройке словарей'
sidebar_label: 'Обзор'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'справочник'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

Словарь — это сопоставление (`key -> attributes`), удобное для различных типов справочников.
ClickHouse поддерживает специальные функции для работы со словарями, которые можно использовать в запросах. Использовать словари через эти функции проще и эффективнее, чем `JOIN` со справочными таблицами.

Словари можно создавать двумя способами:

* [С помощью DDL-запроса](#creating-a-dictionary-with-a-ddl-query) (рекомендуется)
* [С помощью файла конфигурации](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## Создание словаря с помощью DDL-запроса
</div>

<CloudSupportedBadge />

Словари можно создавать с помощью DDL-запросов.
Это рекомендуемый способ, поскольку для словарей, созданных через DDL:

* В файлы конфигурации сервера не добавляются дополнительные записи.
* Словари можно использовать как полноценные сущности, такие как таблицы или представления.
* Данные можно читать напрямую, используя привычный синтаксис `SELECT`, а не табличные функции словаря. Обратите внимание: при прямом доступе к словарю через оператор `SELECT` словарь с кэшем вернет только кэшированные данные, тогда как словарь без кэша вернет все хранимые в нем данные.
* Словари можно легко переименовывать.

<div id="syntax">
  ### Синтаксис
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Конструкция                                 | Описание                                                                                                                                               |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [Атрибуты](./attributes.md)                 | Атрибуты словаря задаются аналогно столбцам таблицы. Единственное обязательное свойство — тип, все остальные могут иметь значения по умолчанию.        |
| PRIMARY KEY                                 | Определяет ключевой столбец или столбцы для поиска в словаре. В зависимости от структуры в качестве ключей можно указать один или несколько атрибутов. |
| [`SOURCE`](./sources/overview.md)           | Определяет источник данных для словаря (например, таблицу ClickHouse, HTTP, PostgreSQL).                                                               |
| [`LAYOUT`](./layouts/overview.md)           | Определяет, как словарь хранится в памяти (например, `FLAT`, `HASHED`, `CACHE`).                                                                       |
| [`LIFETIME`](./lifetime.md)                 | Задаёт интервал обновления словаря.                                                                                                                    |
| [`ON CLUSTER`](../../../distributed-ddl.md) | Создаёт словарь в кластере. Необязательно.                                                                                                             |
| `SETTINGS`                                  | Дополнительные настройки словаря. Необязательно.                                                                                                       |
| `COMMENT`                                   | Добавляет текстовый комментарий к словарю. Необязательно.                                                                                              |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## Создание словаря с помощью файла конфигурации
</div>

<CloudNotSupportedBadge />

:::note
Создание словаря с помощью файла конфигурации не поддерживается в ClickHouse Cloud. Используйте DDL (см. выше) и создайте словарь от имени пользователя `default`.
:::

Файл конфигурации словаря имеет следующий формат:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

В одном файле можно настроить любое количество словарей.

<div id="related-content">
  ## Связанные материалы
</div>

* [Структуры](/ru/sql-reference/statements/create/dictionary/layouts) — Как словари хранятся в памяти
* [Источники](/ru/sql-reference/statements/create/dictionary/sources) — Подключение к источникам данных
* [Время жизни](./lifetime.md) — Настройка автоматического обновления
* [Атрибуты](./attributes.md) — Настройка ключей и атрибутов
* [Встроенные словари](./embedded.md) — Встроенные словари геобазы
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — Системная таблица с информацией о словарях