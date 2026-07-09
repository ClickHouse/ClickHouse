---
description: 'Движок таблицы, позволяющий импортировать данные из кластера YTsaurus.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'Движок таблицы YTsaurus'
keywords: ['YTsaurus', 'движок таблицы']
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # движок таблицы YTsaurus
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Движок таблицы YTsaurus позволяет импортировать данные из кластера YTsaurus.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
Это экспериментальная возможность, и в будущих релизах она может измениться с нарушением обратной совместимости.
Чтобы включить использование движка таблицы YTsaurus,
используйте настройку [`allow_experimental_ytsaurus_table_engine`](/ru/operations/settings/settings#allow_experimental_ytsaurus_table_engine).

Это можно сделать так:

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**Параметры движка таблицы**

* `http_proxy_url` — URL HTTP-прокси YTsaurus.
* `cypress_path` — путь в Cypress к источнику данных.
* `oauth_token` — токен OAuth.

<div id="usage-example">
  ## Пример использования
</div>

Ниже показан запрос, создающий таблицу YTsaurus:

```sql title="Query"
SHOW CREATE TABLE yt_saurus;
```

```sql title="Response"
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')
```

Чтобы получить данные из таблицы, выполните:

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## Типы данных
</div>

<div id="primitive-data-types">
  ### Примитивные типы данных
</div>

| Тип данных YTsaurus      | Тип данных ClickHouse         |
| ------------------------ | ----------------------------- |
| `int8`                   | `Int8`                        |
| `int16`                  | `Int16`                       |
| `int32`                  | `Int32`                       |
| `int64`                  | `Int64`                       |
| `uint8`                  | `UInt8`                       |
| `uint16`                 | `UInt16`                      |
| `uint32`                 | `UInt32`                      |
| `uint64`                 | `UInt64`                      |
| `float`                  | `Float32`                     |
| `double`                 | `Float64`                     |
| `boolean`                | `Bool`                        |
| `string`                 | `String`                      |
| `utf8`                   | `String`                      |
| `json`                   | `JSON`                        |
| `yson(type_v3)`          | `JSON`                        |
| `uuid`                   | `UUID`                        |
| `date32`                 | `Date`(Ещё не поддерживается) |
| `datetime64`             | `Int64`                       |
| `timestamp64`            | `Int64`                       |
| `interval64`             | `Int64`                       |
| `date`                   | `Date`(Ещё не поддерживается) |
| `datetime`               | `DateTime`                    |
| `timestamp`              | `DateTime64(6)`               |
| `interval`               | `UInt64`                      |
| `any`                    | `String`                      |
| `null`                   | `Nothing`                     |
| `void`                   | `Nothing`                     |
| `T` с `required = False` | `Nullable(T)`                 |

<div id="composite-data-types">
  ### Составные типы
</div>

| Тип данных YTsaurus | Тип данных ClickHouse  |
| ------------------- | ---------------------- |
| `decimal`           | `Decimal`              |
| `optional`          | `Nullable`             |
| `list`              | `Array`                |
| `struct`            | `NamedTuple`           |
| `tuple`             | `Tuple`                |
| `variant`           | `Variant`              |
| `dict`              | &#96;Array(Tuple(...)) |
| `tagged`            | `T`                    |

**См. также**

* табличная функция [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md)
* [схема данных YTsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [типы данных YTsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)