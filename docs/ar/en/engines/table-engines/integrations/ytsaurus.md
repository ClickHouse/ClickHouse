---
description: 'محرك الجدول يتيح استيراد البيانات من عنقود YTsaurus.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'محرك YTsaurus للجداول'
keywords: ['YTsaurus', 'محرك الجدول']
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # محرك جدول YTsaurus
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

يتيح محرك جدول YTsaurus استيراد البيانات من عنقود YTsaurus.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
هذه ميزة تجريبية وقد تتغير في الإصدارات القادمة بشكل غير متوافق مع الإصدارات السابقة.
فعِّل استخدام محرك الجدول YTsaurus
باستخدام الإعداد [`allow_experimental_ytsaurus_table_engine`](/ar/operations/settings/settings#allow_experimental_ytsaurus_table_engine).

يمكنك فعل ذلك باستخدام:

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**معلمات المحرك**

* `http_proxy_url` — URL لوكيل HTTP الخاص بـ YTsaurus.
* `cypress_path` — مسار Cypress إلى مصدر البيانات.
* `oauth_token` — رمز OAuth.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

يوضح استعلامًا يُنشئ جدول YTsaurus:

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

لاسترجاع البيانات من الجدول، شغّل:

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## أنواع البيانات
</div>

<div id="primitive-data-types">
  ### أنواع البيانات البدائية
</div>

| نوع بيانات YTsaurus       | نوع بيانات ClickHouse  |
| ------------------------- | ---------------------- |
| `int8`                    | `Int8`                 |
| `int16`                   | `Int16`                |
| `int32`                   | `Int32`                |
| `int64`                   | `Int64`                |
| `uint8`                   | `UInt8`                |
| `uint16`                  | `UInt16`               |
| `uint32`                  | `UInt32`               |
| `uint64`                  | `UInt64`               |
| `float`                   | `Float32`              |
| `double`                  | `Float64`              |
| `boolean`                 | `Bool`                 |
| `string`                  | `String`               |
| `utf8`                    | `String`               |
| `json`                    | `JSON`                 |
| `yson(type_v3)`           | `JSON`                 |
| `uuid`                    | `UUID`                 |
| `date32`                  | `Date` (غير مدعوم بعد) |
| `datetime64`              | `Int64`                |
| `timestamp64`             | `Int64`                |
| `interval64`              | `Int64`                |
| `date`                    | `Date` (غير مدعوم بعد) |
| `datetime`                | `DateTime`             |
| `timestamp`               | `DateTime64(6)`        |
| `interval`                | `UInt64`               |
| `any`                     | `String`               |
| `null`                    | `Nothing`              |
| `void`                    | `Nothing`              |
| `T` مع `required = False` | `Nullable(T)`          |

<div id="composite-data-types">
  ### الأنواع المركبة
</div>

| نوع بيانات YTsaurus | نوع بيانات ClickHouse  |
| ------------------- | ---------------------- |
| `decimal`           | `Decimal`              |
| `optional`          | `Nullable`             |
| `list`              | `Array`                |
| `struct`            | `NamedTuple`           |
| `tuple`             | `Tuple`                |
| `variant`           | `Variant`              |
| `dict`              | &#96;Array(Tuple(...)) |
| `tagged`            | `T`                    |

**انظر أيضًا**

* دالة الجدول [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md)
* [مخطط بيانات YTsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [أنواع بيانات YTsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)