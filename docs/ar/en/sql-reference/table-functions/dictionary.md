---
description: 'يعرض بيانات قاموس على هيئة جدول في ClickHouse. ويعمل بالطريقة نفسها
  التي يعمل بها محرك القاموس.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

يعرض بيانات [قاموس](../statements/create/dictionary/overview.md) على هيئة جدول في ClickHouse. ويعمل بالطريقة نفسها التي يعمل بها محرك [قاموس](../../engines/table-engines/special/dictionary.md).

<div id="syntax">
  ## الصيغة
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## المعاملات
</div>

* `dict` — اسم القاموس. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول في ClickHouse.

<div id="examples">
  ## أمثلة
</div>

جدول الإدخال `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

أنشئ قاموسًا:

```sql title="Query"
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

```sql title="Query"
SELECT * FROM dictionary('new_dictionary');
```

```text title="Response"
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

<div id="related">
  ## مواضيع ذات صلة
</div>

* [محرك Dictionary](/ar/engines/table-engines/special/dictionary)