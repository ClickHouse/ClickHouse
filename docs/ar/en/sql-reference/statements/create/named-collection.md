---
description: 'توثيق CREATE NAMED COLLECTION'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

ينشئ مجموعة مُسمّاة جديدة.

**البنية**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**مثال**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**تعليمات SQL ذات الصلة**

* [CREATE NAMED COLLECTION](/ar/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/ar/sql-reference/statements/drop#drop-function)

**راجع أيضًا**

* [دليل المجموعات المُسمّاة](/ar/operations/named-collections.md)