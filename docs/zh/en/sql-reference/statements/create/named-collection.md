---
description: 'CREATE 命名集合 文档'
sidebar_label: '命名集合'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE 命名集合'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

创建新的命名集合。

**语法**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**示例**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**相关语句**

* [CREATE NAMED COLLECTION](/zh/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/zh/sql-reference/statements/drop#drop-function)

**另请参阅**

* [命名集合指南](/zh/operations/named-collections.md)