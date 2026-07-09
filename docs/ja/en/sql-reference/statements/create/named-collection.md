---
description: 'CREATE NAMED COLLECTION に関するドキュメント'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

新しい名前付きコレクションを作成します。

**構文**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**例**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**関連するステートメント**

* [CREATE NAMED COLLECTION](/ja/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/ja/sql-reference/statements/drop#drop-function)

**関連項目**

* [Named collections ガイド](/ja/operations/named-collections.md)