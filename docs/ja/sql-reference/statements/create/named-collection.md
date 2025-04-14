---
slug: /ja/sql-reference/statements/create/named-collection
sidebar_label: NAMED COLLECTION
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

# NAMED COLLECTIONの作成

新しいNamed Collectionを作成します。

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

**関連ステートメント**

- [NAMED COLLECTIONの作成](https://clickhouse.com/docs/ja/sql-reference/statements/alter/named-collection)
- [NAMED COLLECTIONの削除](https://clickhouse.com/docs/ja/sql-reference/statements/drop#drop-function)


**関連項目**

- [Named collectionsガイド](/docs/ja/operations/named-collections.md)
