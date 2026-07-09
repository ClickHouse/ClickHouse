---
description: 'CREATE NAMED COLLECTION 문서'
sidebar_label: '명명된 컬렉션'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

새로운 명명된 컬렉션을 생성합니다.

**구문**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**예시**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**관련 SQL 문**

* [CREATE NAMED COLLECTION](/ko/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/ko/sql-reference/statements/drop#drop-function)

**관련 항목**

* [명명된 컬렉션 가이드](/ko/operations/named-collections.md)