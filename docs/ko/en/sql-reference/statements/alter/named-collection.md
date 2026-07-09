---
description: 'ALTER 명명된 컬렉션 문서'
sidebar_label: '명명된 컬렉션'
slug: /sql-reference/statements/alter/named-collection
title: 'ALTER NAMED COLLECTION'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="alter-named-collection">
  # ALTER 명명된 컬렉션
</div>

이 쿼리는 기존에 있는 명명된 컬렉션을 수정하는 데 사용됩니다.

**구문**

```sql
ALTER NAMED COLLECTION [IF EXISTS] name [ON CLUSTER cluster]
[ SET
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
... ] |
[ DELETE key_name4, key_name5, ... ]
```

**예시**

```sql
CREATE NAMED COLLECTION foobar AS a = '1' NOT OVERRIDABLE, b = '2';

ALTER NAMED COLLECTION foobar SET a = '2' OVERRIDABLE, c = '3';

ALTER NAMED COLLECTION foobar DELETE b;
```