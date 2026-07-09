---
description: '데이터베이스 주석을 추가, 수정 또는 제거할 수 있는 ALTER DATABASE ... MODIFY COMMENT SQL 문에 대한 문서입니다.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'ALTER DATABASE ... MODIFY COMMENT SQL 문'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

이전에 설정되었는지와 관계없이 데이터베이스 주석을 추가, 수정 또는 제거합니다. 주석 변경 사항은 [`system.databases`](/ko/operations/system-tables/databases.md)와 `SHOW CREATE DATABASE` 쿼리 모두에 반영됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## 예시
</div>

주석을 지정해 `DATABASE`를 생성하려면:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

주석을 수정하려면:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

변경된 주석을 보려면:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

데이터베이스 주석을 제거하려면:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

주석이 제거되었는지 확인하려면:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* [`COMMENT`](/ko/sql-reference/statements/create/table#comment-clause) 절
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)