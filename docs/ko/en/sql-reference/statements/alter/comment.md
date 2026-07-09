---
description: '테이블 주석을 추가, 수정 또는 제거할 수 있는 ALTER TABLE ... MODIFY COMMENT 문서'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

이전에 설정되었는지와 관계없이 테이블 주석을 추가, 수정 또는 제거합니다. 주석 변경 사항은 [`system.tables`](../../../operations/system-tables/tables.md)와 `SHOW CREATE TABLE` 쿼리 모두에 반영됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## 예시
</div>

주석이 있는 table을 생성하려면:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

테이블 주석(table comment)을 수정하려면:

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

수정된 주석(주석)을 보려면:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

테이블 주석을 제거하려면:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

주석이 제거되었는지 확인하려면:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="caveats">
  ## 주의사항
</div>

복제된 테이블(Replicated Table)에서는 주석이 레플리카마다 다를 수 있습니다.
주석을 수정해도 해당 변경은 단일 레플리카에만 적용됩니다.

이 기능은 버전 23.9부터 사용할 수 있습니다. 이전
ClickHouse 버전에서는 작동하지 않습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* [`COMMENT`](/ko/sql-reference/statements/create/table#comment-clause) 절
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)