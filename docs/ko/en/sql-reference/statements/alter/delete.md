---
description: 'ALTER TABLE ... DELETE SQL 문에 대한 설명'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'ALTER TABLE ... DELETE SQL 문'
doc_type: '참고'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

지정된 필터링 표현식과 일치하는 데이터를 삭제합니다. [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

:::note
`ALTER TABLE` 접두사 때문에 이 구문은 SQL을 지원하는 대부분의 다른 시스템과 다릅니다. 이는 OLTP 데이터베이스의 유사한 쿼리와 달리, 자주 사용하도록 설계되지 않은 무거운 작업임을 나타내기 위한 것입니다. `ALTER TABLE`은 삭제 전에 기반 데이터를 머지해야 하는 heavyweight 작업으로 간주됩니다. MergeTree 테이블에서는 경량한 삭제를 수행하며 훨씬 더 빠를 수 있는 [`DELETE FROM` 쿼리](/ko/sql-reference/statements/delete.md) 사용을 고려하십시오.
:::

`filter_expr`는 `UInt8` 유형이어야 합니다. 이 쿼리는 이 표현식 값이 0이 아닌 테이블의 행을 삭제합니다.

하나의 쿼리에는 쉼표로 구분된 여러 명령을 포함할 수 있습니다.

쿼리 처리의 동기성은 [mutations&#95;sync](/ko/operations/settings/settings.md/#mutations_sync) 설정에 의해 정의됩니다. 기본값은 비동기입니다.

**관련 항목**

* [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)
* [ALTER 쿼리의 동기성](/ko/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/ko/operations/settings/settings.md/#mutations_sync) 설정

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 업데이트 및 삭제를 처리하는 방법](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)