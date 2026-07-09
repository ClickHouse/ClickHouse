---
description: 'ALTER TABLE ... UPDATE SQL 문 관련 문서'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'ALTER TABLE ... UPDATE SQL 문'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

지정된 필터링 표현식과 일치하는 데이터를 조작합니다. [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

:::note
`ALTER TABLE` 접두사 때문에 이 구문은 SQL을 지원하는 대부분의 다른 시스템과 다릅니다. 이는 OLTP 데이터베이스의 유사한 쿼리와 달리, 이 작업이 자주 사용할 목적으로 설계된 것이 아닌 비용이 큰 작업임을 나타내기 위한 것입니다.
:::

`filter_expr`는 `UInt8` 타입이어야 합니다. 이 쿼리는 `filter_expr`가 0이 아닌 값을 갖는 행에서 지정된 컬럼의 값을 해당 표현식의 값으로 업데이트합니다. 값은 `CAST` 연산자를 사용해 컬럼 타입으로 변환됩니다. 프라이머리 키 또는 파티션 키 계산에 사용되는 컬럼은 업데이트할 수 없습니다.

하나의 쿼리에는 쉼표로 구분된 여러 명령을 포함할 수 있습니다.

쿼리 처리의 동기성은 [mutations&#95;sync](/ko/operations/settings/settings.md/#mutations_sync) 설정으로 정의됩니다. 기본적으로 비동기입니다.

**관련 항목**

* [Mutations](/ko/sql-reference/statements/alter/index.md#mutations)
* [ALTER Queries의 동기성](/ko/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/ko/operations/settings/settings.md/#mutations_sync) 설정
* [Lightweight `UPDATE`](/ko/sql-reference/statements/update) - patch parts를 사용하는 대체 경량 업데이트
* [`APPLY PATCHES`](/ko/sql-reference/statements/alter/apply-patches) - 경량 업데이트의 패치를 수동으로 적용

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 업데이트 및 삭제를 처리하는 방법](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)