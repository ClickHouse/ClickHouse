---
description: '제약 조건 변경 관련 문서'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: '제약 조건 변경'
doc_type: 'reference'
---

제약 조건은 다음 구문을 사용하여 추가, 수정 또는 삭제할 수 있습니다:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

테이블 생성과 마찬가지로 제약 조건은 `CHECK`(`INSERT` 시 적용) 또는 `ASSUME`(검사하지 않고 옵티마이저가 신뢰)로 선언할 수 있습니다. 두 방식의 차이점은 [제약 조건](../../../sql-reference/statements/create/table.md#constraints)을 참조하십시오.

`MODIFY CONSTRAINT`는 기존 제약 조건의 선언을 대체하면서도 테이블 정의에서의 위치는 유지합니다. 또한 제약 조건의 종류도 변경할 수 있습니다(예: `CHECK`에서 `ASSUME`로). 이는 제약 조건을 삭제한 뒤 새 선언으로 다시 추가하는 것과 같습니다. 제약 조건이 존재하지 않으면 `IF EXISTS`가 지정된 경우를 제외하고 오류가 발생합니다.

[제약 조건](../../../sql-reference/statements/create/table.md#constraints)에 관한 자세한 내용도 참조하십시오.

이 쿼리들은 테이블의 제약 조건 관련 메타데이터만 추가, 변경 또는 제거하므로 즉시 처리됩니다.

:::tip
제약 조건을 추가하거나 수정해도 기존 데이터에 대해서는 검사가 **실행되지 않습니다**.
:::

복제된 테이블의 모든 변경 사항은 ZooKeeper로 전파되며 다른 레플리카에도 적용됩니다.