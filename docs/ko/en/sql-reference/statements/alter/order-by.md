---
description: '키 표현식 변경에 대한 문서'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: '키 표현식 변경'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

이 명령은 테이블의 [정렬 키(sorting key)](../../../engines/table-engines/mergetree-family/mergetree.md)를 `new_expression`(표현식 또는 표현식 튜플)으로 변경합니다. 프라이머리 키는 그대로 유지됩니다.

이 명령은 메타데이터만 변경한다는 점에서 경량입니다. 데이터 파트의 행이 정렬 키 표현식에 따라 정렬된 상태를 유지하도록 하려면, 기존 컬럼을 포함하는 표현식은 정렬 키에 추가할 수 없습니다(`ALTER` 쿼리에서 동일한 `ADD COLUMN` 명령으로 추가된 컬럼만 가능하며, 이 경우 기본 컬럼 값이 없어야 합니다).

:::note
이 기능은 [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 계열( [복제된](../../../engines/table-engines/mergetree-family/replication.md) 테이블 포함) 테이블에서만 작동합니다.
:::