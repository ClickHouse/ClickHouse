---
description: 'SAMPLE BY 표현식을 조작하는 방법에 대한 문서'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: '샘플링 키 표현식 조작'
doc_type: 'reference'
---

다음 작업을 수행할 수 있습니다:

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

이 명령은 테이블의 [샘플링 키](../../../engines/table-engines/mergetree-family/mergetree.md)를 `new_expression`(표현식 또는 표현식의 튜플)로 변경합니다. 프라이머리 키에는 새 샘플링 키가 포함되어야 합니다.

<div id="remove">
  ## REMOVE
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

이 명령은 테이블의 [샘플링 키](../../../engines/table-engines/mergetree-family/mergetree.md)를 제거합니다.

`MODIFY` 및 `REMOVE` 명령은 메타데이터만 변경하거나 파일만 제거하므로 경량입니다.

:::note
이 기능은 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) 계열 테이블([복제된](../../../engines/table-engines/mergetree-family/replication.md) 테이블 포함)에서만 작동합니다.
:::