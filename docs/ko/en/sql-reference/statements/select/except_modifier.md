---
description: '결과에서 제외할 하나 이상의 컬럼 이름을 지정하는 EXCEPT 수정자에 대한 문서입니다. 일치하는 모든 컬럼 이름은 출력에서 생략됩니다.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except-modifier
title: 'EXCEPT 수정자'
keywords: ['EXCEPT', '수정자']
doc_type: 'reference'
---

> 결과에서 제외할 하나 이상의 컬럼 이름을 지정합니다. 일치하는 모든 컬럼 이름은 출력에서 생략됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

<div id="examples">
  ## 예시
</div>

```sql title="Query"
SELECT * EXCEPT (i) from columns_transformers;
```

```response title="Response"
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```