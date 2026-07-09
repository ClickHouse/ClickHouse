---
description: '쿼리의 외부 테이블 표현식이 반환하는 각 행에 대해 함수를 호출할 수 있게 해주는 APPLY 수정자를 설명하는 문서입니다.'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'REPLACE 수정자'
keywords: ['REPLACE', 'modifier']
doc_type: 'reference'
---

> 하나 이상의 [표현식 별칭](/ko/sql-reference/syntax#expression-aliases)을 지정할 수 있습니다.

각 별칭은 `SELECT *` SQL 문의 컬럼 이름과 일치해야 합니다. 출력 컬럼 목록에서 별칭과 일치하는 컬럼은
해당 `REPLACE`에서 지정한 표현식으로 대체됩니다.

이 수정자는 컬럼의 이름이나 순서를 변경하지 않습니다. 하지만 값과 데이터 유형은 변경할 수 있습니다.

**구문:**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**예시:**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```