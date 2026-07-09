---
alias: []
description: 'Vertical 형식 문서'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

각 값을 지정된 컬럼 이름과 함께 별도의 줄에 출력합니다. 이 포맷은 각 행이 많은 수의 컬럼으로 이루어져 있을 때, 하나 또는 몇 개의 행만 출력하기에 편리합니다.

문자열 값 `NULL`과 값이 없음을 더 쉽게 구분할 수 있도록 [`NULL`](/ko/sql-reference/syntax.md)은 `ᴺᵁᴸᴸ`로 출력됩니다. JSON 컬럼은 보기 좋게 출력되며, `NULL`은 유효한 JSON 값이고 `"null"`과도 쉽게 구분할 수 있으므로 `null`로 출력됩니다.

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Vertical 형식에서는 행이 이스케이프되지 않습니다:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

이 포맷은 쿼리 결과를 출력할 때만 적합하며, 파싱(테이블에 삽입할 데이터를 읽어오는 작업)에는 적합하지 않습니다.

<div id="format-settings">
  ## 포맷 설정
</div>
