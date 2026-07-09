---
alias: []
description: 'LineAsStringWithNamesAndTypes 포맷 문서'
input_format: false
keywords: ['LineAsStringWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/LineAsStringWithNamesAndTypes
title: 'LineAsStringWithNamesAndTypes'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

`LineAsStringWithNames` 포맷은 [`LineAsString`](./LineAsString.md) 포맷과 유사하지만,
두 개의 헤더 행을 출력합니다. 하나에는 컬럼 이름이, 다른 하나에는 타입이 표시됩니다.

<div id="example-usage">
  ## 사용 예시
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## 포맷 설정
</div>
