---
alias: []
description: 'One 포맷 문서'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 설명
</div>

`One` 포맷은 파일에서 데이터를 전혀 읽지 않고, [`UInt8`](../../sql-reference/data-types/int-uint.md) 타입의 `dummy`라는 이름을 가진 컬럼과 값 `0`을 포함하는 행 1개만 반환하는 특수한 입력 형식입니다(`system.one` 테이블과 유사).
실제 데이터를 읽지 않고 모든 파일을 나열할 때 가상 컬럼 &#96;&#95;file/&#95;path&#96;&#96;와 함께 사용할 수 있습니다.

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## 포맷 설정
</div>
