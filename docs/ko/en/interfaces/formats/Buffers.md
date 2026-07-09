---
alias: []
description: 'Buffers 포맷 문서'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`Buffers`는 소비자와 생산자가 이미 스키마와 컬럼 순서를 알고 있을 때 **임시** 데이터 교환에 사용하는 매우 단순한 바이너리 형식입니다.

[Native](./Native.md)와 달리 컬럼 이름, 컬럼 타입 또는 추가 메타데이터를 저장하지 **않습니다**.

이 포맷에서는 데이터를 바이너리 형식의 [블록](/ko/development/architecture#block) 단위로 쓰고 읽습니다. Buffers는 [Native](./Native.md) 포맷과 동일한 컬럼별 이진 표현을 사용하며, 동일한 Native 형식 설정을 따릅니다.

각 블록에 대해 다음 시퀀스가 기록됩니다.

1. 컬럼 수 (UInt64, 리틀 엔디언).
2. 행 수 (UInt64, 리틀 엔디언).
3. 각 컬럼에 대해:

* 직렬화된 컬럼 데이터의 총 바이트 크기 (UInt64, 리틀 엔디언).
* [Native](./Native.md) 포맷과 정확히 동일한 직렬화된 컬럼 데이터 바이트.

<div id="example-usage">
  ## 사용 예시
</div>

파일에 쓰기:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

명시적으로 지정한 컬럼 타입으로 다시 읽습니다:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

동일한 컬럼 타입을 가진 테이블이 있으면 바로 채울 수 있습니다:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

테이블을 살펴보십시오:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## 포맷 설정
</div>
