---
description: '주어진 구조로 서버에 전송된 데이터를 다른 구조의 테이블에 맞게 효율적으로 변환하여 삽입할 수 있는 테이블 함수입니다.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - 주어진 구조로 서버에 전송된 데이터를 다른 구조의 테이블에 맞게 효율적으로 변환하여 삽입할 수 있는
테이블 함수입니다.

`structure` - 다음 형식의 `'column1_name column1_type, column2_name column2_type, ...'`로 지정하는, 서버에 전송되는 데이터의 구조입니다.
예를 들어 `'id UInt32, name String'`입니다.

이 함수는 `INSERT SELECT` 쿼리에서만 한 번 사용할 수 있으며, 그 외에는 일반적인 테이블 함수처럼 동작합니다
(예를 들어 subquery 등에서 사용할 수 있습니다).

데이터는 일반적인 `INSERT` 쿼리와 마찬가지로 어떤 방식으로든 전송할 수 있으며, 지원되는 [포맷](/ko/sql-reference/formats)이라면 무엇이든 사용할 수 있습니다.
이 포맷은 일반적인 `INSERT SELECT`와 달리 쿼리 끝에 지정해야 합니다.

이 함수의 주요 기능은 서버가 클라이언트로부터 데이터를 수신할 때 `SELECT` 절의 표현식 목록에 따라 동시에 변환하고,
이를 대상 테이블에 삽입한다는 점입니다. 전송된 모든 데이터를 담는 임시 테이블은 생성되지 않습니다.

<div id="examples">
  ## 예시
</div>

* `test` 테이블의 구조가 `(a String, b String)`이고
  `data.csv`의 데이터 구조는 `(col1 String, col2 Date, col3 Int32)`로 다르다고 가정합니다. `data.csv`의 데이터를 변환하면서 동시에 `test` 테이블에 삽입하는 쿼리는
  다음과 같습니다:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* `data.csv`에 테이블 `test`와 동일한 구조인 `test_structure` 형식의 데이터가 들어 있다면, 다음 두 쿼리는 동일합니다:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```