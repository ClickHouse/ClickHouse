---
description: 'ClickHouse 데이터 포맷 작업을 위한 format 유틸리티 사용 가이드'
slug: /operations/utilities/clickhouse-format
title: 'clickhouse-format'
doc_type: 'reference'
---

입력 쿼리를 포맷할 수 있습니다.

옵션:

* `--help` or `-h` — 도움말 메시지를 출력합니다.
* `--query` — 길이와 복잡도에 관계없이 쿼리를 포맷합니다.
* `--hilite` or `--highlight` — ANSI 터미널 이스케이프 시퀀스를 사용해 구문 강조를 추가합니다.
* `--oneline` — 한 줄로 포맷합니다.
* `--max_line_length` — 지정한 길이보다 짧은 쿼리를 한 줄로 포맷합니다.
* `--comments` — 출력에 주석을 유지합니다.
* `--quiet` or `-q` — 구문만 검사하며, 성공하면 아무 출력도 하지 않습니다.
* `--multiquery` or `-n` — 같은 파일에서 여러 쿼리를 허용합니다.
* `--obfuscate` — 포맷하는 대신 난독화합니다.
* `--seed <string>` — 난독화 결과를 결정하는 임의 문자열 시드입니다.
* `--backslash` — 포맷된 쿼리의 각 줄 끝에 백슬래시를 추가합니다. 웹이나 다른 곳에서 여러 줄 쿼리를 복사한 뒤 명령줄에서 실행하려는 경우 유용할 수 있습니다.
* `--semicolons_inline` — multiquery 모드에서는 세미콜론을 새 줄이 아니라 쿼리의 마지막 줄에 작성합니다.

<div id="examples">
  ## 예시
</div>

1. 쿼리 포맷 지정:

```bash title="Query"
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

```bash title="Response"
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. 강조 표시 및 한 줄:

```bash title="Query"
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

```sql title="Response"
SELECT sum(number) FROM numbers(5)
```

3. 다중 쿼리:

```bash title="Query"
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

```sql title="Response"
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. 난독화:

```bash title="Query"
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

같은 쿼리와 다른 시드 문자열:

```bash title="Query"
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. 백슬래시 추가:

```bash title="Query"
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

```sql title="Response"
SELECT * \
FROM  \
( \
    SELECT 1 AS x \
    UNION ALL \
    SELECT 1 \
    UNION DISTINCT \
    SELECT 3 \
)
```