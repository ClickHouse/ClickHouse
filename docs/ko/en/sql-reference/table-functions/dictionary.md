---
description: '딕셔너리 데이터를 ClickHouse 테이블로 표시합니다. 딕셔너리
  엔진과 동일하게 작동합니다.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

[딕셔너리](../statements/create/dictionary/overview.md) 데이터를 ClickHouse 테이블로 표시합니다. [딕셔너리](../../engines/table-engines/special/dictionary.md) 엔진과 동일하게 작동합니다.

<div id="syntax">
  ## 구문
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## 인수
</div>

* `dict` — 딕셔너리의 이름입니다. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## 반환 값
</div>

ClickHouse 테이블입니다.

<div id="examples">
  ## 예시
</div>

입력용 테이블 `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

딕셔너리를 생성합니다:

```sql title="Query"
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

```sql title="Query"
SELECT * FROM dictionary('new_dictionary');
```

```text title="Response"
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

<div id="related">
  ## 관련 항목
</div>

* [딕셔너리 엔진](/ko/engines/table-engines/special/dictionary)