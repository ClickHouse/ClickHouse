---
description: 'RENAME SQL 문에 대한 문서'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'RENAME SQL 문'
doc_type: 'reference'
---

데이터베이스, 테이블 또는 딕셔너리의 이름을 변경합니다. 여러 엔터티의 이름을 단일 쿼리에서 변경할 수 있습니다.
여러 엔터티를 포함하는 `RENAME` 쿼리는 비원자적 작업입니다. 엔터티 이름을 원자적으로 스왑하려면 [EXCHANGE](./exchange.md) SQL 문을 사용하십시오.

**구문**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

데이터베이스 이름을 변경합니다.

**구문**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

하나 이상의 테이블 이름을 변경합니다.

테이블 이름 변경은 부담이 적은 작업입니다. `TO` 뒤에 다른 데이터베이스를 지정하면 테이블이 해당 데이터베이스로 이동됩니다. 다만, 데이터베이스 디렉터리는 동일한 파일 시스템에 있어야 합니다. 그렇지 않으면 오류가 반환됩니다.
하나의 쿼리에서 여러 테이블의 이름을 변경하면 이 작업은 원자적(atomic)이지 않습니다. 작업이 일부만 실행될 수 있으며, 다른 세션의 쿼리에서 `Table ... does not exist ...` 오류가 발생할 수 있습니다.

**구문**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**예시**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

더 간단한 SQL을 사용할 수도 있습니다:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

하나 이상의 딕셔너리 이름을 변경합니다. 이 쿼리를 사용하면 딕셔너리를 데이터베이스 간에 이동할 수 있습니다.

**구문**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**관련 항목**

* [딕셔너리](./create/dictionary/overview.md)