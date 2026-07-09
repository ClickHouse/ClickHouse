---
description: 'CHECK GRANT 문서'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'CHECK GRANT SQL 문'
doc_type: 'reference'
---

`CHECK GRANT` 쿼리는 현재 사용자 또는 역할에 특정 권한이 부여되었는지 확인하는 데 사용됩니다.

<div id="syntax">
  ## 구문
</div>

쿼리의 기본 구문은 다음과 같습니다.

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — 권한 유형.

<div id="examples">
  ## 예시
</div>

사용자에게 이전에 해당 권한가 granted된 경우, 응답 `check_grant`는 `1`입니다. 그렇지 않으면 응답 `check_grant`는 `0`입니다.

`table_1.col1`이 존재하고 현재 사용자에게 권한 `SELECT`/`SELECT(con)` 또는 역할(해당 권한 포함)이 granted된 경우, 응답은 `1`입니다.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

`table_2.col2`가 존재하지 않거나, 현재 사용자에게 `SELECT`/`SELECT(con)` 권한 또는 해당 권한이 있는 역할이 부여되지 않은 경우 응답은 `0`입니다.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## 와일드카드
</div>

권한을 지정할 때 테이블(table) 또는 데이터베이스 이름 대신 애스터리스크(`*`)를 사용할 수 있습니다. 와일드카드 규칙은 [WILDCARD GRANTS](../../sql-reference/statements/grant.md#wildcard-grants)에서 확인하십시오.