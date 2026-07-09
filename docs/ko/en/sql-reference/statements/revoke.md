---
description: 'REVOKE SQL 문 관련 문서'
sidebar_label: 'REVOKE'
sidebar_position: 39
slug: /sql-reference/statements/revoke
title: 'REVOKE SQL 문'
doc_type: 'reference'
---

사용자 또는 역할에서 권한을 취소합니다.

<div id="syntax">
  ## 구문
</div>

**사용자에게 부여된 권한 취소하기**

```sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**사용자에게 부여된 역할 취소**

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

<div id="description">
  ## 설명
</div>

일부 권한을 취소하려면, 취소하려는 권한보다 더 넓은 범위의 권한을 사용할 수 있습니다. 예를 들어 사용자가 `SELECT (x,y)` 권한을 가지고 있는 경우, 관리자는 이 권한을 취소하기 위해 `REVOKE SELECT(x,y) ...`, `REVOKE SELECT * ...`, 또는 `REVOKE ALL PRIVILEGES ...` 쿼리까지 실행할 수 있습니다.

<div id="partial-revokes">
  ### 부분 권한 취소
</div>

권한의 일부만 취소할 수 있습니다. 예를 들어, 사용자에게 `SELECT *.*` 권한이 있는 경우 특정 테이블이나 데이터베이스의 데이터를 읽는 권한만 해당 권한에서 취소할 수 있습니다.

<div id="examples">
  ## 예시
</div>

`accounts`를 제외한 모든 데이터베이스에 대해 `john` 사용자 계정에 SELECT 권한을 부여합니다:

```sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

`mira` 사용자 계정에 `accounts.staff` 테이블의 모든 컬럼 중 `wage`를 제외한 컬럼을 선택할 수 있는 권한을 부여합니다.

```sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[원문](/ko/operations/settings/settings/)