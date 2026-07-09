---
description: 'EXECUTE AS 문 설명'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'EXECUTE AS 문'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # EXECUTE AS 문
</div>

다른 사용자의 권한으로 쿼리를 실행할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

첫 번째 형식(`subquery` 없음)은 현재 세션에서 뒤이어 실행되는 모든 쿼리가 지정된 `target_user`의 권한으로 실행되도록 설정합니다.

두 번째 형식(`subquery` 있음)은 지정된 `subquery`만 지정된 `target_user`의 권한으로 실행합니다.

두 형식이 모두 작동하려면 구성 설정 `access_control_improvements.allow_impersonate_user`
이 `1`로 설정되어 있어야 하며, `IMPERSONATE` 권한이 부여되어 있어야 합니다. 예를 들어, 다음 명령은

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

사용자 `user2`가 `EXECUTE AS user1 ...` 명령을 실행할 수 있도록 허용하고, 사용자 `user3`는 어떤 사용자로든 명령을 실행할 수 있도록 허용합니다.

다른 사용자로 가장하는 동안 함수 [currentUser()](/ko/sql-reference/functions/other-functions#currentUser)는 그 사용자의 이름을 반환하고,
함수 [authenticatedUser()](/ko/sql-reference/functions/other-functions#authenticatedUser)는 실제로 인증된 사용자의 이름을 반환합니다.

<div id="examples">
  ## 예시
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```