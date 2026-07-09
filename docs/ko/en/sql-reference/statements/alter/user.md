---
description: 'USER 문서'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'reference'
---

ClickHouse 사용자 계정을 변경합니다.

구문:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

`ALTER USER`를 사용하려면 [ALTER USER](../../../sql-reference/statements/grant.md#access-management) 권한이 있어야 합니다.

`SET variable = value`는 `MODIFY SETTING variable = value`의 별칭입니다. 즉, 나머지 설정은 그대로 유지하면서 특정 설정 하나만 변경합니다. 전체 설정 목록을 대체하고 상속된 모든 상위 프로필도 제거하는 일반 `SETTINGS` 절 대신 이것(또는 `MODIFY SETTING`)을 사용하는 것이 좋습니다.

<div id="grantees-clause">
  ## GRANTEES 절
</div>

이 사용자로부터 [권한](../../../sql-reference/statements/grant.md#privileges)을 부여받을 수 있는 사용자 또는 역할을 지정합니다. 단, 그러려면 이 사용자에게도 필요한 모든 접근 권한이 [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax)과 함께 부여되어 있어야 합니다. `GRANTEES` 절의 옵션은 다음과 같습니다.

* `user` — 이 사용자가 권한을 부여할 수 있는 사용자를 지정합니다.
* `role` — 이 사용자가 권한을 부여할 수 있는 역할을 지정합니다.
* `ANY` — 이 사용자는 누구에게나 권한을 부여할 수 있습니다. 기본 설정입니다.
* `NONE` — 이 사용자는 누구에게도 권한을 부여할 수 없습니다.

`EXCEPT` 표현식을 사용하면 특정 사용자 또는 역할을 제외할 수 있습니다. 예를 들어 `ALTER USER user1 GRANTEES ANY EXCEPT user2`는 `user1`이 `GRANT OPTION`과 함께 부여된 권한을 가지고 있는 경우, `user2`를 제외한 누구에게나 해당 권한을 부여할 수 있음을 의미합니다.

<div id="examples">
  ## 예시
</div>

할당된 역할을 기본 역할로 설정:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

사용자에게 역할이 미리 할당되어 있지 않으면 ClickHouse에서 예외가 발생합니다.

할당된 모든 역할을 기본 역할로 설정합니다:

```sql
ALTER USER user DEFAULT ROLE ALL
```

앞으로 사용자에게 역할이 할당되면 해당 역할은 자동으로 기본 역할이 됩니다.

`role1` 및 `role2`를 제외한 할당된 모든 역할을 기본 역할로 설정하세요:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

`john` 계정 사용자가 자신의 권한을 `jack` 계정 사용자에게 부여할 수 있게 합니다:

```sql
ALTER USER john GRANTEES jack;
```

기존 인증 메서드는 유지한 채 사용자에게 새 인증 메서드를 추가합니다:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

참고:

1. 이전 버전의 ClickHouse는 여러 인증 메서드 구문을 지원하지 않을 수 있습니다. 따라서 ClickHouse 서버에 이런 사용자가 있는 상태에서 해당 기능을 지원하지 않는 버전으로 다운그레이드하면, 해당 사용자는 더 이상 사용할 수 없게 되고 일부 사용자 관련 작업도 정상적으로 수행되지 않게 됩니다. 문제 없이 다운그레이드하려면, 다운그레이드 전에 모든 사용자가 단일 인증 메서드만 사용하도록 설정해야 합니다. 또는 적절한 절차 없이 서버를 다운그레이드한 경우에는 문제가 있는 사용자를 삭제해야 합니다.
2. 보안상의 이유로 `no_password`는 다른 인증 메서드와 함께 사용할 수 없습니다.
   따라서 `no_password` 인증 메서드를 `ADD`하는 것은 불가능합니다. 아래 쿼리는 오류를 발생시킵니다:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

사용자의 인증 메서드를 삭제하고 `no_password`를 사용하려면 아래의 대체 형식으로 지정해야 합니다.

인증 메서드를 재설정하고 쿼리에 지정된 메서드를 추가합니다(`ADD` 키워드 없이 앞에 `IDENTIFIED`를 두었을 때와 같은 효과):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

인증 메서드를 재설정하고 가장 최근에 추가한 메서드만 유지하십시오:

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## VALID UNTIL 절
</div>

인증 메서드의 만료 날짜와 필요에 따라 시간을 지정할 수 있습니다. 문자열을 매개변수로 사용합니다. datetime에는 `YYYY-MM-DD [hh:mm:ss] [timezone]` 포맷을 사용하는 것이 좋습니다. 이 매개변수의 기본값은 `'infinity'`입니다.
`VALID UNTIL` 절은 인증 메서드와 함께 지정해야 합니다. 단, 쿼리에서 인증 메서드를 지정하지 않은 경우는 예외입니다. 이 경우 `VALID UNTIL` 절은 기존의 모든 인증 메서드에 적용됩니다.

예시:

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`