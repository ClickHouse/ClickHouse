---
description: 'USER 문서'
sidebar_label: 'USER'
sidebar_position: 39
slug: /sql-reference/statements/create/user
title: 'CREATE USER'
doc_type: 'reference'
---

[사용자 계정](../../../guides/sre/user-management/index.md#user-account-management)을 생성합니다.

구문:

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime] 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [ROLE role [,...]]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` 절을 사용하면 클러스터 전체에 사용자를 생성할 수 있습니다. 자세한 내용은 [분산 DDL](../../../sql-reference/distributed-ddl.md)을 참조하십시오.

<div id="identification">
  ## 식별
</div>

사용자를 식별하는 방법에는 여러 가지가 있습니다.

* `IDENTIFIED WITH no_password`
* `IDENTIFIED WITH plaintext_password BY 'qwerty'`
* `IDENTIFIED WITH sha256_password BY 'qwerty'` 또는 `IDENTIFIED BY 'password'`
* `IDENTIFIED WITH sha256_hash BY 'hash'` 또는 `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
* `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
* `IDENTIFIED WITH double_sha1_hash BY 'hash'`
* `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
* `IDENTIFIED WITH bcrypt_hash BY 'hash'`
* `IDENTIFIED WITH ldap SERVER 'server_name'`
* `IDENTIFIED WITH kerberos` 또는 `IDENTIFIED WITH kerberos REALM 'realm'`
* `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
* `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
* `IDENTIFIED WITH http SERVER 'http_server'` 또는 `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
* `IDENTIFIED BY 'qwerty'`

비밀번호 복잡도 요구 사항은 [config.xml](/ko/operations/configuration-files)에서 수정할 수 있습니다. 아래는 비밀번호가 최소 12자 이상이며 숫자 1개를 포함해야 하는 예시 구성입니다. 각 비밀번호 복잡도 규칙에는 비밀번호와 대조할 정규식 및 해당 규칙에 대한 설명이 필요합니다.

```xml
<clickhouse>
    <password_complexity>
        <rule>
            <pattern>.{12}</pattern>
            <message>be at least 12 characters long</message>
        </rule>
        <rule>
            <pattern>\p{N}</pattern>
            <message>contain at least 1 numeric character</message>
        </rule>
    </password_complexity>
</clickhouse>
```

:::note
ClickHouse Cloud에서는 기본적으로 비밀번호가 다음 복잡성 요구 사항을 충족해야 합니다:

* 12자 이상이어야 합니다
* 숫자를 1개 이상 포함해야 합니다
* 대문자를 1개 이상 포함해야 합니다
* 소문자를 1개 이상 포함해야 합니다
* 특수 문자를 1개 이상 포함해야 합니다
  :::

<div id="examples">
  ## 예시
</div>

1. 다음 사용자 이름은 `name1`이며 비밀번호가 필요하지 않습니다. 물론 이는 보안상 거의 도움이 되지 않습니다:

   ```sql
   CREATE USER name1 NOT IDENTIFIED
   ```

2. 평문 비밀번호를 지정하려면 다음과 같이 합니다:

   ```sql
   CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
   ```

   :::tip
   비밀번호는 `/var/lib/clickhouse/access`의 SQL 텍스트 파일에 저장되므로 `plaintext_password`를 사용하는 것은 좋은 방법이 아닙니다. 대신 다음 예시와 같이 `sha256_password`를 사용해 보십시오...
   :::

3. 가장 일반적인 방법은 SHA-256으로 해시된 비밀번호를 사용하는 것입니다. `IDENTIFIED WITH sha256_password`를 지정하면 ClickHouse가 비밀번호를 대신 해시합니다. 예를 들면 다음과 같습니다:

   ```sql
   CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
   ```

   이제 `name3` 사용자는 `my_password`로 로그인할 수 있지만, 비밀번호는 위의 해시 값으로 저장됩니다. 다음 SQL 파일이 `/var/lib/clickhouse/access`에 생성되며 서버 시작 시 실행됩니다:

   ```bash
   /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
   ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
   ```

   :::tip
   사용자 이름에 대한 해시 값과 해당 salt 값을 이미 생성한 경우 `IDENTIFIED WITH sha256_hash BY 'hash'` 또는 `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`를 사용할 수 있습니다. `SALT`를 사용해 `sha256_hash`로 식별하는 경우 해시는 &#39;password&#39;와 &#39;salt&#39;를 이어 붙인 값으로부터 계산되어야 합니다.
   :::

4. `double_sha1_password`는 일반적으로 필요하지 않지만, 이를 요구하는 클라이언트(예: MySQL interface)와 작업할 때 유용합니다:

   ```sql
   CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
   ```

   ClickHouse는 다음 쿼리를 생성하고 실행합니다:

   ```response
   CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
   ```

5. `bcrypt_password`는 비밀번호 저장에 가장 안전한 옵션입니다. [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) 알고리즘을 사용하며, 비밀번호 해시가 침해된 경우에도 무차별 대입 공격에 강합니다.

   ```sql
   CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
   ```

   이 메서드에서는 비밀번호 길이가 72자로 제한됩니다.
   해시를 계산하고 비밀번호를 검증하는 데 필요한 연산량과 시간을 정의하는 bcrypt work factor 매개변수는 서버 구성에서 수정할 수 있습니다:

   ```xml
   <bcrypt_workfactor>12</bcrypt_workfactor>
   ```

   work factor는 4에서 31 사이여야 하며, 기본값은 12입니다.

   :::warning
   인증 빈도가 높은 애플리케이션의 경우
   높은 work factor에서 발생하는 bcrypt의 계산 오버헤드를 고려하여
   다른 인증 메서드 사용을 검토하십시오.
   :::

6. 비밀번호 타입은 생략할 수도 있습니다:

   ```sql
   CREATE USER name6 IDENTIFIED BY 'my_password'
   ```

   이 경우 ClickHouse는 서버 구성에 지정된 기본 비밀번호 타입을 사용합니다:

   ```xml
   <default_password_type>sha256_password</default_password_type>
   ```

   사용 가능한 비밀번호 타입은 다음과 같습니다: `plaintext_password`, `sha256_password`, `double_sha1_password`.

7. 여러 인증 메서드를 지정할 수 있습니다:

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

참고:

1. 이전 버전의 ClickHouse는 여러 인증 메서드 구문을 지원하지 않을 수 있습니다. 따라서 ClickHouse 서버에 이러한 사용자가 있는 상태에서 이를 지원하지 않는 버전으로 다운그레이드하면, 해당 사용자는 더 이상 사용할 수 없게 되고 일부 사용자 관련 작업도 정상적으로 수행되지 않습니다. 다운그레이드를 문제없이 수행하려면, 다운그레이드 전에 모든 사용자가 단일 인증 메서드만 갖도록 설정해야 합니다. 또는 서버를 올바른 절차 없이 다운그레이드했다면, 문제가 있는 사용자를 삭제해야 합니다.
2. 보안상의 이유로 `no_password`는 다른 인증 메서드와 함께 사용할 수 없습니다. 따라서 쿼리에서 `no_password`가 유일한 인증 메서드인 경우에만
   `no_password`를 지정할 수 있습니다.

<div id="user-host">
  ## 사용자 호스트
</div>

사용자 호스트는 ClickHouse 서버에 연결할 수 있는 호스트를 의미합니다. 호스트는 `HOST` 쿼리 섹션에서 다음과 같은 방식으로 지정할 수 있습니다.

* `HOST IP 'ip_address_or_subnetwork'` — 사용자는 지정된 IP 주소 또는 [subnetwork](https://en.wikipedia.org/wiki/Subnetwork)에서만 ClickHouse 서버에 연결할 수 있습니다. 예시: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. 프로덕션 환경에서 사용할 때는 `host` 및 `host_regexp`를 사용하면 추가 지연 시간이 발생할 수 있으므로 `HOST IP` 요소(IP 주소와 해당 마스크)만 지정하십시오.
* `HOST ANY` — 사용자는 어느 위치에서나 연결할 수 있습니다. 기본 옵션입니다.
* `HOST LOCAL` — 사용자는 로컬에서만 연결할 수 있습니다.
* `HOST NAME 'fqdn'` — 사용자 호스트는 FQDN으로 지정할 수 있습니다. 예를 들어 `HOST NAME 'mysite.com'`입니다.
* `HOST REGEXP 'regexp'` — 사용자 호스트를 지정할 때 [pcre](http://www.pcre.org/) 정규식을 사용할 수 있습니다. 예를 들어 `HOST REGEXP '.*\.mysite\.com'`입니다.
* `HOST LIKE 'template'` — [LIKE](/ko/sql-reference/functions/string-search-functions#like) 연산자를 사용해 사용자 호스트를 필터링할 수 있습니다. 예를 들어 `HOST LIKE '%'`는 `HOST ANY`와 같고, `HOST LIKE '%.mysite.com'`는 `mysite.com` 도메인의 모든 호스트를 필터링합니다.

호스트를 지정하는 또 다른 방법은 사용자 이름 뒤에 오는 `@` 구문을 사용하는 것입니다. 예시는 다음과 같습니다.

* `CREATE USER mira@'127.0.0.1'` — `HOST IP` 구문과 같습니다.
* `CREATE USER mira@'localhost'` — `HOST LOCAL` 구문과 같습니다.
* `CREATE USER mira@'192.168.%.%'` — `HOST LIKE` 구문과 같습니다.

:::tip
ClickHouse는 `user_name@'address'` 전체를 하나의 사용자 이름으로 처리합니다. 따라서 기술적으로는 동일한 `user_name`에 대해 `@` 뒤의 구성이 다른 여러 사용자를 만들 수 있습니다. 하지만 이렇게 하는 것은 권장하지 않습니다.
:::

<div id="valid-until-clause">
  ## VALID UNTIL 절
</div>

인증 메서드의 만료 날짜와 필요에 따라 시간을 지정할 수 있습니다. 문자열을 매개변수로 받습니다. 날짜/시간에는 `YYYY-MM-DD [hh:mm:ss] [timezone]` 포맷을 사용하는 것이 좋습니다. 여기서 `[timezone]`은 `+09:00` 같은 숫자 오프셋이거나 `UTC`, `GMT`, `Z`, `MSK`, `MSD` 중 하나여야 합니다. `Asia/Tokyo`처럼 이름이 지정된 IANA 시간대는 인식되지 않습니다(아래 참고). 기본값은 `'infinity'`입니다.
`VALID UNTIL` 절은 쿼리에서 인증 메서드를 전혀 지정하지 않은 경우를 제외하면 인증 메서드와 함께만 지정할 수 있습니다. 이 경우 `VALID UNTIL` 절은 기존의 모든 인증 메서드에 적용됩니다.

예시:

* `CREATE USER name1 VALID UNTIL '2025-01-01'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
* `CREATE USER name1 VALID UNTIL 'infinity'`
* `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`

:::note
날짜/시간 문자열은 `parseDateTimeBestEffort`로 파싱되며, 이 함수는 `UTC`, `GMT`, `Z`, `MSK`, `MSD` 시간대 토큰과 `+09:00` 또는 `-05:00` 같은 숫자 오프셋만 인식합니다. `Asia/Tokyo` 또는 `Europe/London`처럼 이름이 지정된 IANA 시간대는 지원되지 않습니다. 또한 고정 오프셋은 일광 절약 시간을 사용하는 지역에서는 IANA 시간대와 동일하지 않으므로, 인코딩하는 해당 날짜에 맞는 올바른 오프셋을 직접 계산해야 합니다.
:::

<div id="grantees-clause">
  ## GRANTEES 절
</div>

이 사용자가 [권한](../../../sql-reference/statements/grant.md#privileges)을 [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax)과 함께 필요한 모든 접근 권한으로 부여받은 경우, 이 사용자로부터 권한을 받을 수 있는 사용자 또는 역할을 지정합니다. `GRANTEES` 절의 옵션은 다음과 같습니다.

* `user` — 이 사용자가 권한을 부여할 수 있는 사용자를 지정합니다.
* `role` — 이 사용자가 권한을 부여할 수 있는 역할을 지정합니다.
* `ANY` — 이 사용자는 누구에게나 권한을 부여할 수 있습니다. 기본 설정입니다.
* `NONE` — 이 사용자는 누구에게도 권한을 부여할 수 없습니다.

`EXCEPT` 표현식을 사용하면 특정 사용자나 역할을 제외할 수 있습니다. 예를 들어, `CREATE USER user1 GRANTEES ANY EXCEPT user2`는 `user1`이 `GRANT OPTION`과 함께 일부 권한을 부여받은 경우 `user2`를 제외한 누구에게나 해당 권한을 부여할 수 있음을 의미합니다.

<div id="examples">
  ## 예시
</div>

비밀번호 `qwerty`가 설정된 사용자 계정 `mira`를 생성합니다:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira`는 ClickHouse 서버가 실행 중인 호스트에서 클라이언트 앱을 실행해야 합니다.

사용자 계정 `john`을 생성하고 역할을 할당하십시오:

```sql
CREATE USER john ROLE role1, role2;
```

사용자 계정 `john`을 생성하고 역할을 할당한 다음, 그중 일부를 기본 역할로 설정합니다:

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE role1;
```

또는

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE ALL EXCEPT role2;
```

사용자 계정 `john`을 생성하고, `jack` 계정의 사용자에게 자신의 권한을 부여할 수 있도록 합니다:

```sql
CREATE USER john GRANTEES jack;
```

쿼리 매개변수로 사용자 계정 `john`을 생성합니다:

```sql
SET param_user=john;
CREATE USER {user:Identifier};
```