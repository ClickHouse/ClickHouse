---
description: 'ClickHouse용 LDAP 인증 구성 가이드'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

LDAP 서버를 사용해 ClickHouse 사용자를 인증할 수 있습니다. 이를 위한 방법은 두 가지입니다.

* `users.xml` 또는 로컬 access control 경로에 정의된 기존 사용자에 대해 LDAP를 외부 인증자(external authenticator)로 사용합니다.
* LDAP를 외부 사용자 디렉터리로 사용하고, 로컬에 정의되지 않은 사용자가 LDAP 서버에 존재하면 인증을 허용합니다.

두 방법 모두에서, 구성의 다른 부분이 이를 참조할 수 있도록 ClickHouse 구성에 내부에서 사용할 이름이 지정된 LDAP 서버를 정의해야 합니다.

<div id="ldap-server-definition">
  ## LDAP 서버 정의
</div>

LDAP 서버를 정의하려면 `config.xml`에 `ldap_servers` 섹션을 추가해야 합니다.

**예시**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

`ldap_servers` 섹션에서 서로 다른 이름으로 여러 LDAP 서버를 정의할 수 있습니다.

**매개변수**

| 매개변수                           | 기본값           | 설명                                                                                                                                                                                                                                                                                                   |
| ------------------------------ | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                         | —             | LDAP 서버 호스트명 또는 IP입니다. 이 매개변수는 필수이며 비워 둘 수 없습니다.                                                                                                                                                                                                                                                 |
| `port`                         | `636` / `389` | LDAP 서버 포트입니다. `enable_tls`가 `yes`로 설정된 경우 기본값은 `636`이고, 그렇지 않으면 `389`입니다.                                                                                                                                                                                                                       |
| `bind_dn`                      | —             | bind에 사용할 DN을 구성하는 템플릿입니다. 각 authentication 시도 시 템플릿의 모든 `{user_name}` substring이 실제 사용자 이름으로 치환되어 최종 DN이 구성됩니다.                                                                                                                                                                                     |
| `auth_dn_prefix`               | —             | **Deprecated.** `bind_dn`의 대안입니다. `bind_dn`과 함께 사용할 수 없습니다. 지정하면 bind DN은 `auth_dn_prefix + {user_name} + auth_dn_suffix`로 구성됩니다. 예를 들어 `auth_dn_prefix`를 `uid=`로, `auth_dn_suffix`를 `,ou=users,dc=example,dc=com`으로 설정하는 것은 `bind_dn`을 `uid={user_name},ou=users,dc=example,dc=com`으로 설정하는 것과 같습니다. |
| `auth_dn_suffix`               | —             | **Deprecated.** `auth_dn_prefix`를 참조하십시오.                                                                                                                                                                                                                                                            |
| `verification_cooldown`        | `0`           | bind에 성공한 후 일정 시간(초) 동안 LDAP 서버에 문의하지 않고도 이후의 모든 요청에 대해 사용자가 계속 성공적으로 authentication된 것으로 간주됩니다. 캐싱을 비활성화하고 각 authentication 요청마다 LDAP 서버에 강제로 문의하려면 `0`을 지정하십시오.                                                                                                                            |
| `follow_referrals`             | `false`       | LDAP 클라이언트 라이브러리가 server가 반환한 LDAP referral을 자동으로 따라가도록 허용하는 flag입니다. 주로 Microsoft Active Directory 환경에서 상위 base DN(예: `DC=example,DC=com`)에 대해 subtree search를 수행할 때 referral/search reference(예: `DC=DomainDnsZones,...`)가 반환될 수 있는 경우와 관련이 있습니다. 파티션 간 search가 명시적으로 필요한 경우에만 `true`로 설정하십시오.     |
| `enable_tls`                   | `yes`         | LDAP 서버에 대한 보안 connection 사용을 활성화하는 flag입니다. 일반 텍스트 `ldap://` protocol에는 `no`(권장하지 않음), SSL/TLS를 사용하는 LDAP `ldaps://` protocol에는 `yes`(권장), 레거시 StartTLS protocol(일반 텍스트 `ldap://` protocol을 TLS로 업그레이드)에 대해서는 `starttls`를 지정하십시오.                                                               |
| `tls_minimum_protocol_version` | `tls1.2`      | SSL/TLS의 최소 protocol version입니다. 허용되는 값: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`.                                                                                                                                                                                                               |
| `tls_require_cert`             | `demand`      | SSL/TLS 피어 certificate verification 동작입니다. 허용되는 값: `never`, `allow`, `try`, `demand`.                                                                                                                                                                                                                |
| `tls_cert_file`                | —             | certificate file 경로입니다.                                                                                                                                                                                                                                                                              |
| `tls_key_file`                 | —             | certificate key file 경로입니다.                                                                                                                                                                                                                                                                          |
| `tls_ca_cert_file`             | —             | CA certificate file 경로입니다.                                                                                                                                                                                                                                                                           |
| `tls_ca_cert_dir`              | —             | CA certificates가 들어 있는 directory 경로입니다.                                                                                                                                                                                                                                                              |
| `tls_cipher_suite`             | —             | 허용되는 cipher suite(OpenSSL 표기법)입니다.                                                                                                                                                                                                                                                                   |
| `search_limit`                 | `256`         | 이 server definition에서 수행하는 LDAP search 쿼리(user DN 감지 및 역할 매핑용)로 반환할 수 있는 최대 entries 수입니다.                                                                                                                                                                                                            |

**`user_dn_detection` 하위 매개변수**

바인드된 사용자의 실제 user DN을 감지하기 위한 LDAP search 매개변수 섹션입니다. 이는 주로 server가 Active Directory인 경우 추가 역할 매핑을 위한 search filter에서 사용됩니다. 최종 user DN은 `{user_dn}` substring을 사용할 수 있는 모든 위치에서 이를 대체할 때 사용됩니다. 기본적으로 user DN은 bind DN과 같게 설정되지만, search가 수행되면 실제로 감지된 user DN 값으로 업데이트됩니다.

| 매개변수            | 기본값       | 설명                                                                                                                                                                                                    |
| --------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | LDAP search용 base DN을 구성하는 템플릿입니다. LDAP search 중 템플릿의 모든 `{user_name}` 및 `{bind_dn}` substring이 실제 사용자 이름과 bind DN으로 치환되어 최종 DN이 구성됩니다.                                                               |
| `scope`         | `subtree` | LDAP search 범위입니다. 허용되는 값: `base`, `one_level`, `children`, `subtree`.                                                                                                                                |
| `search_filter` | —         | LDAP search용 search filter를 구성하는 템플릿입니다. LDAP search 중 템플릿의 모든 `{user_name}`, `{bind_dn}`, `{base_dn}` substring이 실제 사용자 이름, bind DN, base DN으로 치환되어 최종 filter가 구성됩니다. 특수 문자는 XML에서 올바르게 이스케이프해야 합니다. |

<div id="ldap-external-authenticator">
  ## LDAP 외부 인증자
</div>

원격 LDAP 서버는 로컬에 정의된 사용자(`users.xml` 또는 로컬 access control 경로에 정의된 사용자)의 비밀번호를 검증하는 방법으로 사용할 수 있습니다. 이를 위해 사용자 정의에서 `password` 또는 이와 유사한 섹션 대신, 앞서 정의한 LDAP 서버 이름을 지정하십시오.

로그인을 시도할 때마다 ClickHouse는 제공된 자격 증명을 사용해 [LDAP 서버 정의](#ldap-server-definition)의 `bind_dn` 매개변수에 정의된 지정된 DN에 &quot;bind&quot;를 시도하며, 성공하면 해당 사용자는 인증된 것으로 간주됩니다. 이를 흔히 &quot;simple bind&quot; 메서드라고 합니다.

**예시**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

참고로, 사용자 `my_user`는 `my_ldap_server`를 참조합니다. 이 LDAP 서버는 앞서 설명한 대로 기본 `config.xml` 파일에 구성되어 있어야 합니다.

SQL 기반 [액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage)가 활성화되면, LDAP 서버를 통해 인증되는 사용자도 [CREATE USER](/ko/sql-reference/statements/create/user) SQL 문을 사용해 생성할 수 있습니다.

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## LDAP 외부 사용자 디렉터리
</div>

로컬에 정의된 사용자 외에도 원격 LDAP 서버를 사용자 정의 정보의 원본으로 사용할 수 있습니다. 이렇게 하려면 `config.xml` 파일의 `users_directories` 섹션 안에 있는 `ldap` 섹션에 미리 정의한 LDAP 서버 이름을 지정하십시오([LDAP 서버 정의](#ldap-server-definition) 참조).

로그인을 시도할 때마다 ClickHouse는 먼저 로컬에서 사용자 정의를 찾고 평소와 같이 인증합니다. 사용자가 정의되어 있지 않으면 ClickHouse는 해당 정의가 외부 LDAP 디렉터리에 있다고 간주하고, 제공된 자격 증명을 사용해 LDAP 서버의 지정된 DN에 &quot;bind&quot;를 시도합니다. 이 작업이 성공하면 해당 사용자는 존재하며 인증된 것으로 간주됩니다. 사용자에게는 `roles` 섹션에 지정된 목록의 역할이 할당됩니다. 또한 `role_mapping` 섹션도 구성되어 있으면 LDAP &quot;search&quot;를 수행할 수 있으며, 그 결과를 변환해 역할 이름으로 간주한 다음 사용자에게 할당할 수 있습니다. 이는 SQL 기반 [액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage)가 활성화되어 있고, [CREATE ROLE](/ko/sql-reference/statements/create/role) SQL 문을 사용해 역할이 생성되어 있음을 의미합니다.

**예시**

`config.xml`에 추가합니다.

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

`user_directories` 섹션의 `ldap` 섹션에서 참조하는 `my_ldap_server`는 `config.xml`에 구성된, 사전에 정의된 LDAP 서버여야 합니다([LDAP 서버 정의](#ldap-server-definition) 참조).

**매개변수**

| 매개변수 | Default | Description                                                                                                            |
| --------- | ------- | ---------------------------------------------------------------------------------------------------------------------- |
| `server`  | —       | 위의 `ldap_servers` 구성 섹션에 정의된 LDAP 서버 이름 중 하나입니다. 이 매개변수는 필수이며 비워 둘 수 없습니다.                                             |
| `roles`   | —       | LDAP 서버에서 가져온 각 사용자에게 할당할 로컬 역할 목록이 포함된 섹션입니다. 여기에서 역할을 지정하지 않고 아래의 역할 매핑 과정에서도 할당되지 않으면, 사용자는 인증 후 어떤 작업도 수행할 수 없습니다. |

**`role_mapping` 하위 매개변수**

LDAP search 매개변수와 매핑 규칙이 포함된 섹션입니다. 사용자가 인증하면, 아직 LDAP에 바인딩된 상태에서 `search_filter`와 로그인한 사용자 이름을 사용해 LDAP search가 수행됩니다. 이 검색에서 찾은 각 항목에 대해 지정된 속성 값을 추출합니다. 지정된 접두사로 시작하는 각 속성 값은 접두사를 제거한 뒤, 남은 값이 ClickHouse에 정의된 로컬 역할의 이름이 됩니다. 이 역할은 [CREATE ROLE](/ko/sql-reference/statements/create/role) 문으로 미리 생성되어 있어야 합니다. 동일한 `ldap` 섹션 안에 여러 개의 `role_mapping` 섹션을 정의할 수 있으며, 모두 적용됩니다.

| 매개변수            | 기본값       | 설명                                                                                                                                                                                                                                      |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | LDAP search의 base DN을 구성하는 데 사용하는 Template입니다. 결과 DN은 각 LDAP search 시 템플릿의 모든 `{user_name}`, `{bind_dn}`, `{user_dn}` 부분 문자열을 실제 사용자 이름, bind DN, user DN으로 치환하여 생성됩니다.                                                                 |
| `scope`         | `subtree` | LDAP search의 범위입니다. 허용되는 값: `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                 |
| `search_filter` | —         | LDAP search의 search filter를 구성하는 데 사용하는 Template입니다. 결과 filter는 각 LDAP search 시 템플릿의 모든 `{user_name}`, `{bind_dn}`, `{user_dn}`, `{base_dn}` 부분 문자열을 실제 사용자 이름, bind DN, user DN, base DN으로 치환하여 생성됩니다. XML에서는 특수 문자를 올바르게 이스케이프해야 합니다. |
| `attribute`     | `cn`      | LDAP search에서 반환할 값이 들어 있는 속성 이름입니다.                                                                                                                                                                                                    |
| `prefix`        | 비어 있음     | LDAP search에서 반환된 원본 문자열 목록에서 각 문자열 앞에 있을 것으로 예상되는 prefix입니다. 이 prefix는 원본 문자열에서 제거되며, 결과 문자열은 로컬 역할 이름으로 처리됩니다.                                                                                                                        |