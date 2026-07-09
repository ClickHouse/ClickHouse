---
description: '사용자 및 역할 구성을 위한 설정입니다.'
sidebar_label: '사용자 설정'
sidebar_position: 63
slug: /operations/settings/settings-users
title: '사용자 및 역할 설정'
doc_type: 'reference'
---

`users.xml` 설정 파일의 `users` 섹션에는 사용자 설정이 포함되어 있습니다.

:::note
ClickHouse는 사용자 관리를 위한 [SQL 기반 워크플로우](/ko/operations/access-rights#access-control-usage)도 지원합니다. 이 방법을 사용하실 것을 권장합니다.
:::

`users` 섹션의 구조:

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

비밀번호는 평문 또는 SHA256(16진수 포맷)으로 지정할 수 있습니다.

* 비밀번호를 평문으로 지정하려면(**권장하지 않음**) `password` 요소에 넣으십시오.

  예를 들어, `<password>qwerty</password>`입니다. 비밀번호는 비워 둘 수 있습니다.

<a id="password_sha256_hex" />

* SHA256 해시를 사용해 비밀번호를 지정하려면 `password_sha256_hex` 요소에 넣으십시오.

  예를 들어, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`입니다.

  셸에서 비밀번호를 생성하는 예시는 다음과 같습니다.

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  결과의 첫 번째 줄은 비밀번호이고, 두 번째 줄은 해당 SHA256 해시입니다.

<a id="password_double_sha1_hex" />

* MySQL 클라이언트와의 호환성을 위해 비밀번호를 double SHA1 해시로 지정할 수 있습니다. `password_double_sha1_hex` 요소에 넣으십시오.

  예를 들어, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`입니다.

  셸에서 비밀번호를 생성하는 예시는 다음과 같습니다.

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  결과의 첫 번째 줄은 비밀번호이고, 두 번째 줄은 해당 double SHA1 해시입니다.

<div id="totp-authentication-configuration">
  ### TOTP 인증 구성
</div>

시간 기반 일회용 비밀번호(TOTP)는 제한된 시간 동안만 유효한 임시 액세스 코드를 생성하여 ClickHouse 사용자를 인증하는 데 사용할 수 있습니다.
이 TOTP 인증 메서드는 [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) 표준을 준수하므로 Google Authenticator, 1Password 등의 널리 사용되는 TOTP 애플리케이션과 호환됩니다.
비밀번호 기반 인증과 함께 `users.xml` 설정 파일에서 구성할 수 있습니다.
아직 SQL 기반 Access Control에서는 지원되지 않습니다.

TOTP로 인증하려면 TOTP 애플리케이션에서 생성한 일회용 비밀번호와 프라이머리 비밀번호를 함께 제공해야 합니다. 일회용 비밀번호는 `--one-time-password` 명령줄 옵션으로 지정하거나, &#39;+&#39; 문자를 사용해 기본 비밀번호 뒤에 이어 붙일 수 있습니다.
예를 들어 프라이머리 비밀번호가 `some_password`이고 생성된 TOTP 코드가 `345123`이면 ClickHouse에 연결할 때 `--password some_password+345123` 또는 `--password some_password --one-time-password 345123`를 지정할 수 있습니다. 비밀번호를 지정하지 않으면 `clickhouse-client`가 대화형으로 입력을 요청합니다.

사용자에 대해 TOTP 인증을 활성화하려면 `users.xml`에서 `time_based_one_time_password` 섹션을 구성하십시오. 이 섹션에서는 시크릿, 유효 주기, 자릿수, 해시 알고리즘과 같은 TOTP 설정을 정의합니다.

**예시**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
````

이 명령은 users.xml의 secret 필드에 추가할 수 있는 base32 인코딩 비밀값을 생성합니다.

특정 사용자의 TOTP를 활성화하려면 기존 비밀번호 기반 필드(`password` 또는 `password_sha256_hex` 등)에 `time_based_one_time_password` 섹션을 하나 더 추가하십시오.

[qrencode](https://linux.die.net/man/1/qrencode) 도구를 사용하면 TOTP 비밀값에 대한 QR 코드를 생성할 수 있습니다.

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

사용자에 대해 TOTP를 설정한 후에는 위에서 설명한 대로 일회용 비밀번호를 인증 과정의 일부로 사용할 수 있습니다.

### username/ssh-key

이 설정을 사용하면 SSH key를 사용해 인증할 수 있습니다.

`ssh-keygen`으로 생성한 다음과 같은 SSH key가 있다고 가정합니다.

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

`ssh_key` 요소는 다음과 같은 형식이어야 합니다

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

지원되는 다른 알고리즘을 사용하는 경우 `ssh-ed25519` 대신 `ssh-rsa` 또는 `ecdsa-sha2-nistp256`을 사용하십시오.

### 여러 인증 방법

단일 사용자에 대해 `<auth_methods>` 요소를 사용하여 여러 인증 메서드를 구성할 수 있습니다. 이렇게 하면 나열된 메서드 중 어느 하나로 인증할 수 있습니다. 예를 들어, 한 사용자가 비밀번호와 LDAP 자격 증명을 모두 가질 수 있으며, 둘 중 어느 쪽으로 로그인해도 성공합니다.

`<auth_methods>`의 각 하위 요소는 정확히 하나의 인증 유형을 포함하는, 이름을 임의로 지정할 수 있는 래퍼입니다. 래퍼 이름(예: `<method1>`, `<primary>`, `<a1>`)은 중요하지 않으며, 내부의 인증 요소만 사용됩니다.

**예시: 여러 비밀번호**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**예시: 혼합 인증 방식**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

다음 인증 유형이 `<auth_methods>` 내부에서 지원됩니다:

* **`password`** — 평문 비밀번호
* **`password_sha256_hex`** — SHA256 비밀번호 해시값
* **`password_scram_sha256_hex`** — SCRAM-SHA-256 비밀번호 해시값
* **`password_double_sha1_hex`** — double SHA1 비밀번호 해시값
* **`ldap`** — LDAP 서버 인증
* **`kerberos`** — Kerberos 인증
* **`ssl_certificates`** — SSL 인증서 인증
* **`ssh_keys`** — SSH key 인증
* **`http_authentication`** — HTTP 인증

**규칙 및 제한 사항:**

* `<auth_methods>`는 사용자 수준에서 지정된 인증 메서드와 **함께 사용할 수 없습니다**. 둘을 함께 사용하지 말고 한 가지 방식만 사용하십시오.
* `<auth_methods>`에는 최소 1개의 인증 메서드가 포함되어야 합니다.
* `<auth_methods>` 내부의 각 래퍼 요소에는 정확히 하나의 인증 유형만 포함되어야 합니다(`backwards compatibility`를 위해 여러 개를 포함할 수 있는 `<ssh_keys>`는 예외).
* TOTP (`<time_based_one_time_password>`)는 사용자 수준(`<auth_methods>` 외부)에서 지정되며 목록의 모든 비밀번호 기반 메서드에 적용됩니다. TOTP가 활성화되면 최소 1개의 비밀번호 기반 메서드가 필요합니다.

**예시: TOTP와 함께 사용하는 `auth_methods`**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

이 예시에서는 비밀번호 기반 방식(`<password>`)에 TOTP 검증이 적용되며, LDAP 방식은 외부 서버를 대상으로 별도로 인증합니다.

### access_management

이 설정은 사용자에 대해 SQL 기반 [액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage) 사용을 활성화하거나 비활성화합니다.

가능한 값:

* 0 — 비활성화됨.
* 1 — 활성화됨.

기본값: 0.

### 권한 부여

이 설정을 사용하면 선택한 사용자에게 모든 권한을 부여할 수 있습니다.
목록의 각 항목은 권한을 받을 대상을 지정하지 않은 `GRANT` 쿼리여야 합니다.

예시:

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

이 설정은
`dictionaries`, `access_management`, `named_collection_control`, `show_named_collections_secrets`,
`allow_databases` 설정과 동시에 지정할 수 없습니다.

### user_name/networks

사용자가 ClickHouse 서버에 연결할 수 있는 네트워크 목록입니다.

목록의 각 요소는 다음 형식 중 하나일 수 있습니다.

* `<ip>` — IP 주소 또는 네트워크 마스크.

  예시: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — 호스트명.

  예시: `example01.host.ru`.

  액세스를 확인할 때 DNS 쿼리를 수행하며, 반환된 모든 IP 주소를 피어 주소와 비교합니다.

* `<host_regexp>` — 호스트명에 대한 정규식.

  예시: `^example\d\d-\d\d-\d\.host\.ru$`

  액세스를 확인할 때 먼저 피어 주소에 대해 [DNS PTR 쿼리](https://en.wikipedia.org/wiki/Reverse_DNS_lookup)를 수행한 다음 지정된 정규식을 적용합니다. 그런 다음 PTR 쿼리 결과에 대해 다시 DNS 쿼리를 수행하고, 반환된 모든 주소를 피어 주소와 비교합니다. 정규식은 $로 끝나도록 지정할 것을 강력히 권장합니다.

DNS 요청의 모든 결과는 서버가 다시 시작될 때까지 캐시됩니다.

**예시**

모든 네트워크에서 사용자의 액세스를 허용하려면 다음과 같이 지정하십시오:

```xml
<ip>::/0</ip>
```

:::note
방화벽을 올바르게 구성했거나 서버가 인터넷에 직접 연결되어 있지 않은 경우가 아니라면, 모든 네트워크에서의 접근을 허용하는 것은 안전하지 않습니다.
:::

localhost에서만 접근을 허용하려면 다음을 지정하세요:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

사용자에게 설정 프로필을 할당할 수 있습니다. 설정 프로필은 `users.xml` 파일의 별도 섹션에서 구성합니다. 자세한 내용은 [설정 프로필](../../operations/settings/settings-profiles.md)을 참조하십시오.

### user_name/quota

쿼터를 사용하면 일정 주기 동안 리소스 사용량을 추적하거나 제한할 수 있습니다. 쿼터는 `users.xml` 설정 파일의 `quotas`
섹션에서 설정합니다.

사용자에게 쿼터 세트를 할당할 수 있습니다. 쿼터 구성에 대한 자세한 내용은 [Quotas](/ko/operations/quotas)를 참조하십시오.

### user_name/databases

이 섹션에서는 현재 사용자의 `SELECT` 쿼리에 대해 ClickHouse가 반환하는 행을 제한하여 기본적인 행 수준 보안을 구현할 수 있습니다.

**예시**

다음 구성은 사용자 `user1`이 `SELECT` 쿼리 결과로 `id` 필드 값이 1000인 `table1`의 행만 볼 수 있도록 강제합니다.

```xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

`filter`는 [UInt8](../../sql-reference/data-types/int-uint.md) 타입의 값을 반환하는 임의의 표현식일 수 있습니다. 일반적으로 비교와 논리 연산자를 포함합니다. `database_name.table1`에서 `filter`가 0으로 평가되는 행은 이 사용자에게 반환되지 않습니다. 이 필터링은 `PREWHERE` 연산과 호환되지 않으며 `WHERE→PREWHERE` 최적화를 비활성화합니다.

## 역할

미리 정의된 역할은 `user.xml` 설정 파일의 `roles` 섹션에서 생성할 수 있습니다.

`roles` 섹션의 구조:

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

이러한 역할은 `users` 섹션을 통해 사용자에게 부여할 수도 있습니다:

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```