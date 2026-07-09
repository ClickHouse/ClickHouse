---
description: '기존에 올바르게 구성된 ClickHouse 사용자는 Kerberos 인증 프로토콜을
  사용해 인증할 수 있습니다.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

기존에 올바르게 구성된 ClickHouse 사용자는 Kerberos 인증 프로토콜을 통해 인증할 수 있습니다.

현재 Kerberos는 `users.xml` 또는 로컬 액세스 제어 경로에 정의된 기존 사용자에 대한 외부 인증 수단으로만 사용할 수 있습니다. 이러한 사용자는 HTTP 요청을 통해서만 사용할 수 있으며, GSS-SPNEGO 메커니즘으로 인증할 수 있어야 합니다.

이 방식을 사용하려면 시스템에서 Kerberos를 구성해야 하며, ClickHouse 구성에서도 이를 활성화해야 합니다.

<div id="enabling-kerberos-in-clickhouse">
  ## ClickHouse에서 Kerberos 활성화
</div>

Kerberos를 활성화하려면 `config.xml`에 `kerberos` 섹션을 추가해야 합니다. 이 섹션에는 추가 매개변수가 포함될 수 있습니다.

<div id="parameters">
  #### 매개변수
</div>

* `principal` - 보안 Context를 수락할 때 획득하여 사용하는 정규 서비스 주체 이름입니다.
  * 이 매개변수는 선택 사항이며, 생략하면 기본 주체가 사용됩니다.

* `realm` - 요청 initiator의 realm이 이 값과 일치하는 경우에만 인증을 허용하도록 제한하는 realm입니다.
  * 이 매개변수는 선택 사항이며, 생략하면 realm 기준의 추가 필터링은 적용되지 않습니다.

* `keytab` - 서비스 keytab 파일의 경로입니다.
  * 이 매개변수는 선택 사항이며, 생략하면 서비스 keytab 파일의 경로를 `KRB5_KTNAME` 환경 변수에 설정해야 합니다.

예시 (`config.xml`에 입력):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

주체를 지정하는 경우:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

realm 기준으로 필터링하는 경우:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
`kerberos` 섹션은 하나만 정의할 수 있습니다. `kerberos` 섹션이 여러 개 있으면 ClickHouse는 Kerberos 인증을 비활성화합니다.
:::

:::note
`principal` 및 `realm` 섹션은 동시에 지정할 수 없습니다. `principal`과 `realm` 섹션이 모두 있으면 ClickHouse는 Kerberos 인증을 비활성화합니다.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## 기존 사용자를 위한 외부 인증 수단으로서의 Kerberos
</div>

Kerberos는 로컬에 정의된 사용자의 아이덴티티를 확인하는 메서드로 사용할 수 있습니다(`users.xml`에 정의된 사용자 또는 로컬 액세스 제어 경로에 정의된 사용자). 현재는 HTTP 인터페이스를 통한 요청에만 Kerberos를 적용할 수 있습니다(GSS-SPNEGO 메커니즘 사용).

Kerberos 주체 이름 형식은 일반적으로 다음 패턴을 따릅니다.

* *primary/instance@REALM*

*/instance* 부분은 0회 이상 나타날 수 있습니다. 인증이 성공하려면 **initiator의 정규 주체 이름에서 *primary* 부분이 Kerberos가 적용된 사용자 이름과 일치해야 합니다**.

<div id="enabling-kerberos-in-users-xml">
  ### `users.xml`에서 Kerberos 활성화
</div>

사용자에 대해 Kerberos 인증을 활성화하려면 사용자 정의에서 `password` 또는 유사한 섹션 대신 `kerberos` 섹션을 지정하십시오.

매개변수:

* `realm` - 인증을 initiator의 realm이 해당 `realm`과 일치하는 요청으로만 제한하는 데 사용되는 realm입니다.
  * 이 매개변수는 선택 사항이며, 생략하면 realm에 따른 추가 필터링은 적용되지 않습니다.

예시 (`users.xml`에 포함됨):

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
Kerberos 인증은 다른 어떤 인증 방식과도 함께 사용할 수 없습니다. `kerberos`와 함께 `password` 같은 다른 섹션이 있으면 ClickHouse가 강제 종료됩니다.
:::

:::info 알림
이제 사용자 `my_user`가 `kerberos`를 사용하므로, 앞서 설명한 대로 기본 `config.xml` 파일에서 Kerberos를 활성화해야 합니다.
:::

<div id="enabling-kerberos-using-sql">
  ### SQL을 사용한 Kerberos 활성화
</div>

ClickHouse에서 [SQL 기반 액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage)가 활성화된 경우, Kerberos로 식별되는 사용자도 SQL 문을 사용해 생성할 수 있습니다.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...또는 realm 기준으로 필터링하지 않고:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```