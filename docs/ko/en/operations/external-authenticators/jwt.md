---
description: 'ClickHouse Cloud의 JWT 기반 인증 및 임시 사용자 가이드'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'JWT 인증'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ClickHouse는 JSON Web Token(JWT)을 사용해 사용자를 인증할 수 있습니다. [LDAP](/ko/operations/external-authenticators/ldap)나 [Kerberos](/ko/operations/external-authenticators/kerberos) 같은 다른 외부 인증 방식과 달리, JWT 인증은 미리 존재하는 사용자의 아이덴티티를 검증하지 않습니다. 대신 각 토큰에 내장된 클레임을 기반으로 **임시 사용자**를 동적으로 생성합니다. 이러한 사용자는 메모리에만 존재하며, 토큰 클레임에서 파생된 접근 권한을 부여받고, 토큰이 만료되면 자동으로 제거됩니다.

따라서 JWT 인증은 password-based 또는 certificate-based 방식과 본질적으로 다릅니다. `CREATE USER ... IDENTIFIED WITH jwt` statement는 존재하지 않으며, 이를 시도하면 예외가 발생합니다. JWT 사용자는 전적으로 토큰 수명 주기에 따라 관리됩니다.

<div id="overview">
  ## 개요
</div>

인증 흐름은 다음과 같습니다:

1. 클라이언트는 지원되는 전송 메커니즘 중 하나(HTTP `Authorization: Bearer` 헤더, TCP 네이티브 프로토콜 또는 gRPC `jwt` 필드)를 통해 서명된 JWT를 제시합니다.
2. ClickHouse가 토큰 서명을 검증합니다.
3. 필수 클레임(`exp`, `iat`, `iss`, `sub`, `aud`)을 검증합니다.
4. 메모리에 임시 사용자를 생성하며, 이 사용자의 접근 권한은 `clickhouse:grants` 및 `clickhouse:roles` 토큰 클레임에서 파생되고 권한 제한과의 교집합으로 결정됩니다.
5. 토큰이 만료되면 백그라운드 가비지 컬렉션 작업이 해당 사용자를 제거합니다.

<div id="token-claims">
  ## 토큰 클레임
</div>

<div id="required-claims">
  ### 필수 클레임
</div>

ClickHouse에 제시되는 모든 JWT에는 다음 클레임이 포함되어야 합니다.

| 클레임   | 설명                                                        |
| ----- | --------------------------------------------------------- |
| `alg` | 서명 알고리즘(헤더 클레임)입니다. 지원되는 값: `HS256`, `RS256`, `ES256`입니다. |
| `exp` | 만료 시각입니다. 임시 사용자의 `valid_until`을 설정합니다.                   |
| `iat` | 발급 시각입니다. 동일한 아이덴티티에 대해 이전 토큰이 재사용되는 것을 방지하는 데 사용됩니다.     |
| `iss` | 발급자입니다. 프로바이더에 설정된 예상 발급자와 대조됩니다.                         |
| `sub` | 주체입니다. 생성된 username의 일부가 됩니다.                             |
| `aud` | 대상입니다. 프로바이더에 설정된 예상 audience와 대조됩니다.                     |

JWKS 기반 키 확인을 사용하는 경우 `kid`(키 ID) 헤더 클레임도 필요합니다.

:::note JWKS 모드는 RSA 키만 지원합니다
정적 키 프로바이더는 `HS256`, `RS256`, `ES256`을 모두 허용하지만, JWKS 기반 프로바이더는 `kty`가 `RSA`인 JWK만 허용합니다(즉, `RS256`으로 서명된 토큰만 허용). HMAC(`HS256`) 또는 EC(`ES256`) 키로 서명된 토큰은 JWKS endpoint를 통해 검증할 수 없으므로 거부됩니다.
:::

<div id="other-recognized-claims">
  ### 기타 인식되는 클레임
</div>

| 클레임   | 설명                                                       |
| ----- | -------------------------------------------------------- |
| `nbf` | 유효 시작 시각입니다. 이 클레임은 필수는 아니지만, 존재할 경우 이 시각 이전의 토큰은 거부됩니다. |
| `jti` | 예약됨. 토큰에서는 허용되지만 현재는 검증하거나 사용하지 않습니다.                    |

<div id="optional-claims">
  ### 선택적 클레임
</div>

| 클레임                                                                                  | 기본 이름               | 설명                                                                                                                |
| ------------------------------------------------------------------------------------ | ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| 권한 부여                                                                                | `clickhouse:grants` | SQL `GRANT` 프래그먼트로 이루어진 JSON 배열입니다. 예: `["SELECT ON db.*", "INSERT ON db.table1"]`. 각 요소는 `GRANT` 구문의 본문으로 해석됩니다. |
| 역할                                                                                   | `clickhouse:roles`  | 할당할 역할 이름의 JSON 배열입니다. 예: `["analyst", "reader"]`.                                                                |
| IdP(Identity Provider)에서 다른 명명 규칙을 사용하는 경우 기본 클레임 이름을 사용자 지정 클레임 이름으로 다시 매핑할 수 있습니다. |                     |                                                                                                                   |

<div id="example-token-header-and-payload">
  ### 예시 token header와 payload
</div>

```json
{
  "alg": "RS256",
  "kid": "my-key-id"
}
```

```json
{
  "iss": "https://idp.example.com",
  "sub": "jane.doe",
  "aud": "my-clickhouse-cluster",
  "exp": 1719504000,
  "iat": 1719500400,
  "clickhouse:grants": ["SELECT ON analytics.*", "INSERT ON analytics.events"],
  "clickhouse:roles": ["analyst"]
}
```

<div id="ephemeral-user-behavior">
  ## 임시 사용자의 동작 방식
</div>

JWT 사용자는 일반 ClickHouse 사용자와 몇 가지 중요한 차이점이 있습니다.

<div id="identity-and-naming">
  ### 아이덴티티와 이름 지정
</div>

각 JWT 사용자는 `iss`, `sub`, `aud` 클레임(claims)으로부터 계산된 결정적 UUID를 받습니다. 이 UUID는 로그인해도 **안정적으로 유지됩니다**. 서로 다른 토큰으로 여러 번 로그인하더라도(issuer, 주체, 대상이 같다면) 항상 동일한 UUID를 받습니다.

하지만 username은 **유동적입니다**. 다음과 같이 구성됩니다:

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

`<claims_hash>` 부분은 `clickhouse:roles` 또는 `clickhouse:grants` 클레임이 변경될 때마다 달라집니다. 즉, 동일한 아이덴티티라도 역할이나 권한 부여 Set이 다르면 서로 다른 사용자 이름이 생성됩니다.

<div id="access-rights">
  ### 접근 권한
</div>

실제 적용되는 접근 권한은 다음과 같이 계산됩니다:

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

여기서 `permission_limit`는 상한으로 설정된 기준 Role 또는 사용자가 보유한 접근 권한의 집합입니다. 토큰이 요청한 권한이 이 한도를 초과하면 해당 권한은 별도 알림 없이 자동으로 제외됩니다.

<div id="token-freshness">
  ### 토큰 최신성
</div>

ClickHouse는 각 안정적인 아이덴티티에 대해 가장 최근에 인증된 토큰의 `iat`(issued-at) 클레임을 추적합니다. 저장된 값과 같거나 그보다 이전인 `iat`를 가진 토큰이 제시되면, 서버는 클레임을 다시 평가하지 않고 기존 임시 사용자(ephemeral user)를 재사용합니다. 이렇게 하면 오래된 토큰으로 사용자 권한이 낮아지는 것을 방지할 수 있습니다.

<div id="lifetime-and-garbage-collection">
  ### 수명 및 가비지 컬렉션
</div>

임시 사용자는 토큰이 처음 인증되면 생성되며, `valid_until`(`exp`에서 파생됨)이 지난 후 백그라운드 가비지 컬렉션 작업에 의해 제거됩니다. GC 인터벌은 `gc_interval` 매개변수로 제어되며, 기본값은 5분입니다.

GC 실행 사이에는 만료된 사용자가 `system.users`에 계속 표시될 수 있지만, 더 이상 인증에는 사용할 수 없습니다.

<div id="persistent-access-assignments">
  ### 영구적인 접근 권한 할당
</div>

UUID는 변경되지 않으므로 SQL 문을 사용해 JWT 사용자에게 설정 프로필, 쿼터, 행 정책, 컬럼 마스킹 정책을 할당할 수 있습니다. 이러한 할당은 접근 제어 스토리지(디스크 또는 ZooKeeper)에 유지되며, 토큰이 만료되거나 다시 인증한 후에도 계속 유지됩니다.

현재 사용자 이름을 사용해 사용자를 참조하십시오:

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
해당 아이덴티티의 username 및 UUID는 사용자가 활성 상태인 동안 `system.users`의 `name` 및 `id` 컬럼에서 확인할 수 있습니다.
:::

JWT 사용자는 읽기 전용이므로 `ALTER USER`를 직접 사용할 수 없습니다. 설정 프로필, 쿼터 또는 정책을 할당하려면 위에 표시된 것처럼 `ALTER SETTINGS PROFILE`, `ALTER QUOTA` 또는 `ALTER ROW POLICY` SQL 문을 사용하십시오.

<div id="differences-from-regular-users">
  ## 일반 사용자와의 차이점
</div>

| 기능                                    | JWT 사용자                           | 일반 사용자                  |
| ------------------------------------- | --------------------------------- | ----------------------- |
| 생성                                    | 토큰 claims를 기반으로 자동 생성             | `CREATE USER` 문         |
| 저장소                                   | 인메모리 전용(임시)                       | 디스크, ZooKeeper 또는 구성 파일 |
| `CREATE USER ... IDENTIFIED WITH jwt` | 지원되지 않음(예외 발생)                    | 그 외 모든 인증 유형 지원         |
| `ALTER USER` / `DROP USER`            | 지원되지 않음                           | 지원됨                     |
| 백업 및 복원                               | 포함되지 않음                           | 포함됨                     |
| 사용자 이름                                | 자동 생성되며 유동적임                      | 관리자가 지정하며 고정됨           |
| UUID                                  | `iss`+`sub`+`aud`를 기준으로 결정적으로 생성됨 | 생성 시 무작위로 생성됨           |
| 수명                                    | 토큰 `exp`로 제한됨                     | 명시적으로 삭제될 때까지 유지됨       |
| 접근 권한                                 | 토큰 claims에서 파생되며 권한 한도로 상한이 정해짐   | `GRANT`를 통해 명시적으로 부여됨   |
| 호스트 제한                                | 제공자별 네트워크 구성                      | 사용자별 `HOST` 절           |
| 설정 프로필                                | UUID로 할당 가능(영구적)                  | 직접 구성 가능                |
| 쿼터 및 행 정책                             | UUID로 할당 가능(영구적)                  | 직접 구성 가능                |
| 기본 역할                                 | 구성할 수 없음                          | 구성 가능                   |

<div id="sql-security-definer-views">
  ## SQL SECURITY DEFINER 뷰
</div>

임시 JWT 사용자가 `SQL SECURITY DEFINER`로 뷰를 생성하면, 서버는 해당 뷰의 definer 역할을 하도록 사용자의 영구적인 섀도 복사본을 자동으로 생성합니다. 이 섀도 사용자는 다음과 같은 특성을 가집니다.

* 이름은 `<original_jwt_username>:definer`입니다
* `NO_AUTHENTICATION`이 설정됩니다(로그인에 사용할 수 없음)
* 뷰가 생성된 시점의 원래 JWT 사용자와 동일한 접근 권한을 유지합니다

이렇게 하면 임시 사용자의 토큰이 만료되고 원래 사용자가 가비지 컬렉션으로 제거된 뒤에도 뷰가 계속 동작합니다.

<div id="client-usage">
  ## 클라이언트 사용법
</div>

<div id="passing-token-directly">
  ### 토큰을 직접 전달하기
</div>

`clickhouse-client`에서 `--jwt` 플래그를 사용해 미리 발급받은 토큰으로 인증합니다:

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
`--jwt` 플래그는 `--user`와 함께 사용할 수 없습니다. `--jwt`를 지정하면 username은 토큰에서 가져옵니다.
:::

<div id="http-interface">
  ### HTTP 인터페이스
</div>

토큰을 `Authorization` 헤더에 Bearer 토큰으로 전송합니다.

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
항상 HTTPS를 통해 JWT를 전송하십시오. 일반 HTTP로 전송한 Bearer token은 네트워크 경로상의 누구에게나 노출될 수 있으며, 이는 자격 증명을 유출하는 것과 같습니다.
:::

<div id="oauth2-device-code-login">
  ### OAuth2 디바이스 코드 로그인
</div>

`clickhouse-client`는 `--login` 플래그를 통해 대화형 OAuth2 디바이스 코드 흐름을 지원합니다. ClickHouse Cloud 엔드포인트의 경우, 클라이언트는 ClickHouse 전용 JWT를 얻기 위해 자동으로 토큰 교환을 수행합니다. 토큰은 세션 중에 자동으로 갱신됩니다. 새 토큰을 획득하면 클라이언트는 자동으로 다시 연결합니다.

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## ClickHouse Cloud 기본 제공 JWT authenticator
</div>

모든 ClickHouse Cloud 서비스에는 SQL 콘솔과 `clickhouse-client` `--login` 흐름에서 사용하는 미리 정의된 JWT authenticator가 포함되어 있습니다. 이 authenticator는 다음과 같이 구성됩니다.

| 매개변수             | 값                                    |
| ---------------- | ------------------------------------ |
| `iss` (발급자)   | `ClickHouse`                         |
| `aud` (대상) | 서비스 UUID (Cloud Console URL에서 확인 가능) |
| `sub` (주체)  | ClickHouse Cloud 계정 이메일 주소           |

기본 제공 authenticator의 권한 제한은 `default_role` Role과 `default` USER로 설정되어 있습니다. 즉, 모든 JWT 사용자의 실효 권한은 이 두 엔터티가 보유한 권한 부여와의 교집합으로 제한되므로, 토큰이 `default_role` 및 `default`에 허용된 범위를 넘어 권한을 상승시킬 수 없습니다.

이 authenticator를 사용하기 위해 별도로 구성할 사항은 없습니다. 서비스가 생성될 때 자동으로 프로비저닝됩니다.

<div id="interserver-communication">
  ## 서버 간 통신
</div>

쿼리가 다른 세그먼트 또는 레플리카로 전달되면 JWT 토큰이 서버 간 프로토콜에 포함됩니다. 원격 노드는 토큰을 자체적으로 다시 인증하여 자체 임시 사용자를 생성합니다.

<div id="troubleshooting">
  ## 문제 해결
</div>

* **접근 권한이 부여되지 않음:** 참조된 역할 또는 USER에 필요한 권한이 부여되지 않았을 수 있습니다. `clickhouse:roles`에 지정된 역할이 존재하며, 적절한 권한 부여를 포함하는지 확인하십시오.
* **토큰이 거부됨:** 토큰의 `iss`, `aud`, 서명 알고리즘이 JWT 제공자가 기대하는 값과 일치하는지 확인하십시오. JWKS를 사용하는 경우 토큰의 `kid`가 제공자의 키 집합에 있는 key와 일치하는지 확인하십시오.
* **쿼리 사이에 사용자가 사라짐:** 임시 사용자는 토큰이 만료되면 제거됩니다. 장시간 유지되는 세션(session)에는 토큰 갱신을 지원하는 클라이언트(예: `--login` mode)를 사용하십시오.
* **`CREATE USER ... IDENTIFIED WITH jwt` 실패:** 이는 예상된 동작입니다. JWT 사용자는 DDL로 생성할 수 없습니다. 토큰 수명 주기에 따라 전적으로 관리됩니다.