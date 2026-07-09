---
description: '쿼리 수준 설정'
sidebar_label: '쿼리 수준 세션 설정'
slug: /operations/settings/query-level
title: '쿼리 수준 세션 설정'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

특정 설정을 적용하여 SQL 문을 실행하는 방법은 여러 가지가 있습니다.
설정은 여러 계층으로 적용되며, 각 후속 계층은 이전 계층의 설정 값을 재정의합니다.

<div id="order-of-priority">
  ## 우선순위
</div>

설정을 정의할 때의 우선순위는 다음과 같습니다.

1. 설정을 사용자에게 직접 적용하거나 SETTINGS PROFILE 내에 적용

   * SQL(권장)
   * `/etc/clickhouse-server/users.d`에 하나 이상의 XML 또는 YAML 파일 추가

2. 세션 설정

   * ClickHouse Cloud SQL 콘솔 또는
     대화형 모드의 `clickhouse client`에서 `SET setting=value`를 전송합니다. 마찬가지로 HTTP protocol에서도 ClickHouse
     세션을 사용할 수 있습니다. 이렇게 하려면
     `session_id` HTTP 매개변수를 지정해야 합니다.

3. 쿼리 설정

   * 비대화형 모드로 `clickhouse client`를 시작할 때 시작
     매개변수 `--setting=value`를 설정합니다.
   * HTTP API를 사용할 때 CGI 매개변수(`URL?setting_1=value&setting_2=value...`)를 전달합니다.
   * SELECT 쿼리의
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     절에서 설정을 정의합니다. 설정 값은 해당 쿼리에만 적용되며,
     쿼리 실행 후 기본값 또는 이전 값으로 재설정됩니다.

<div id="converting-a-setting-to-its-default-value">
  ## 설정을 기본값으로 되돌리기
</div>

설정을 변경한 뒤 기본값으로 되돌리려면 값을 `DEFAULT`로 지정하십시오. 구문은 다음과 같습니다:

```sql
SET setting_name = DEFAULT
```

예를 들어 `async_insert`의 기본값은 `0`입니다. 이 값을 `1`로 변경했다고 가정해 보겠습니다:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

응답은 다음과 같습니다:

```response
┌─value──┐
│ 1      │
└────────┘
```

다음 명령을 실행하면 값이 다시 0으로 설정됩니다:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

이제 설정이 기본값으로 되돌아왔습니다:

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## 사용자 지정 설정
</div>

일반적인 [설정](/ko/operations/settings/settings.md) 외에도 사용자는 사용자 지정 설정을 정의할 수 있습니다.
사용자 지정 설정을 사용하면 **세션별 매개변수**를 전달할 수 있으며, 이 매개변수는 쿼리, 정책 또는 함수 내에서 참조할 수 있습니다. 다음과 같은 경우에 유용합니다:

* 사용자 아이덴티티 또는 organization을 기준으로 데이터 필터링
* Context에 따라 서로 다른 비즈니스 로직 적용
* 세션 내 여러 쿼리에서 상태 정보를 유지

사용자 지정 설정 이름은 사용자가 정의한 목록에 포함된 미리 정의된 접두사 중 하나로 시작해야 합니다.
접두사 목록은 서버 구성 파일에 정의된 [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes) 서버 설정을 사용해 지정할 수 있습니다.

아래 예시에서는 `SQL_`를 사용자 지정 접두사로 선택했습니다:

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
ClickHouse Cloud에서는 사용자 지정 접두사를 지정할 수 없습니다.
모든 사용자 지정 사용자 설정의 접두사는 `SQL_`입니다.
:::

사용자 지정 설정을 정의하려면 `SET` 명령을 사용하십시오:

```sql
SET SQL_a = 123;
```

사용자 지정 설정의 현재 값을 확인하려면 `getSetting()` 함수를 사용합니다:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## 예시
</div>

이 예시에서는 모두 `async_insert` 설정 값을 `1`로 지정하고,
실행 중인 시스템에서 해당 설정을 확인하는 방법을 보여줍니다.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### SQL을 사용해 사용자에게 설정을 직접 적용하기
</div>

다음은 `async_inset = 1` 설정으로 사용자 `ingester`를 생성하는 예입니다:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### SETTINGS PROFILE과 할당 확인
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### SQL을 사용해 SETTINGS PROFILE을 만들고 사용자에게 할당하기
</div>

다음은 설정 `async_inset = 1`로 프로필 `log_ingest`를 생성하는 예입니다:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

이 명령은 사용자 `ingester`를 생성하고 해당 사용자에게 SETTINGS PROFILE `log_ingest`를 할당합니다:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### XML을 사용해 SETTINGS PROFILE과 사용자를 생성하기
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment-1">
  #### SETTINGS PROFILE 및 할당 확인
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### 세션에 설정 지정하기
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### 쿼리 중 설정 지정하기
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## 관련 항목
</div>

* ClickHouse 설정에 대한 설명은 [설정](/ko/operations/settings/settings.md) 페이지에서 확인하십시오.
* [전역 서버 설정](/ko/operations/server-configuration-parameters/settings.md)