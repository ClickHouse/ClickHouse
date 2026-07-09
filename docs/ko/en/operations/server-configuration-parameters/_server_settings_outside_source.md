---
title: Source 외부의 서버 설정
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

ClickHouse Cloud 배포에서는 기본적으로 활성화됩니다.

사용 중인 환경에서 이 설정이 기본적으로 활성화되지 않는 경우, ClickHouse 설치 방식에 따라 아래 안내에 따라 활성화하거나 비활성화할 수 있습니다.

**활성화**

비동기 메트릭 로그 이력 수집 [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md)을 수동으로 활성화하려면, 다음 내용으로 `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**비활성화**

`asynchronous_metric_log` 설정을 비활성화하려면, 다음 내용을 사용하여 `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

프록시를 통해 연결된 클라이언트의 인증에 원본 주소를 사용합니다.

:::note
전달된 주소는 쉽게 스푸핑될 수 있으므로 이 설정은 특히 주의해서 사용해야 합니다. 이러한 인증을 허용하는 서버에는 직접 접근하지 말고, 반드시 신뢰할 수 있는 프록시를 통해서만 접근해야 합니다.
:::

<div id="backups">
  ## 백업
</div>

[`BACKUP` 및 `RESTORE`](/ko/operations/backup/overview) SQL 문을 실행할 때 사용하는 백업 설정입니다.

다음 설정은 하위 태그를 통해 구성할 수 있습니다.

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','동일한 호스트에서 여러 백업 작업을 동시에 실행할 수 있는지 결정합니다.', 'true'),
    ('allow_concurrent_restores', 'Bool', '동일한 호스트에서 여러 복원 작업을 동시에 실행할 수 있는지 결정합니다.', 'true'),
    ('allowed_disk', 'String', '`File()`을 사용할 때 백업할 디스크입니다. `File`을 사용하려면 이 설정을 지정해야 합니다.', ''),
    ('allowed_path', 'String', '`File()`을 사용할 때 백업할 경로입니다. `File`을 사용하려면 이 설정을 지정해야 합니다.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', '수집한 메타데이터를 비교한 후 불일치가 있는 경우, 대기(sleep) 전에 메타데이터를 수집하려고 시도하는 횟수입니다.', '2'),
    ('collect_metadata_timeout', 'UInt64', '백업 중 메타데이터를 수집할 때의 제한 시간(밀리초)입니다.', '600000'),
    ('compare_collected_metadata', 'Bool', 'true이면 백업 중 메타데이터가 변경되지 않았는지 확인하기 위해 수집한 메타데이터를 기존 메타데이터와 비교합니다.', 'true'),
    ('create_table_timeout', 'UInt64', '복원 중 테이블 생성 제한 시간(밀리초)입니다.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', '조정된 백업/복원 중 잘못된 version 오류가 발생한 후 재시도할 수 있는 최대 횟수입니다.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '다음 메타데이터 수집 시도 전 최대 대기 시간(밀리초)입니다.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '다음 메타데이터 수집 시도 전 최소 대기 시간(밀리초)입니다.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', '`BACKUP` 명령이 실패하면 ClickHouse는 실패 전에 백업에 이미 복사된 파일을 제거하려고 시도하며, 그렇지 않으면 복사된 파일을 그대로 둡니다.', 'true'),
    ('sync_period_ms', 'UInt64', '조정된 백업/복원을 위한 동기화 주기(밀리초)입니다.', '5000'),
    ('test_inject_sleep', 'Bool', '테스트 관련 대기', 'false'),
    ('test_randomize_order', 'Bool', 'true이면 테스트 목적으로 특정 작업의 순서를 무작위로 섞습니다.', 'false'),
    ('zookeeper_path', 'String', '`ON CLUSTER` 절을 사용할 때 백업 및 복원 메타데이터가 저장되는 ZooKeeper 경로입니다.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS 설정, t.2 AS 유형, t.3 AS 설명, concat('`', t.4, '`') AS 기본값 FROM settings FORMAT Markdown
  */ }

| 설정                                                  | 유형     | 설명                                                                                        | 기본값                   |
| :-------------------------------------------------- | :----- | :---------------------------------------------------------------------------------------- | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | 동일한 호스트에서 여러 백업 작업을 동시에 실행할 수 있는지 결정합니다.                                                  | `true`                |
| `allow_concurrent_restores`                         | Bool   | 동일한 호스트에서 여러 복원 작업을 동시에 실행할 수 있는지 결정합니다.                                                  | `true`                |
| `allowed_disk`                                      | String | `File()`을 사용할 때 백업 대상 디스크입니다. `File`을 사용하려면 이 설정을 반드시 지정해야 합니다.                           | &#96;&#96;            |
| `allowed_path`                                      | String | `File()`을 사용할 때 백업 대상 경로입니다. `File`을 사용하려면 이 설정을 반드시 지정해야 합니다.                            | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | 수집된 메타데이터를 비교한 후 불일치가 있을 경우, 대기 상태로 들어가기 전에 메타데이터 수집을 시도하는 횟수입니다.                         | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | 백업 중 메타데이터를 수집할 때의 제한 시간(밀리초)입니다.                                                         | `600000`              |
| `compare_collected_metadata`                        | Bool   | `true`이면 백업 중 메타데이터가 변경되지 않았는지 확인하기 위해 수집된 메타데이터를 기존 메타데이터와 비교합니다.                        | `true`                |
| `create_table_timeout`                              | UInt64 | 복원 중 테이블 생성 시의 제한 시간(밀리초)입니다.                                                             | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | 조정형 백업/복원 중 잘못된 버전 오류가 발생한 뒤 재시도할 수 있는 최대 횟수입니다.                                          | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 다음 메타데이터 수집 시도 전 대기하는 최대 시간(밀리초)입니다.                                                      | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 다음 메타데이터 수집 시도 전 대기하는 최소 시간(밀리초)입니다.                                                      | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | `BACKUP` 명령이 실패하면 ClickHouse는 실패 전에 이미 백업으로 복사된 파일을 제거하려고 시도합니다. 그렇지 않으면 복사된 파일을 그대로 둡니다. | `true`                |
| `sync_period_ms`                                    | UInt64 | 조정형 백업/복원을 위한 동기화 주기(밀리초)입니다.                                                             | `5000`                |
| `test_inject_sleep`                                 | Bool   | 테스트용 대기 설정입니다.                                                                            | `false`               |
| `test_randomize_order`                              | Bool   | `true`이면 테스트 목적으로 특정 작업의 순서를 무작위로 바꿉니다.                                                   | `false`               |
| `zookeeper_path`                                    | String | `ON CLUSTER` 절을 사용할 때 백업 및 복원 메타데이터를 저장하는 ZooKeeper 내 경로입니다.                              | `/clickhouse/backups` |

이 설정은 기본적으로 다음과 같이 구성됩니다.

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

다양한 백그라운드 풀에서 실행되는 모든 백그라운드 작업에 대한 정보를 포함합니다.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

[Bcrypt algorithm](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/)을 사용하는 `bcrypt_password` 인증 유형의 work factor입니다.
work factor는 해시를 계산하고 비밀번호를 검증하는 데 필요한 연산량과 시간을 결정합니다.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
인증이 매우 빈번하게 발생하는 애플리케이션에서는
work factor가 높을수록 bcrypt의 계산 오버헤드가 커지므로
대체 인증 메서드를 고려하십시오.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

`true`로 설정하면 특정 엔진으로 테이블을 생성할 때 사용자에게 해당 권한이 있어야 합니다. 예: `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
기본적으로는 이전 버전과의 호환성을 위해 특정 테이블 엔진으로 테이블을 생성할 때 권한을 검사하지 않습니다. 하지만 이 값을 `true`로 설정하면 이 동작을 변경할 수 있습니다.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

내장 딕셔너리를 다시 로드하기까지의 인터벌(초)입니다.

ClickHouse는 x초마다 내장 딕셔너리를 다시 로드하므로 서버를 재시작하지 않고도 실행 중에 딕셔너리를 수정할 수 있습니다.

**예시**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## 압축
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 엔진 테이블의 데이터 압축 관련 설정입니다.

:::note
ClickHouse를 막 사용하기 시작했다면 이 설정은 변경하지 않는 것이 좋습니다.
:::

**구성 템플릿**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**`<case>` 필드**:

* `min_part_size` – 데이터 파트(data part)의 최소 크기입니다.
* `min_part_size_ratio` – 데이터 파트 크기와 테이블 크기의 비율입니다.
* `method` – 압축 방식입니다. 허용 값: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – 압축 수준입니다. [코덱](/ko/sql-reference/statements/create/table#general-purpose-codecs)을 참조하십시오.

:::note
여러 개의 `<case>` 섹션을 구성할 수 있습니다.
:::

**조건이 충족되었을 때의 동작**:

* 데이터 파트가 조건 세트와 일치하면 ClickHouse는 지정된 압축 방식을 사용합니다.
* 데이터 파트가 여러 조건 세트와 일치하면 ClickHouse는 가장 먼저 일치한 조건 세트를 사용합니다.

:::note
데이터 파트에 대해 충족되는 조건이 없으면 ClickHouse는 `lz4` 압축을 사용합니다.
:::

**예시**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## encryption
</div>

[암호화 코덱](/ko/sql-reference/statements/create/table#encryption-codecs)에서 사용할 키를 가져오는 명령을 설정합니다. 키(또는 여러 키)는 환경 변수에 저장하거나 설정 파일에 지정해야 합니다.

키는 16바이트 길이의 16진수 값 또는 문자열일 수 있습니다.

**예시**

구성에서 로드:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
설정 파일에 키를 저장하는 것은 권장되지 않습니다. 안전하지 않기 때문입니다. 키는 보안이 적용된 디스크의 별도 설정 파일로 옮기고, 해당 설정 파일을 가리키는 심볼릭 링크를 `config.d/` 폴더에 둘 수 있습니다.
:::

설정에서 로드하는 경우, 키가 16진수일 때:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

환경 변수에서 키를 불러옵니다:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

여기서 `current_key_id`는 암호화에 사용할 현재 키를 지정하며, 지정된 모든 키는 복호화에 사용할 수 있습니다.

각 메서드는 여러 키에 적용할 수 있습니다:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

여기서 `current_key_id`는 암호화에 사용 중인 현재 key를 나타냅니다.

또한 길이가 반드시 12바이트인 nonce를 추가할 수 있습니다(기본적으로 암호화 및 복호화 과정에서는 0바이트로만 구성된 nonce를 사용합니다):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

또는 16진수로 지정할 수 있습니다:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
위에서 언급한 모든 내용은 `aes_256_gcm_siv`에도 적용됩니다(단, 키 길이는 32바이트여야 합니다).
:::

<div id="error_log">
  ## error_log
</div>

기본적으로 비활성화되어 있습니다.

**활성화**

오류 이력 수집 [`system.error_log`](../../operations/system-tables/error_log.md)을 수동으로 활성화하려면, 다음 내용으로 `/etc/clickhouse-server/config.d/error_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**비활성화**

`error_log` 설정을 비활성화하려면 다음 내용으로 `/etc/clickhouse-server/config.d/disable_error_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

[사용자 지정 설정](/ko/operations/settings/query-level#custom_settings)에 사용되는 프리픽스 목록입니다.
여러 프리픽스는 쉼표로 구분합니다.

**예시**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**관련 항목**

* [사용자 지정 설정](/ko/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

코어 덤프 파일 크기의 소프트 리밋을 설정합니다.

:::note
하드 리밋은 시스템 도구를 통해 설정합니다.
:::

**예시**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

기본 설정 프로필입니다. 설정 프로필은 `user_config` 설정에서 지정한 파일에 정의되어 있습니다.

**예시**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

딕셔너리 구성 파일의 경로입니다.

경로:

* 절대 경로 또는 서버 구성 파일을 기준으로 하는 상대 경로를 지정하십시오.
* 경로에는 와일드카드 `*` 및 `?`를 포함할 수 있습니다.

관련 항목:

* &quot;[딕셔너리](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**예시**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

실행형 사용자 정의 함수의 구성 파일 경로입니다.

경로:

* 절대 경로 또는 서버 구성 파일을 기준으로 한 상대 경로를 지정합니다.
* 경로에는 와일드카드 `*` 및 `?`를 사용할 수 있습니다.

관련 항목:

* &quot;[실행형 사용자 정의 함수](/ko/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**예시**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

[Graphite](https://github.com/graphite-project)로 데이터를 전송합니다.

설정:

* `host` – Graphite server입니다.
* `port` – Graphite server의 포트입니다.
* `interval` – 전송 인터벌이며, 초 단위입니다.
* `timeout` – 데이터 전송 timeout이며, 초 단위입니다.
* `root_path` – 키의 접두사입니다.
* `metrics` – [system.metrics](/ko/operations/system-tables/metrics) 테이블의 데이터를 전송합니다.
* `events` – [system.events](/ko/operations/system-tables/events) 테이블에서 시간 범위 동안 누적된 delta 데이터를 전송합니다.
* `events_cumulative` – [system.events](/ko/operations/system-tables/events) 테이블의 누적 데이터를 전송합니다.
* `asynchronous_metrics` – [system.asynchronous&#95;metrics](/ko/operations/system-tables/asynchronous_metrics) 테이블의 데이터를 전송합니다.

여러 개의 `<graphite>` 절을 구성할 수 있습니다. 예를 들어, 서로 다른 데이터를 서로 다른 인터벌로 전송할 때 사용할 수 있습니다.

**예시**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Graphite 데이터 축소 설정입니다.

자세한 내용은 [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md)를 참조하십시오.

**예시**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

사용자 지정 HTTP handler를 사용할 수 있습니다.
새 http handler를 추가하려면 새 `<rule>`를 추가하면 됩니다.
규칙은 정의된 순서대로 위에서 아래로 검사되며,
가장 먼저 일치하는 규칙의 handler가 실행됩니다.
일치 조건이 없는 규칙(`handler`만 있는 경우)은 모든 요청과 일치합니다. 규칙은 순서대로 검사되므로,
이러한 규칙은 마지막에 배치하는 폴백으로만 유용합니다.

다음 설정은 하위 태그로 구성할 수 있습니다(이 하위 태그들은 `handler`를 제외하면 모두 선택 사항입니다):

| Sub-tags             | Definition                                                                                                                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | 요청 URL 경로와 일치시키는 데 사용합니다. 일치 여부를 확인할 때 쿼리 문자열은 무시됩니다                                                                                                                                               |
| `url_prefix`         | 요청 URL 경로를 기준 경로와 일치시키는 데 사용합니다. 즉, 경로 자체 또는 경로 세그먼트 경계를 기준으로 그 하위의 모든 경로와 일치합니다(예: &#39;/api/v1&#39;는 /api/v1, /api/v1/ 및 /api/v1/write와 일치하지만 /api/v1beta와는 일치하지 않음). 일치 여부를 확인할 때 쿼리 문자열은 무시됩니다 |
| `url_regexp`         | 요청 URL 경로를 정규식과 일치시키는 데 사용합니다. 일치 여부를 확인할 때 쿼리 문자열은 무시됩니다                                                                                                                                          |
| `full_url`           | 전체 요청 URL `scheme://host:port/path`와 일치시키는 데 사용합니다. 일치 여부를 확인할 때 쿼리 문자열은 무시되며, host는 `Host` 헤더가 아니라 연결 IP 주소입니다                                                                                    |
| `full_url_prefix`    | 전체 요청 URL `scheme://host:port/path`를 기준 URL `scheme://host:port/base_path`와 경로 세그먼트 경계에서 일치시키는 데 사용합니다(`url_prefix` 참조). 일치 여부를 확인할 때 쿼리 문자열은 무시됩니다                                                |
| `full_url_regexp`    | 전체 요청 URL `scheme://host:port/path`를 정규식과 일치시키는 데 사용합니다. 일치 여부를 확인할 때 쿼리 문자열은 무시됩니다                                                                                                                |
| `methods`            | 요청 메서드를 일치시키는 데 사용합니다. 여러 메서드를 지정할 때는 쉼표로 구분할 수 있습니다                                                                                                                                               |
| `headers`            | 요청 헤더를 일치시키는 데 사용하며, 각 하위 요소를 개별적으로 일치시킵니다(하위 요소 이름은 헤더 이름임)                                                                                                                                       |
| `headers_regexp`     | `headers`와 같지만, 각 하위 요소의 값은 정규식으로 일치시킵니다                                                                                                                                                           |
| `empty_query_string` | URL에 쿼리 문자열이 없는지 확인합니다                                                                                                                                                                             |
| `handler`            | 요청 handler입니다(필수)                                                                                                                                                                                  |

:::note
`url_regexp`, `full_url_regexp`, `headers_regexp` 대신 `url`, `full_url`, `headers`에 `regex:` 접두사를 사용해 정규식을 작성할 수도 있습니다(예: `<url>regex:/api/.*</url>`). 이는 이전 버전과의 호환성을 위해 계속 지원되지만 obsolete 상태입니다. 전용 `url_regexp`, `full_url_regexp`, `headers_regexp` 하위 태그를 사용하는 것이 좋습니다.
:::

`handler`에는 다음 설정이 포함되며, 하위 태그로 구성할 수 있습니다:

| Sub-tags           | Definition                                                                                                                       |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | 리디렉션 대상 위치                                                                                                                       |
| `type`             | 지원되는 타입: static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                       |
| `status`           | static 타입과 함께 사용하며, 응답 status code를 지정합니다                                                                                        |
| `query_param_name` | dynamic&#95;query&#95;handler 타입과 함께 사용하며, HTTP request 파라미터에서 `<query_param_name>` 값에 해당하는 값을 추출해 실행합니다                         |
| `query`            | predefined&#95;query&#95;handler 타입과 함께 사용하며, handler가 호출되면 쿼리를 실행합니다                                                            |
| `content_type`     | static 타입과 함께 사용하며, 응답 content-type을 지정합니다                                                                                       |
| `response_content` | static 타입과 함께 사용하며, 클라이언트로 전송할 Response 내용입니다. 접두사 &#39;file://&#39; 또는 &#39;config://&#39;를 사용하면 파일 또는 구성에서 내용을 찾아 클라이언트로 전송합니다 |

규칙 목록과 함께 `<defaults/>`를 지정하여 모든 기본 handler를 활성화할 수도 있습니다.

예시:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

ClickHouse HTTP(s) server에 접속할 때 기본적으로 표시되는 페이지입니다.
기본값은 &quot;Ok.&quot;입니다(끝에 line feed가 포함됨)

**예시**

`http://localhost: http_port`에 접속하면 `https://tabix.io/`가 열립니다.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

`OPTIONS` HTTP 요청의 응답에 헤더를 추가하는 데 사용됩니다.
`OPTIONS` 메서드는 CORS Preflight 요청을 수행할 때 사용됩니다.

자세한 내용은 [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS)를 참조하십시오.

예시:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

HSTS의 만료 시간(초)입니다.

:::note
값이 `0`이면 ClickHouse에서 HSTS를 비활성화합니다. 양수를 설정하면 HSTS가 활성화되며, max-age는 설정한 값으로 지정됩니다.
:::

**예시**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

ClickHouse 서버 간 데이터 교환을 허용할 호스트를 제한합니다.
Keeper를 사용하는 경우 동일한 제한이 서로 다른 Keeper 인스턴스 간 통신에도 적용됩니다.

:::note
기본적으로 이 값은 [`listen_host`](#listen_host) 설정과 같습니다.
:::

**예시**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

유형:

default:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

[복제](../../engines/table-engines/mergetree-family/replication.md) 중 다른 서버에 연결할 때 사용하는 사용자 이름과 비밀번호입니다. 또한 서버는 이 자격 증명을 사용해 다른 레플리카를 인증합니다.
따라서 `interserver_http_credentials`는 클러스터의 모든 레플리카에서 동일해야 합니다.

:::note

* 기본적으로 `interserver_http_credentials` 섹션을 생략하면 복제 중에는 인증이 사용되지 않습니다.
* `interserver_http_credentials` 설정은 ClickHouse 클라이언트 자격 증명 [구성](../../interfaces/client.md#configuration_files)과는 관련이 없습니다.
* 이 자격 증명은 `HTTP` 및 `HTTPS`를 통한 복제에 공통으로 사용됩니다.
  :::

다음 설정은 하위 태그로 구성할 수 있습니다.

* `user` — 사용자 이름.
* `password` — 비밀번호.
* `allow_empty` — `true`이면 자격 증명이 설정되어 있어도 다른 레플리카가 인증 없이 연결할 수 있습니다. `false`이면 인증되지 않은 연결은 거부됩니다. 기본값: `false`.
* `old` — 자격 증명 교체 중에 사용하는 이전 `user` 및 `password`를 포함합니다. 여러 개의 `old` 섹션을 지정할 수 있습니다.

**자격 증명 교체**

ClickHouse는 구성을 업데이트하기 위해 모든 레플리카를 동시에 중지하지 않고도 interserver 자격 증명을 동적으로 교체할 수 있도록 지원합니다. 자격 증명은 여러 단계에 걸쳐 변경할 수 있습니다.

인증을 활성화하려면 `interserver_http_credentials.allow_empty`를 `true`로 설정하고 자격 증명을 추가하십시오. 그러면 인증이 있는 연결과 없는 연결이 모두 허용됩니다.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

모든 레플리카 구성을 완료한 후 `allow_empty`를 `false`로 설정하거나 이 설정을 제거하십시오. 이렇게 하면 새 자격 증명을 사용한 인증이 필수가 됩니다.

기존 자격 증명을 변경하려면 사용자 이름과 비밀번호를 `interserver_http_credentials.old` 섹션으로 옮기고, `user`와 `password`를 새 값으로 업데이트하십시오. 이 시점부터 서버는 다른 레플리카에 연결할 때 새 자격 증명을 사용하며, 새 자격 증명과 기존 자격 증명 모두로 들어오는 연결을 허용합니다.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

새 자격 증명이 모든 레플리카에 적용된 후에는 기존 자격 증명을 제거할 수 있습니다.

<div id="ldap_servers">
  ## ldap_servers
</div>

여기에 LDAP 서버와 해당 연결 매개변수를 나열하여 다음과 같이 사용할 수 있습니다:

* `password` 대신 `ldap` 인증 메커니즘이 지정된 전용 로컬 사용자의 인증자로 사용
* 원격 사용자 디렉터리로 사용

다음 SETTING은 하위 태그로 구성할 수 있습니다:

| SETTING                          | 설명                                                                                                                                                                                                                                                             |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | bind에 사용할 DN을 구성하는 템플릿입니다. 결과 DN은 각 인증 시도 시 템플릿의 모든 `\{user_name\}` 부분 문자열을 실제 사용자 이름으로 바꾸어 구성됩니다.                                                                                                                                                             |
| `enable_tls`                   | LDAP 서버에 대해 보안 연결을 사용할지 결정하는 플래그입니다. 일반 텍스트(`ldap://`) 프로토콜에는 `no`를 지정하십시오(권장하지 않음). SSL/TLS를 통한 LDAP(`ldaps://`) 프로토콜에는 `yes`를 지정하십시오(권장되며 기본값). 레거시 StartTLS 프로토콜(일반 텍스트(`ldap://`) 프로토콜을 TLS로 업그레이드)에 대해서는 `starttls`를 지정하십시오.                              |
| `host`                         | LDAP 서버 호스트명 또는 IP입니다. 이 매개변수는 필수이며 비워 둘 수 없습니다.                                                                                                                                                                                                               |
| `port`                         | LDAP 서버 포트입니다. `enable_tls`가 true로 설정된 경우 기본값은 636이고, 그렇지 않으면 `389`입니다.                                                                                                                                                                                        |
| `tls_ca_cert_dir`              | CA 인증서가 들어 있는 디렉터리의 경로입니다.                                                                                                                                                                                                                                     |
| `tls_ca_cert_file`             | CA 인증서 파일의 경로입니다.                                                                                                                                                                                                                                              |
| `tls_cert_file`                | 인증서 파일의 경로입니다.                                                                                                                                                                                                                                                 |
| `tls_cipher_suite`             | 허용되는 암호군(cipher suite)입니다(OpenSSL 표기법).                                                                                                                                                                                                                        |
| `tls_key_file`                 | 인증서 키 파일의 경로입니다.                                                                                                                                                                                                                                               |
| `tls_minimum_protocol_version` | SSL/TLS의 최소 프로토콜 버전입니다. 허용되는 값은 `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`(기본값)입니다.                                                                                                                                                                          |
| `tls_require_cert`             | SSL/TLS 피어 인증서 검증 동작입니다. 허용되는 값은 `never`, `allow`, `try`, `demand`(기본값)입니다.                                                                                                                                                                                    |
| `user_dn_detection`            | bind된 사용자의 실제 user DN을 감지하기 위한 LDAP search 매개변수 섹션입니다. 이는 주로 서버가 Active Directory일 때 추가 역할 매핑을 위한 search filter에서 사용됩니다. 결과 user DN은 허용된 위치에서 `\{user_dn\}` 부분 문자열을 치환할 때 사용됩니다. 기본적으로 user DN은 bind DN과 동일하게 설정되지만, search가 수행되면 실제로 감지된 user DN 값으로 업데이트됩니다. |
| `verification_cooldown`        | bind에 성공한 후 일정 시간(초) 동안은 LDAP 서버에 다시 연결하지 않고도 이후의 모든 요청에 대해 사용자가 인증에 성공한 것으로 간주됩니다. 캐싱을 비활성화하고 각 인증 요청마다 LDAP 서버에 강제로 연결하려면 `0`(기본값)을 지정하십시오.                                                                                                                  |

`user_dn_detection` SETTING은 다음 하위 태그로 구성할 수 있습니다:

| SETTING           | 설명                                                                                                                                                                                                          |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | LDAP search의 base DN을 구성하는 템플릿입니다. 결과 DN은 LDAP search 중 템플릿의 모든 `\{user_name\}` 및 `\{bind_dn\}` 부분 문자열을 실제 사용자 이름과 bind DN으로 바꾸어 구성됩니다.                                                                     |
| `scope`         | LDAP search의 범위입니다. 허용되는 값은 `base`, `one_level`, `children`, `subtree`(기본값)입니다.                                                                                                                             |
| `search_filter` | LDAP search의 search filter를 구성하는 템플릿입니다. 결과 filter는 LDAP search 중 템플릿의 모든 `\{user_name\}`, `\{bind_dn\}`, `\{base_dn\}` 부분 문자열을 실제 사용자 이름, bind DN, base DN으로 바꾸어 구성됩니다. 참고로 특수 문자는 XML에서 올바르게 이스케이프해야 합니다. |

예시:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

예시 (추가 역할 매핑을 위해 user DN 감지가 설정된 일반적인 Active Directory):

```xml
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
```

<div id="listen_host">
  ## listen_host
</div>

요청을 받을 수 있는 호스트를 제한합니다. server가 모든 호스트의 요청에 응답하도록 하려면 `::`를 지정하십시오.

예시:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

로그 메시지의 위치와 포맷입니다.

**키**:

| 키                          | Description                                                                                                                                                          |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | `true`(기본값)인 경우 로깅은 비동기로 수행됩니다(출력 채널당 백그라운드 스레드 1개). 그렇지 않으면 `LOG`를 호출한 스레드 내부에서 로깅합니다                                                                               |
| `async_queue_max_size`       | 비동기 로깅을 사용할 때 플러시를 기다리며 큐에 보관되는 메시지의 최대 개수입니다. 이를 초과하는 메시지는 버려집니다                                                                                                    |
| `console`                    | 콘솔 로깅을 활성화합니다. 활성화하려면 `1` 또는 `true`로 설정합니다. ClickHouse가 데몬 모드로 실행되지 않으면 기본값은 `1`이고, 그렇지 않으면 `0`입니다.                                                                  |
| `console_log_level`          | 콘솔 출력용 로그 레벨입니다. 기본값은 `level`입니다.                                                                                                                                    |
| `console_shutdown_log_level` | 종료 레벨은 서버 종료 시 콘솔 로그 레벨을 설정하는 데 사용됩니다.                                                                                                                               |
| `console_startup_log_level`  | 시작 레벨은 서버 시작 시 콘솔 로그 레벨을 설정하는 데 사용됩니다. 시작이 완료되면 로그 레벨은 `console_log_level` 설정으로 되돌아갑니다                                                                               |
| `count`                      | 롤링 정책: ClickHouse가 보관하는 과거 로그 파일의 최대 개수입니다.                                                                                                                          |
| `errorlog`                   | 오류 로그 파일의 경로입니다.                                                                                                                                                     |
| `formatting.type`            | 콘솔 출력용 로그 포맷입니다. 현재는 `json`만 지원됩니다                                                                                                                                   |
| `level`                      | 로그 레벨입니다. 허용되는 값: `none`(로깅 끄기), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                                           |
| `log`                        | 로그 파일의 경로입니다.                                                                                                                                                        |
| `rotation`                   | 롤링 정책: 로그 파일을 언제 롤링할지 제어합니다. 롤링은 크기, 시간 또는 둘의 조합을 기준으로 할 수 있습니다. 예시: 100M, daily, 100M,daily. 로그 파일이 지정된 크기를 초과하거나 지정된 시간 인터벌에 도달하면 파일 이름이 변경되어 보관되고 새 로그 파일이 생성됩니다. |
| `shutdown_level`             | 종료 레벨은 서버 종료 시 루트 로거의 로그 레벨을 설정하는 데 사용됩니다.                                                                                                                           |
| `size`                       | 롤링 정책: 로그 파일의 최대 크기(바이트)입니다. 로그 파일 크기가 이 임계값을 초과하면 파일 이름이 변경되어 보관되고 새 로그 파일이 생성됩니다.                                                                                  |
| `startup_level`              | 시작 레벨은 서버 시작 시 루트 로거의 로그 레벨을 설정하는 데 사용됩니다. 시작이 완료되면 로그 레벨은 `level` 설정으로 되돌아갑니다                                                                                       |
| `stream_compress`            | LZ4를 사용해 로그 메시지를 압축합니다. 활성화하려면 `1` 또는 `true`로 설정합니다.                                                                                                                 |
| `syslog_level`               | syslog로 기록할 때의 로그 레벨입니다.                                                                                                                                             |
| `use_syslog`                 | 로그 출력을 syslog로도 전달합니다.                                                                                                                                               |

**로그 포맷 지정자**

`log` 및 `errorLog` 경로의 파일 이름은 생성되는 파일 이름에서 아래 포맷 지정자를 지원합니다(디렉터리 부분에서는 지원되지 않음).

「예시」 컬럼은 `2023-07-06 18:32:07` 시점의 출력을 보여줍니다.

| 지정자  | 설명                                                                                                                          | 예시                         |
| ---- | --------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%` | 리터럴 %                                                                                                                       | `%`                        |
| `%n` | 줄바꿈 문자                                                                                                                      |                            |
| `%t` | 가로 탭 문자                                                                                                                     |                            |
| `%Y` | 10진수 연도, 예: 2017                                                                                                            | `2023`                     |
| `%y` | 10진수 연도의 마지막 2자리(범위 [00,99])                                                                                                | `23`                       |
| `%C` | 10진수 연도의 앞 2자리(범위 [00,99])                                                                                                  | `20`                       |
| `%G` | 4자리 [ISO 8601 주 기준 연도](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), 즉 지정된 주를 포함하는 연도입니다. 일반적으로 `%V`와 함께 사용할 때만 유용합니다 | `2023`                     |
| `%g` | [ISO 8601 주 기준 연도](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)의 마지막 2자리, 즉 지정된 주를 포함하는 연도입니다.                        | `23`                       |
| `%b` | 축약된 월 이름, 예: Oct (로캘에 따라 다름)                                                                                                | `Jul`                      |
| `%h` | %b의 동의어                                                                                                                     | `Jul`                      |
| `%B` | 전체 월 이름, 예: October (로캘에 따라 다름)                                                                                             | `July`                     |
| `%m` | 10진수 월(범위 [01,12])                                                                                                          | `07`                       |
| `%U` | 연중 주를 10진수로 표시한 값(일요일이 한 주의 첫째 날)(범위 [00,53])                                                                               | `27`                       |
| `%W` | 연중 주를 10진수로 표시한 값(월요일이 한 주의 첫째 날)(범위 [00,53])                                                                               | `27`                       |
| `%V` | ISO 8601 주 번호(범위 [01,53])                                                                                                   | `27`                       |
| `%j` | 연중 일수를 10진수로 표시한 값(범위 [001,366])                                                                                            | `187`                      |
| `%d` | 일을 0으로 채운 10진수로 표시한 값(범위 [01,31])입니다. 한 자리 숫자 앞에는 0이 붙습니다.                                                                  | `06`                       |
| `%e` | 일을 공백으로 채운 10진수로 표시한 값(범위 [1,31])입니다. 한 자리 숫자 앞에는 공백이 붙습니다.                                                                 | `&nbsp; 6`                 |
| `%a` | 축약된 요일 이름, 예: Fri (로캘에 따라 다름)                                                                                               | `Thu`                      |
| `%A` | 전체 요일 이름, 예: Friday (로캘에 따라 다름)                                                                                             | `Thursday`                 |
| `%w` | 일요일을 0으로 하는 정수형 요일 번호(범위 [0-6])                                                                                             | `4`                        |
| `%u` | 월요일을 1로 하는 10진수 요일 번호(ISO 8601 포맷)(범위 [1-7])                                                                                | `4`                        |
| `%H` | 24시간제의 시를 10진수로 표시한 값(범위 [00-23])                                                                                           | `18`                       |
| `%I` | 12시간제의 시를 10진수로 표시한 값(범위 [01,12])                                                                                           | `06`                       |
| `%M` | 분을 10진수로 표시한 값(범위 [00,59])                                                                                                  | `32`                       |
| `%S` | 초를 10진수로 표시한 값(범위 [00,60])                                                                                                  | `07`                       |
| `%c` | 표준 날짜 및 시간 문자열, 예: Sun Oct 17 04:41:13 2010 (로캘에 따라 다름)                                                                     | `Thu Jul  6 18:32:07 2023` |
| `%x` | 지역화된 날짜 표현(로캘에 따라 다름)                                                                                                       | `07/06/23`                 |
| `%X` | 지역화된 시간 표현, 예: 18:40:20 또는 6:40:20 PM (로캘에 따라 다름)                                                                           | `18:32:07`                 |
| `%D` | 짧은 MM/DD/YY 날짜, %m/%d/%y와 동일                                                                                                | `07/06/23`                 |
| `%F` | 짧은 YYYY-MM-DD 날짜로, `%Y-%m-%d`와 동일합니다                                                                                        | `2023-07-06`               |
| `%r` | 로캘에 따른 12시간제 시각                                                                                                             | `06:32:07 PM`              |
| `%R` | &quot;%H:%M&quot;와 동일합니다                                                                                                    | `18:32`                    |
| `%T` | &quot;%H:%M:%S&quot;와 동일합니다(ISO 8601 시각 포맷)                                                                                 | `18:32:07`                 |
| `%p` | 로캘에 따른 오전 또는 오후 표시                                                                                                          | `PM`                       |
| `%z` | ISO 8601 포맷의 UTC 오프셋(예: -0430) 또는 time zone 정보를 사용할 수 없는 경우 아무 문자도 출력하지 않습니다                                                | `+0800`                    |
| `%Z` | 로캘에 따른 time zone 이름 또는 약어이거나, time zone 정보를 사용할 수 없는 경우 아무 문자도 출력하지 않습니다                                                    | `Z AWST `                  |

**예시**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

로그 메시지를 콘솔에만 출력하려면:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**수준별 재정의**

개별 로그 이름마다 로그 레벨을 재정의할 수 있습니다. 예를 들어, 로거 &quot;Backup&quot; 및 &quot;RBAC&quot;의 모든 메시지가 출력되지 않도록 설정할 수 있습니다.

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

로그 메시지를 추가로 syslog에 기록하려면:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

`<syslog>`의 키:

| 키          | 설명                                                                                                                                                                                                                              |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | `host\[:port\]` 포맷의 syslog 주소입니다. 생략하면 로컬 데몬을 사용합니다.                                                                                                                                                                            |
| `hostname` | 로그가 전송되는 호스트의 이름입니다(선택 사항).                                                                                                                                                                                                     |
| `facility` | syslog [facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility)입니다. `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`처럼 &quot;LOG&#95;&quot; 접두사가 붙은 대문자로 지정해야 합니다. 기본값은 `address`를 지정한 경우 `LOG_USER`이고, 그렇지 않으면 `LOG_DAEMON`입니다. |
| `format`   | 로그 메시지 포맷입니다. 가능한 값은 `bsd`와 `syslog`입니다.                                                                                                                                                                                        |

**로그 포맷**

콘솔 로그에 출력할 로그 포맷을 지정할 수 있습니다. 현재는 JSON만 지원합니다.

**예시**

다음은 출력되는 JSON 로그의 예시입니다:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

JSON 로깅 지원을 활성화하려면 다음 스니펫을 사용하십시오:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**JSON 로그의 키 이름 변경**

키 이름은 `<names>` 태그 안의 태그 값을 변경해 수정할 수 있습니다. 예를 들어 `DATE_TIME`을 `MY_DATE_TIME`으로 변경하려면 `<date_time>MY_DATE_TIME</date_time>`을 사용하면 됩니다.

**JSON 로그의 키 생략**

속성을 주석 처리하면 로그 속성을 생략할 수 있습니다. 예를 들어 로그에 `query_id`가 출력되지 않게 하려면 `<query_id>` 태그를 주석 처리하면 됩니다.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

ClickHouse 코어 개발팀에 크래시 보고서를 전송하기 위한 설정입니다.

특히 프로덕션 이전 환경에서는 이 기능을 활성화해 주시면 큰 도움이 됩니다.

키:

| 키                    | Description                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------ |
| `enabled`             | 기능을 활성화하는 Boolean 플래그이며, 기본값은 `true`입니다. 크래시 보고서를 전송하지 않으려면 `false`로 설정하십시오.                                 |
| `endpoint`            | 크래시 보고서를 전송할 endpoint URL을 재정의할 수 있습니다.                                                                      |
| `send_logical_errors` | `LOGICAL_ERROR`는 `assert`와 비슷하며, ClickHouse의 버그를 의미합니다. 이 Boolean 플래그를 사용하면 이러한 예외를 전송할 수 있습니다(기본값: `true`). |

**권장 사용법**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

호스트 키의 공개 부분은
처음 연결할 때 SSH 클라이언트 측의 known&#95;hosts 파일에 기록됩니다.

호스트 키 구성은 기본적으로 비활성화되어 있습니다.
호스트 키 구성을 활성화하려면 주석 처리를 해제하고 각 SSH key의 경로를 지정하십시오:

예시:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

PTY를 통해 내장 클라이언트를 사용해 대화형으로 연결하고 쿼리를 실행할 수 있도록 하는 SSH server의 포트입니다.

예시:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

스토리지에 대해 여러 디스크를 구성할 수 있습니다.

스토리지 구성은 다음 구조를 따릅니다:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### `disks` 구성
</div>

`disks` 구성은 다음 구조를 따릅니다:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

위의 하위 태그는 `disks`에 대해 다음 설정을 정의합니다:

| SETTING                 | Description                                                    |
| ----------------------- | -------------------------------------------------------------- |
| `<disk_name_N>`         | 디스크 이름입니다. 고유해야 합니다.                                           |
| `path`                  | `server` 데이터(`data` 및 `shadow` 디렉터리)가 저장될 경로입니다. `/`로 끝나야 합니다. |
| `keep_free_space_bytes` | 디스크에 예약해 둘 여유 공간의 크기입니다.                                       |

:::note
디스크의 순서는 중요하지 않습니다.
:::

<div id="configuration-of-policies">
  ### 정책 구성
</div>

위의 하위 태그는 `policies`에 대해 다음 설정을 정의합니다.

| SETTING                      | Description                                                                                                                                                                                                                              |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | 정책 이름입니다. 정책 이름은 고유해야 합니다.                                                                                                                                                                                                               |
| `volume_name_N`              | 볼륨 이름입니다. 볼륨 이름은 고유해야 합니다.                                                                                                                                                                                                               |
| `disk`                       | 볼륨 내에 있는 디스크입니다.                                                                                                                                                                                                                         |
| `max_data_part_size_bytes`   | 이 볼륨의 디스크에 저장할 수 있는 데이터 청크의 최대 크기입니다. 머지 결과 생성되는 청크 크기가 `max_data_part_size_bytes`보다 클 것으로 예상되면, 해당 청크는 다음 볼륨에 기록됩니다. 이 기능을 사용하면 새롭거나 작은 청크는 고속(SSD) 볼륨에 저장하고, 크기가 커지면 저속(HDD) 볼륨으로 이동할 수 있습니다. 정책에 볼륨이 1개뿐이라면 이 옵션은 사용하지 마십시오.          |
| `move_factor`                | 볼륨에서 사용 가능한 여유 공간의 비율입니다. 여유 공간이 이 값보다 작아지면, 다음 볼륨이 있는 경우 데이터가 그쪽으로 이동하기 시작합니다. 이동할 때는 청크를 큰 것부터 작은 것 순서(내림차순)로 정렬한 뒤, `move_factor` 조건을 충족하기에 충분한 총 크기의 청크를 선택합니다. 모든 청크의 총 크기가 충분하지 않으면 모든 청크를 이동합니다.                                  |
| `perform_ttl_move_on_insert` | 삽입 시 TTL이 만료된 데이터의 이동을 비활성화합니다. 기본적으로(활성화된 경우) 수명 기반 이동 규칙에 따라 이미 만료된 데이터를 삽입하면, 해당 데이터는 즉시 이동 규칙에 지정된 볼륨/디스크로 이동됩니다. 대상 볼륨/디스크가 느린 경우(예: S3) 삽입 속도가 크게 저하될 수 있습니다. 비활성화하면 만료된 데이터 부분은 기본 볼륨에 기록된 후, 만료된 TTL에 대해 규칙에서 지정한 볼륨으로 즉시 이동됩니다. |
| `load_balancing`             | 디스크 밸런싱 정책입니다. `round_robin` 또는 `least_used`를 사용할 수 있습니다.                                                                                                                                                                                |
| `least_used_ttl_ms`          | 모든 디스크의 사용 가능한 공간을 갱신하는 timeout(밀리초 단위)을 설정합니다(`0` - 항상 갱신, `-1` - 갱신 안 함, 기본값은 `60000`). 참고로 디스크를 ClickHouse만 사용하고 파일 시스템 크기가 실행 중 동적으로 조정되지 않는다면 `-1` 값을 사용할 수 있습니다. 그 외의 경우에는 결국 잘못된 공간 할당으로 이어질 수 있으므로 권장하지 않습니다.                    |
| `prefer_not_to_merge`        | 이 볼륨에서 데이터 파트의 머지를 비활성화합니다. 참고: 이 설정은 잠재적으로 해로울 수 있으며 성능 저하를 유발할 수 있습니다. 이 설정이 활성화되면(이렇게 하지 마십시오) 이 볼륨에서는 데이터 머지가 금지됩니다(바람직하지 않음). 이를 통해 ClickHouse가 느린 디스크와 상호작용하는 방식을 제어할 수 있습니다. 이 설정은 사용하지 않을 것을 권장합니다.                              |
| `volume_priority`            | 볼륨이 채워지는 우선순위(순서)를 정의합니다. 값이 작을수록 우선순위가 높습니다. 매개변수 값은 자연수여야 하며, 1부터 N까지(N은 지정된 매개변수 값 중 가장 큰 값) 범위를 빠짐없이 모두 포함해야 합니다.                                                                                                                    |

`volume_priority`에 대해 설명하면 다음과 같습니다.

* 모든 볼륨에 이 매개변수가 있으면 지정된 순서대로 우선순위가 적용됩니다.
* *일부* 볼륨에만 이 매개변수가 있으면, 이 매개변수가 없는 볼륨이 가장 낮은 우선순위를 가집니다. 이 매개변수가 있는 볼륨은 태그 값에 따라 우선순위가 정해지며, 나머지 볼륨끼리의 우선순위는 설정 파일에 기술된 순서에 따라 결정됩니다.
* *어떤* 볼륨에도 이 매개변수가 없으면, 순서는 설정 파일에 기술된 순서에 따라 결정됩니다.
* 볼륨 우선순위는 동일할 수 없습니다.

<div id="macros">
  ## macros
</div>

복제된 테이블용 매개변수 치환입니다.

복제된 테이블을 사용하지 않는 경우 생략할 수 있습니다.

자세한 내용은 [복제된 테이블 생성](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables) 섹션을 참조하십시오.

**예시**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Replicated 데이터베이스의 레플리카 그룹 이름입니다.

복제된 데이터베이스에서 생성된 클러스터는 동일한 그룹에 속한 레플리카로 구성됩니다.
DDL 쿼리는 동일한 그룹에 속한 레플리카만 기다립니다.

기본적으로 비어 있습니다.

**예시**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

세션 timeout의 최댓값으로, 초 단위입니다.

예시:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 테이블용 세부 설정입니다.

자세한 내용은 MergeTreeSettings.h 헤더 파일을 참조하십시오.

**예시**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

기본적으로 비활성화되어 있습니다.

**활성화**

메트릭 이력 수집용 [`system.metric_log`](../../operations/system-tables/metric_log.md)을 수동으로 활성화하려면, 다음 내용을 사용해 `/etc/clickhouse-server/config.d/metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**비활성화**

`metric_log` 설정을 비활성화하려면 다음 내용을 포함한 `/etc/clickhouse-server/config.d/disable_metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

[ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 테이블용 세부 설정입니다. 이 설정이 더 높은 우선순위를 가집니다.

자세한 내용은 MergeTreeSettings.h 헤더 파일을 참조하십시오.

**예시**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

시스템 테이블(system table) [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md)에 대한 설정입니다.

<SystemLogParameters />

예시:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

SSL 클라이언트/서버 구성입니다.

SSL 지원은 `libpoco` 라이브러리에서 제공됩니다. 사용 가능한 구성 옵션은 [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)에 설명되어 있습니다. 기본값은 [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp)에서 확인할 수 있습니다.

서버/클라이언트 설정 키는 다음과 같습니다:

| 옵션                            | 설명                                                                                                                                                                                                                                                                                                                                                                       | 기본값                                                                                        |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | 세션 캐싱 사용 여부를 설정합니다. `sessionIdContext`와 함께 사용해야 합니다. 허용 값: `true`, `false`.                                                                                                                                                                                                                                                                                              | `false`                                                                                    |
| `caConfig`                    | 신뢰할 수 있는 CA certificate가 포함된 파일 또는 디렉터리의 경로입니다. 파일을 가리키는 경우 PEM 포맷이어야 하며, 여러 개의 CA certificate를 포함할 수 있습니다. 디렉터리를 가리키는 경우 각 CA certificate마다 .pem 파일이 하나씩 있어야 합니다. 파일 이름은 CA subject name hash value를 기준으로 조회됩니다. 자세한 내용은 [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html)의 man 페이지를 참조하십시오. |                                                                                            |
| `certificateFile`             | PEM 포맷의 클라이언트/서버 인증서 파일 경로입니다. `privateKeyFile`에 인증서가 포함되어 있으면 생략할 수 있습니다.                                                                                                                                                                                                                                                                                               |                                                                                            |
| `cipherList`                  | 지원되는 OpenSSL 암호화 방식입니다.                                                                                                                                                                                                                                                                                                                                                  | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | 사용할 수 없도록 설정된 protocol입니다.                                                                                                                                                                                                                                                                                                                                               |                                                                                            |
| `extendedVerification`        | 활성화되면 certificate의 CN 또는 SAN이 peer 호스트명과 일치하는지 확인합니다.                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `fips`                        | OpenSSL FIPS mode를 활성화합니다. 라이브러리의 OpenSSL version이 FIPS를 지원하는 경우에만 지원됩니다.                                                                                                                                                                                                                                                                                                | `false`                                                                                    |
| `invalidCertificateHandler`   | 유효하지 않은 인증서를 검증하는 클래스(CertificateHandler의 하위 클래스)입니다. 예시: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                             | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | OpenSSL의 내장 CA 인증서를 사용할지 여부입니다. ClickHouse는 내장 CA 인증서가 파일 `/etc/ssl/cert.pem`(또는 디렉터리 `/etc/ssl/certs`)에 있거나, 환경 변수 `SSL_CERT_FILE`(또는 `SSL_CERT_DIR`)로 지정된 파일(또는 디렉터리)에 있다고 간주합니다.                                                                                                                                                                                      | `true`                                                                                     |
| `preferServerCiphers`         | 클라이언트가 선호하는 서버 암호군.                                                                                                                                                                                                                                                                                                                                                      | `false`                                                                                    |
| `privateKeyFile`              | PEM 인증서의 비밀 키가 포함된 파일의 경로입니다. 이 파일에는 키와 인증서가 동시에 포함될 수 있습니다.                                                                                                                                                                                                                                                                                                             |                                                                                            |
| `privateKeyPassphraseHandler` | private key에 접근할 때 사용할 패스프레이스를 요청하는 클래스(PrivateKeyPassphraseHandler 하위 클래스)입니다. 예시: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                    | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | TLSv1 연결이 필요합니다. 허용 값: `true`, `false`.                                                                                                                                                                                                                                                                                                                                  | `false`                                                                                    |
| `requireTLSv1_1`              | TLSv1.1 연결이 필요합니다. 허용되는 값: `true`, `false`.                                                                                                                                                                                                                                                                                                                              | `false`                                                                                    |
| `requireTLSv1_2`              | TLSv1.2 연결이 필요합니다. 허용되는 값: `true`, `false`.                                                                                                                                                                                                                                                                                                                              | `false`                                                                                    |
| `sessionCacheSize`            | 서버가 캐시하는 세션의 최대 개수입니다. 값이 `0`이면 세션 수에 제한이 없음을 의미합니다.                                                                                                                                                                                                                                                                                                                     | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | 서버가 생성하는 각 식별자 끝에 추가하는 고유한 무작위 문자 집합입니다. 문자열 길이는 `SSL_MAX_SSL_SESSION_ID_LENGTH`를 초과해서는 안 됩니다. 이 매개변수는 서버가 세션을 캐시하는 경우와 클라이언트가 캐싱을 요청한 경우 모두에서 문제를 방지하는 데 도움이 되므로 항상 사용하는 것이 좋습니다.                                                                                                                                                                                       | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | 서버에서 세션을 캐싱하는 시간(시간 단위)입니다.                                                                                                                                                                                                                                                                                                                                              | `2`                                                                                        |
| `verificationDepth`           | 검증 체인의 최대 길이입니다. 인증서 체인 길이가 설정된 값을 초과하면 검증이 실패합니다.                                                                                                                                                                                                                                                                                                                       | `9`                                                                                        |
| `verificationMode`            | 노드의 인증서를 확인하는 방법입니다. 자세한 내용은 [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) 클래스 설명을 참조하십시오. 가능한 값: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                      | `relaxed`                                                                                  |

**설정 예시:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)와 관련된 이벤트를 로깅합니다. 예를 들어 데이터 추가나 머지 작업이 여기에 해당합니다. 이 로그를 사용하면 머지 알고리즘을 시뮬레이션하고 특성을 비교할 수 있습니다. 머지 프로세스를 시각화할 수도 있습니다.

쿼리는 별도의 파일이 아니라 [system.part&#95;log](/ko/operations/system-tables/part_log) 테이블에 기록됩니다. 이 테이블의 이름은 `table` 매개변수에서 설정할 수 있습니다(아래 참조).

<SystemLogParameters />

**예시**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

[`processors_profile_log`](../system-tables/processors_profile_log.md) 시스템 테이블(system table)의 설정입니다.

<SystemLogParameters />

기본 설정은 다음과 같습니다:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

[Prometheus](https://prometheus.io)에서 스크레이핑할 수 있도록 메트릭 데이터를 노출합니다.

설정:

* `endpoint` – prometheus server가 메트릭을 스크레이핑할 HTTP endpoint입니다. &#39;/&#39;로 시작해야 합니다.
* `port` – `endpoint`에 사용할 포트입니다.
* `metrics` – [system.metrics](/ko/operations/system-tables/metrics) 테이블의 메트릭을 노출합니다.
* `events` – [system.events](/ko/operations/system-tables/events) 테이블의 메트릭을 노출합니다.
* `asynchronous_metrics` – [system.asynchronous&#95;metrics](/ko/operations/system-tables/asynchronous_metrics) 테이블의 현재 메트릭 값을 노출합니다.
* `errors` - 마지막 server 재시작 이후 발생한 오류 수를 오류 코드별로 노출합니다. 이 정보는 [system.errors](/ko/operations/system-tables/errors)에서도 확인할 수 있습니다.

**예시**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

확인하세요(`127.0.0.1` 대신 ClickHouse 서버의 IP 주소 또는 호스트명을 사용):

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

[log&#95;queries=1](../../operations/settings/settings.md) 설정으로 수신된 쿼리를 로깅하기 위한 설정입니다.

쿼리는 별도 파일이 아니라 [system.query&#95;log](/ko/operations/system-tables/query_log) 테이블에 기록됩니다. 테이블 이름은 `table` 매개변수에서 변경할 수 있습니다(아래 참조).

<SystemLogParameters />

테이블이 없으면 ClickHouse가 생성합니다. ClickHouse 서버 업데이트로 query log의 구조가 변경되면, 이전 구조의 테이블 이름이 변경되고 새 테이블이 자동으로 생성됩니다.

**예시**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

기본적으로 비활성화되어 있습니다.

**활성화**

메트릭 이력 수집 [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md)을 수동으로 사용 설정하려면, 다음 내용으로 `/etc/clickhouse-server/config.d/query_metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**비활성화**

`query_metric_log` 설정을 비활성화하려면 다음 내용으로 `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` 파일을 생성하십시오:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

[쿼리 캐시](../query-cache.md) 구성입니다.

다음 설정을 사용할 수 있습니다.

| Setting                   | Description                                  | Default Value |
| ------------------------- | -------------------------------------------- | ------------- |
| `max_entries`             | 캐시에 저장되는 `SELECT` 쿼리 결과의 최대 개수입니다.           | `1024`        |
| `max_entry_size_in_bytes` | 캐시에 저장할 수 있는 `SELECT` 쿼리 결과의 최대 크기(바이트)입니다.  | `1048576`     |
| `max_entry_size_in_rows`  | 캐시에 저장할 수 있는 `SELECT` 쿼리 결과의 최대 행 수입니다.      | `30000000`    |
| `max_size_in_bytes`       | 캐시의 최대 크기(바이트)입니다. `0`은 쿼리 캐시가 비활성화됨을 의미합니다. | `1073741824`  |

:::note

* 변경된 설정은 즉시 적용됩니다.
* 쿼리 캐시 데이터는 DRAM에 할당됩니다. 메모리가 부족한 경우 `max_size_in_bytes`를 작은 값으로 설정하거나 쿼리 캐시를 완전히 비활성화하십시오.
  :::

**예시**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

[log&#95;query&#95;threads=1](/ko/operations/settings/settings#log_query_threads) 설정으로 수신된 쿼리의 스레드를 로깅하는 설정입니다.

쿼리는 별도의 파일이 아니라 [system.query&#95;thread&#95;log](/ko/operations/system-tables/query_thread_log) 테이블에 기록됩니다. `table` 매개변수에서 테이블 이름을 변경할 수 있습니다(아래 참조).

<SystemLogParameters />

테이블이 없으면 ClickHouse가 생성합니다. ClickHouse 서버 업데이트로 쿼리 스레드 로그의 구조가 변경된 경우, 이전 구조의 테이블은 이름이 변경되고 새 테이블이 자동으로 생성됩니다.

**예시**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

[log&#95;query&#95;views=1](/ko/operations/settings/settings#log_query_views) 설정으로 수신된 쿼리와 관련된 뷰(라이브 뷰(Live View), 구체화된 뷰(Materialized View) 등)를 로깅하기 위한 설정입니다.

쿼리는 별도 파일이 아니라 [system.query&#95;views&#95;log](/ko/operations/system-tables/query_views_log) 테이블에 기록됩니다. `table` 매개변수에서 테이블 이름을 변경할 수 있습니다(아래 참조).

<SystemLogParameters />

테이블이 없으면 ClickHouse가 생성합니다. ClickHouse 서버 업데이트 시 쿼리 뷰 로그의 구조가 변경된 경우, 이전 구조의 테이블은 이름이 변경되고 새 테이블이 자동으로 생성됩니다.

**예시**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

텍스트 메시지 로깅을 위한 [text&#95;log](/ko/operations/system-tables/text_log) 시스템 테이블의 설정입니다.

<SystemLogParameters />

추가로:

| 설정      | 설명                                   | 기본값     |
| ------- | ------------------------------------ | ------- |
| `level` | 테이블에 저장할 최대 메시지 수준(기본값은 `Trace`)입니다. | `Trace` |

**예시**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

[trace&#95;log](/ko/operations/system-tables/trace_log) 시스템 테이블에 대한 설정입니다.

<SystemLogParameters />

기본 서버 설정 파일 `config.xml`에는 다음과 같은 설정 섹션이 있습니다:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

비동기 삽입을 기록하는 [asynchronous&#95;insert&#95;log](/ko/operations/system-tables/asynchronous_insert_log) 시스템 테이블에 대한 설정입니다.

<SystemLogParameters />

**예시**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

[crash&#95;log](../../operations/system-tables/crash_log.md) 시스템 테이블 작업용 설정입니다.

다음 설정은 하위 태그로 구성할 수 있습니다:

| Setting                            | Description                                                                                                               | Default             | Note                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | 줄 수에 대한 임계값입니다. 이 임계값에 도달하면 로그를 디스크로 플러시하는 작업이 백그라운드에서 시작됩니다.                                                             | `max_size_rows / 2` |                                                                                       |
| `database`                         | 데이터베이스 이름입니다.                                                                                                             |                     |                                                                                       |
| `engine`                           | 시스템 테이블용 [MergeTree 엔진 정의](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table)입니다. |                     | `partition_by` 또는 `order_by`가 정의되어 있으면 사용할 수 없습니다. 지정하지 않으면 기본적으로 `MergeTree`가 선택됩니다. |
| `flush_interval_milliseconds`      | 메모리 버퍼에서 테이블로 데이터를 플러시하는 인터벌입니다.                                                                                          | `7500`              |                                                                                       |
| `flush_on_crash`                   | 크래시가 발생했을 때 로그를 디스크에 덤프할지 여부를 설정합니다.                                                                                      | `false`             |                                                                                       |
| `max_size_rows`                    | 로그의 최대 줄 수입니다. 플러시되지 않은 로그 수가 max&#95;size에 도달하면 로그가 디스크에 덤프됩니다.                                                          | `1024`              |                                                                                       |
| `order_by`                         | 시스템 테이블용 [사용자 지정 정렬 키](/ko/engines/table-engines/mergetree-family/mergetree#order_by)입니다. `engine`이 정의되어 있으면 사용할 수 없습니다.     |                     | 시스템 테이블에 `engine`이 지정된 경우 `order_by` 매개변수는 &#39;engine&#39; 내부에 직접 지정해야 합니다.          |
| `partition_by`                     | 시스템 테이블용 [사용자 지정 파티셔닝 키](/ko/engines/table-engines/mergetree-family/custom-partitioning-key.md)입니다.                          |                     | 시스템 테이블에 `engine`이 지정된 경우 `partition_by` 매개변수는 &#39;engine&#39; 내부에 직접 지정해야 합니다.      |
| `reserved_size_rows`               | 로그용으로 미리 할당하는 메모리 크기(줄 수)입니다.                                                                                             | `1024`              |                                                                                       |
| `settings`                         | MergeTree의 동작을 제어하는 [추가 매개변수](/ko/engines/table-engines/mergetree-family/mergetree/#settings)입니다(선택 사항).                     |                     | 시스템 테이블에 `engine`이 지정된 경우 `settings` 매개변수는 &#39;engine&#39; 내부에 직접 지정해야 합니다.          |
| `storage_policy`                   | 테이블에 사용할 스토리지 정책 이름입니다(선택 사항).                                                                                            |                     | 시스템 테이블에 `engine`이 지정된 경우 `storage_policy` 매개변수는 &#39;engine&#39; 내부에 직접 지정해야 합니다.    |
| `table`                            | 시스템 테이블 이름입니다.                                                                                                            |                     |                                                                                       |
| `ttl`                              | 테이블 [TTL](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)을 지정합니다.                           |                     | 시스템 테이블에 `engine`이 지정된 경우 `ttl` 매개변수는 &#39;engine&#39; 내부에 직접 지정해야 합니다.               |

기본 서버 설정 파일 `config.xml`에는 다음 설정 섹션이 포함되어 있습니다:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

이 설정은 사용자 지정(SQL로 생성된) cached 디스크의 캐시 경로를 지정합니다.
사용자 지정 디스크의 경우 `custom_cached_disks_base_directory`가 `filesystem_caches_path`(`filesystem_caches_path.xml`에 있음)보다 우선 적용됩니다.
`custom_cached_disks_base_directory`가 없으면 `filesystem_caches_path`가 사용됩니다.
파일 시스템 캐시 설정 경로는 해당 디렉터리 내부에 있어야 하며,
그렇지 않으면 디스크가 생성되지 않도록 예외가 발생합니다.

:::note
이 설정은 서버 업그레이드 전에 이전 버전에서 생성된 디스크에는 영향을 주지 않습니다.
이 경우 서버가 정상적으로 시작될 수 있도록 예외가 발생하지 않습니다.
:::

예시:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

`BACKUP` 및 `RESTORE` 작업을 로깅하는 [backup&#95;log](../../operations/system-tables/backup_log.md) 시스템 테이블용 설정입니다.

<SystemLogParameters />

**예시**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

[`blob_storage_log`](../system-tables/blob_storage_log.md) 시스템 테이블에 대한 설정입니다.

<SystemLogParameters />

예시:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

쿼리와 모든 로그 메시지에 적용되는 정규식 기반 규칙이며, 서버 로그, [`system.query_log`](/ko/operations/system-tables/query_log), [`system.text_log`](/ko/operations/system-tables/text_log), [`system.processes`](/ko/operations/system-tables/processes) 테이블, 그리고 클라이언트로 전송되는 로그에 저장되기 전에 적용됩니다. 이를 통해 이름, 이메일, 개인 식별자 또는 신용카드 번호와 같은 민감한 데이터가 SQL 쿼리에서 로그로 유출되는 것을 방지할 수 있습니다.

**예시**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**구성 필드**:

| Setting   | Description                                 |
| --------- | ------------------------------------------- |
| `name`    | 규칙 이름(선택 사항)                                |
| `regexp`  | RE2와 호환되는 정규식(필수)                           |
| `replace` | 민감한 데이터에 사용할 치환 문자열(선택 사항, 기본값 - 애스터리스크 6개) |

마스킹 규칙은 쿼리 전체에 적용됩니다(형식이 잘못되었거나 parse할 수 없는 쿼리에서 민감한 데이터가 유출되는 것을 방지하기 위함입니다).

[`system.events`](/ko/operations/system-tables/events) 테이블에는 `QueryMaskingRulesMatch` 카운터가 있으며, 이 값은 쿼리 마스킹 규칙과 일치한 총 횟수를 나타냅니다.

분산 쿼리의 경우 각 서버를 개별적으로 구성해야 합니다. 그렇지 않으면 다른
노드로 전달되는 서브쿼리가 마스킹되지 않은 상태로 저장됩니다.

<div id="remote_servers">
  ## remote_servers
</div>

[분산](../../engines/table-engines/special/distributed.md) 테이블 엔진과 `cluster` 테이블 함수에서 사용하는 클러스터 구성입니다.

**예시**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

`incl` 속성 값은 &quot;[설정 파일](/ko/operations/configuration-files)&quot; 섹션을 참조하십시오.

**관련 항목**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [클러스터 디스커버리](../../operations/cluster-discovery.md)
* [복제된 데이터베이스 엔진](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

URL 관련 스토리지 엔진과 테이블 함수에서 사용할 수 있도록 허용된 호스트 목록입니다.

`\<<host>\>` xml tag로 호스트를 추가할 때는 다음 사항을 따르십시오.

* 이름은 DNS 해석 전에 확인되므로 URL에 있는 그대로 정확히 지정해야 합니다. 예시: `<host>clickhouse.com</host>`
* URL에 포트가 명시적으로 지정된 경우에는 `host:port` 전체를 확인합니다. 예시: `<host>clickhouse.com:80</host>`
* 호스트를 포트 없이 지정한 경우에는 해당 호스트의 모든 포트가 허용됩니다. 예시: `<host>clickhouse.com</host>`를 지정하면 `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS) 등이 허용됩니다.
* 호스트를 IP 주소로 지정한 경우에는 URL에 지정된 그대로 확인합니다. 예시: `[2a02:6b8:a::a]`.
* 리디렉션이 발생하고 리디렉션 지원이 활성화된 경우에는 각 리디렉션(`location` field)을 모두 확인합니다.

예시:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

서버의 시간대입니다.

UTC 시간대 또는 지리적 위치(예: Africa/Abidjan)를 나타내는 IANA 식별자로 지정합니다.

시간대는 DateTime 필드를 텍스트 포맷으로 출력할 때(화면이나 파일에 표시할 때) String과 DateTime 포맷 간 변환에 필요하며, 문자열에서 DateTime 값을 가져올 때도 사용됩니다. 또한 시간 및 날짜를 처리하는 함수에 입력 매개변수로 시간대가 전달되지 않은 경우에도 사용됩니다.

**예시**

```xml
<timezone>Asia/Istanbul</timezone>
```

**관련 항목**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

TCP 프로토콜을 통해 클라이언트와 통신할 때 사용하는 포트입니다.

**예시**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

클라이언트와 보안 통신할 때 사용하는 TCP 포트입니다. [OpenSSL](#openssl) 설정과 함께 사용하십시오.

**기본값**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

MySQL 프로토콜을 통해 클라이언트와 통신하는 데 사용하는 포트입니다.

:::note

* 양의 정수는 수신 대기할 포트 번호를 지정합니다.
* 빈 값은 MySQL 프로토콜을 통한 클라이언트와의 통신을 비활성화하는 데 사용됩니다.
  :::

**예시**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

PostgreSQL 프로토콜을 통해 클라이언트와 통신하는 데 사용할 포트입니다.

:::note

* 양의 정수는 수신 대기할 포트 번호를 지정합니다.
* 빈 값은 PostgreSQL 프로토콜을 통한 클라이언트 통신을 비활성화할 때 사용됩니다.
  :::

**예시**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

축약형 또는 기호형 URL 접두사를 전체 URL로 변환하는 구성입니다.

예시:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

사용자 정의 파일이 저장된 디렉터리입니다. SQL 사용자 정의 함수 [SQL 사용자 정의 함수](/ko/sql-reference/functions/udf)에 사용됩니다.

**예시**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

다음을 포함하는 파일의 경로입니다.

* 사용자 설정
* 접근 권한
* 설정 프로필
* 할당량 설정

**예시**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

access control 시스템의 선택적 개선 사항을 위한 설정입니다.

| 설정                                              | 설명                                                                                                                                                                                                                                                                                                                                                                  | 기본값     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | `ON CLUSTER` 쿼리에 `CLUSTER` 권한이 필요한지 설정합니다.                                                                                                                                                                                                                                                                                                                          | `true`  |
| `role_cache_expiration_time_seconds`            | 마지막 접근 후 역할이 Role Cache에 저장되는 시간을 초 단위로 설정합니다.                                                                                                                                                                                                                                                                                                                      | `600`   |
| `select_from_information_schema_requires_grant` | `SELECT * FROM information_schema.<table>``를` 실행하는 데 권한이 필요한지, 또는 모든 사용자가 실행할 수 있는지 설정합니다. true로 설정하면 이 쿼리는 일반 테이블과 마찬가지로 `GRANT SELECT ON information_schema.<table>` 권한이 필요합니다.                                                                                                                                                                                   | `true`  |
| `select_from_system_db_requires_grant`          | `SELECT * FROM system.<table>`를 실행하는 데 권한이 필요한지, 또는 모든 사용자가 실행할 수 있는지 설정합니다. true로 설정하면 이 쿼리는 일반 테이블과 마찬가지로 `GRANT SELECT ON system.<table>` 권한이 필요합니다. 예외: 일부 system table(`tables`, `columns`, `databases`, 그리고 `one`, `contributors` 같은 일부 상수 테이블)은 계속 모든 사용자가 접근할 수 있습니다. 또한 `SHOW` 권한(예: `SHOW USERS`)이 부여된 경우 해당 system table(즉, `system.users`)에 접근할 수 있습니다. | `true`  |
| `settings_constraints_replace_previous`         | 특정 설정에 대해 설정 프로필의 제약 조건이 이전 제약 조건(다른 프로필에 정의됨)의 동작을 대체할지 설정합니다. 여기에는 새 제약 조건에서 설정되지 않은 필드도 포함됩니다. 또한 `changeable_in_readonly` 제약 조건 유형을 활성화합니다.                                                                                                                                                                                                                     | `true`  |
| `table_engines_require_grant`                   | 특정 테이블 엔진으로 테이블을 생성할 때 권한이 필요한지 설정합니다.                                                                                                                                                                                                                                                                                                                              | `false` |
| `throw_on_unmatched_row_policies`               | 테이블에 row policy가 있지만 현재 사용자에 해당하는 policy가 하나도 없을 때, 테이블을 읽으면 예외를 발생시킬지 설정합니다.                                                                                                                                                                                                                                                                                       | `false` |
| `users_without_row_policies_can_read_rows`      | 허용형 row policy가 없는 사용자도 `SELECT` 쿼리로 행을 읽을 수 있는지 설정합니다. 예를 들어 사용자 A와 B가 있고 row policy가 A에 대해서만 정의되어 있다면, 이 설정이 true이면 사용자 B는 모든 행을 보게 됩니다. 이 설정이 false이면 사용자 B는 어떤 행도 볼 수 없습니다.                                                                                                                                                                                     | `true`  |

예시:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

`s3queue_log` 시스템 테이블의 설정입니다.

<SystemLogParameters />

기본 설정은 다음과 같습니다.

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

&#39;dead&#95;letter&#95;queue&#39; 시스템 테이블에 대한 설정입니다.

<SystemLogParameters />

기본 설정은 다음과 같습니다.

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

[ZooKeeper](http://zookeeper.apache.org/) 클러스터와 ClickHouse가 상호작용할 수 있도록 하는 설정을 포함합니다. ClickHouse는 복제된 테이블(Replicated Table)을 사용할 때 레플리카의 메타데이터를 저장하기 위해 ZooKeeper를 사용합니다. 복제된 테이블을 사용하지 않는 경우 이 매개변수 섹션은 생략할 수 있습니다.

다음 설정은 하위 태그로 구성할 수 있습니다:

| Setting                                         | Description                                                                                                                                                                                                                                                                              |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | ZooKeeper 엔드포인트입니다. 여러 엔드포인트를 설정할 수 있습니다. 예: `<node index="1"><host>example_host</host><port>2181</port></node>`. `index` 속성은 ZooKeeper 클러스터에 연결을 시도할 때 노드 순서를 지정합니다.                                                                                                                    |
| `operation_timeout_ms`                          | 단일 작업의 최대 제한 시간이며, 밀리초 단위로 설정합니다.                                                                                                                                                                                                                                                        |
| `session_timeout_ms`                            | 클라이언트 세션의 최대 제한 시간이며, 밀리초 단위로 설정합니다.                                                                                                                                                                                                                                                     |
| `root` (optional)                               | ClickHouse 서버에서 사용하는 znode의 루트로 사용되는 znode입니다.                                                                                                                                                                                                                                           |
| `fallback_session_lifetime.min` (optional)      | 프라이머리를 사용할 수 없을 때 폴백 노드에 연결되는 ZooKeeper 세션 수명의 최소 한도입니다(로드 밸런싱). 초 단위로 설정합니다. 기본값: 3시간.                                                                                                                                                                                                  |
| `fallback_session_lifetime.max` (optional)      | 프라이머리를 사용할 수 없을 때 폴백 노드에 연결되는 ZooKeeper 세션 수명의 최대 한도입니다(로드 밸런싱). 초 단위로 설정합니다. 기본값: 6시간.                                                                                                                                                                                                  |
| `identity` (optional)                           | 요청한 znode에 접근하기 위해 ZooKeeper가 요구하는 사용자 및 비밀번호입니다.                                                                                                                                                                                                                                        |
| `use_compression` (optional)                    | `true`로 설정하면 Keeper 프로토콜에서 압축을 활성화합니다.                                                                                                                                                                                                                                                   |
| `use_xid_64` (optional)                         | 64비트 transaction ID를 활성화합니다. 확장 transaction ID 포맷을 사용하려면 `true`로 설정합니다. 기본값: `false`.                                                                                                                                                                                                    |
| `pass_opentelemetry_tracing_context` (optional) | Keeper 요청으로 OpenTelemetry tracing Context를 전파하도록 활성화합니다. 활성화하면 Keeper 작업에 대한 tracing 스팬이 생성되어 ClickHouse와 Keeper 전반에서 distributed tracing을 사용할 수 있습니다. 자세한 내용은 [Tracing ClickHouse Keeper Requests](/ko/operations/opentelemetry#tracing-clickhouse-keeper-requests)를 참조하십시오. 기본값: `false`. |

또한 `zookeeper_load_balancing` 설정(선택 사항)을 사용해 ZooKeeper 노드 선택 알고리즘을 지정할 수 있습니다:

| Algorithm Name                   | Description                                                           |
| -------------------------------- | --------------------------------------------------------------------- |
| `random`                         | ZooKeeper 노드 중 하나를 무작위로 선택합니다.                                        |
| `in_order`                       | 첫 번째 ZooKeeper 노드를 선택하고, 사용할 수 없으면 두 번째 노드를 선택하는 방식으로 진행합니다.          |
| `nearest_hostname`               | 서버의 호스트명과 가장 유사한 호스트명을 가진 ZooKeeper 노드를 선택하며, 호스트명은 이름 접두사로 비교합니다.    |
| `hostname_levenshtein_distance`  | `nearest_hostname`와 유사하지만, 호스트명을 Levenshtein distance 방식으로 비교합니다.     |
| `hostname_longest_common_prefix` | `nearest_hostname`와 유사하지만, 서버의 호스트명과 가장 긴 공통 접두사를 공유하는 노드를 우선 선택합니다.  |
| `hostname_longest_common_suffix` | `nearest_hostname`와 유사하지만, 서버의 호스트명과 가장 긴 공통 접미사를 공유하는 노드를 우선 선택합니다.  |
| `first_or_random`                | 첫 번째 ZooKeeper 노드를 선택하고, 사용할 수 없으면 나머지 ZooKeeper 노드 중 하나를 무작위로 선택합니다. |
| `round_robin`                    | 첫 번째 ZooKeeper 노드를 선택하며, 재연결이 발생하면 다음 노드를 선택합니다.                      |

**예시 구성**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**관련 항목**

* [복제](../../engines/table-engines/mergetree-family/replication.md)
* [ZooKeeper 프로그래머 가이드](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [ClickHouse와 ZooKeeper 간 보안 통신(선택 사항)](/ko/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

ZooKeeper에서 데이터 파트 헤더를 저장하는 방식입니다. 이 설정은 [`MergeTree`](/ko/engines/table-engines/mergetree-family) 계열에만 적용됩니다. 다음과 같이 지정할 수 있습니다.

**`config.xml` 파일의 [merge&#95;tree](#merge_tree) 섹션에서 전역으로**

ClickHouse는 서버의 모든 테이블에 이 설정을 적용합니다. 이 설정은 언제든지 변경할 수 있습니다. 기존 테이블은 설정이 변경되면 동작 방식도 함께 변경됩니다.

**각 테이블별로**

테이블을 생성할 때 해당 [엔진 설정](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)을 지정하십시오. 이 설정이 지정된 기존 테이블의 동작은 전역 설정이 변경되더라도 바뀌지 않습니다.

**가능한 값**

* `0` — 기능이 비활성화됩니다.
* `1` — 기능이 활성화됩니다.

[`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper)인 경우, [복제된](../../engines/table-engines/mergetree-family/replication.md) 테이블은 데이터 파트 헤더를 단일 `znode`를 사용해 간결한 형태로 저장합니다. 테이블에 컬럼이 많을수록 이 저장 방식은 ZooKeeper에 저장되는 데이터의 양을 크게 줄여 줍니다.

:::note
`use_minimalistic_part_header_in_zookeeper = 1`을 적용한 후에는 이 설정을 지원하지 않는 버전으로 ClickHouse 서버를 다운그레이드할 수 없습니다. 클러스터의 서버에서 ClickHouse를 업그레이드할 때는 주의하십시오. 모든 서버를 한 번에 업그레이드하지 마십시오. 테스트 환경이나 클러스터의 일부 서버에서만 새 ClickHouse 버전을 먼저 시험하는 편이 더 안전합니다.

이 설정으로 이미 저장된 데이터 파트 헤더는 이전의 비압축 형식으로 복원할 수 없습니다.
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

클러스터에서 [분산 DDL 쿼리](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`)를 실행하는 작업을 관리합니다.
[ZooKeeper](/ko/operations/server-configuration-parameters/settings#zookeeper)가 활성화된 경우에만 동작합니다.

`<distributed_ddl>` 내에서 구성 가능한 설정은 다음과 같습니다.

| Setting                | Description                                                                  | Default Value                  |
| ---------------------- | ---------------------------------------------------------------------------- | ------------------------------ |
| `cleanup_delay_period` | 마지막 정리가 `cleanup_delay_period`초보다 최근에 수행되지 않은 경우, 새 노드 이벤트를 수신한 뒤 정리를 시작합니다. | `60`초                          |
| `max_tasks_in_queue`   | 큐에 포함될 수 있는 작업의 최대 개수입니다.                                                    | `1,000`                        |
| `path`                 | DDL 쿼리용 `task_queue`에 대한 Keeper 경로입니다.                                       |                                |
| `pool_size`            | 동시에 실행할 수 있는 `ON CLUSTER` 쿼리 수입니다.                                           |                                |
| `profile`              | DDL 쿼리를 실행할 때 사용하는 프로필입니다.                                                   |                                |
| `task_max_lifetime`    | 생성 후 경과 시간이 이 값보다 크면 해당 노드를 삭제합니다.                                           | `7 * 24 * 60 * 60` (1주를 초로 환산) |

**예시**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

ClickHouse 서버가 SQL 명령으로 생성된 사용자 및 역할 구성을 저장하는 폴더 경로입니다.

**관련 항목**

* [액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

평문 비밀번호 타입(안전하지 않음)의 허용 여부를 설정합니다.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

보안상 안전하지 않은 no&#95;password 비밀번호 유형의 허용 여부를 설정합니다.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

&#39;IDENTIFIED WITH no&#95;password&#39;를 명시적으로 지정하지 않는 한, 비밀번호 없이 사용자를 생성할 수 없도록 합니다.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

기본 세션 타임아웃입니다(초 단위).

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

`CREATE USER u IDENTIFIED BY 'p'`와 같은 쿼리에서 자동으로 설정할 비밀번호 유형을 지정합니다.

허용되는 값은 다음과 같습니다.

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

다음 설정을 포함하는 설정 파일의 섹션입니다.

* 미리 정의된 사용자가 있는 설정 파일의 경로
* SQL 명령으로 생성된 사용자가 저장되는 폴더의 경로
* SQL 명령으로 생성된 사용자가 저장되고 복제되는 ZooKeeper 노드 경로

이 섹션이 지정되면 [users&#95;config](/ko/operations/server-configuration-parameters/settings#users_config) 및 [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path)의 경로는 사용되지 않습니다.

`user_directories` 섹션에는 항목을 원하는 만큼 포함할 수 있으며, 항목의 순서는 precedence를 의미합니다(위에 있는 항목일수록 precedence가 높습니다).

**예시**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

사용자, 역할, 행 정책(row policies), 쿼터, 프로필은 ZooKeeper에도 저장할 수 있습니다:

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

또한 `memory` 섹션과 `ldap` 섹션을 정의할 수 있습니다. `memory`는 정보를 디스크에 기록하지 않고 메모리에만 저장함을 의미하며, `ldap`는 정보를 LDAP 서버에 저장함을 의미합니다.

로컬에 정의되지 않은 사용자를 위한 원격 사용자 디렉터리로 LDAP 서버를 추가하려면, 다음 설정으로 단일 `ldap` 섹션을 정의하십시오.

| Setting  | Description                                                                                                                                                                  |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`  | LDAP 서버에서 가져온 각 사용자에게 할당할 로컬에 정의된 역할 목록이 포함된 섹션입니다. 역할이 지정되지 않으면 사용자는 인증 후 어떤 작업도 수행할 수 없습니다. 나열된 역할 중 하나라도 인증 시점에 로컬에 정의되어 있지 않으면, 제공된 비밀번호가 올바르지 않은 경우와 동일하게 인증 시도가 실패합니다. |
| `server` | `ldap_servers` 구성 섹션에 정의된 LDAP 서버 이름 중 하나입니다. 이 매개변수는 필수이며 비워 둘 수 없습니다.                                                                                                      |

**예시**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

추가할 사용자 지정 최상위 도메인 목록을 정의합니다. 각 항목은 `<name>/path/to/file</name>` 형식입니다.

예시:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

관련 항목:

* 사용자 지정 TLD 목록 이름을 받아 최상위 하위 도메인부터 유의미한 하위 도메인까지 포함된 도메인 부분을 반환하는 함수 [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) 및 그 변형

<div id="proxy">
  ## 프록시
</div>

현재 S3 storage, S3 테이블 함수, URL 함수에서 지원하는 HTTP 및 HTTPS 요청용 프록시 서버를 정의합니다.

프록시 서버를 정의하는 방법은 세 가지입니다:

* 환경 변수
* 프록시 목록
* 원격 프록시 리졸버

특정 호스트에 대해 프록시 서버를 우회하는 기능도 `no_proxy`를 사용해 지원됩니다.

**환경 변수**

`http_proxy` 및 `https_proxy` 환경 변수로 지정한 프로토콜에 대한
프록시 서버를 설정할 수 있습니다. 시스템에 설정되어 있으면
별도 작업 없이 정상적으로 작동합니다.

이 방법은 특정 프로토콜에
프록시 서버가 하나만 있고, 해당 프록시 서버가 변경되지 않는 경우
가장 간단합니다.

**프록시 목록**

이 방법을 사용하면 특정 프로토콜에 대해 하나 이상의
프록시 서버를 지정할 수 있습니다. 둘 이상의 프록시 서버가 정의되어 있으면
ClickHouse는 서로 다른 프록시를 라운드 로빈 방식으로 사용하여
서버 간에 부하를 분산합니다. 이 방법은 특정 프로토콜에 프록시 서버가 둘 이상 있고
프록시 서버 목록이 변경되지 않는 경우 가장 간단합니다.

**구성 템플릿**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

하위 항목을 보려면 아래 탭에서 상위 field를 선택하십시오:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | 필드        | 설명                  |
    | --------- | ------------------- |
    | `<http>`  | 하나 이상의 HTTP 프록시 목록  |
    | `<https>` | 하나 이상의 HTTPS 프록시 목록 |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | 필드      | 설명       |
    | ------- | -------- |
    | `<uri>` | 프록시의 URI |
  </TabItem>
</Tabs>

**원격 프록시 리졸버**

프록시 서버가 동적으로 변경될 수도 있습니다. 이 경우
리졸버 엔드포인트를 정의할 수 있습니다. ClickHouse는
해당 엔드포인트로 빈 GET 요청을 보내고, 원격 리졸버는 프록시 host를 반환해야 합니다.
ClickHouse는 이를 사용해 다음 템플릿으로 프록시 URI를 구성합니다: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**구성 템플릿**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

아래 탭에서 상위 필드를 선택하면 해당 하위 필드를 볼 수 있습니다:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | 필드        | 설명                 |
    | --------- | ------------------ |
    | `<http>`  | 하나 이상의 리졸버* 목록 |
    | `<https>` | 하나 이상의 리졸버* 목록 |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | 필드           | 설명                       |
    | ------------ | ------------------------ |
    | `<resolver>` | 리졸버의 엔드포인트 및 기타 세부 정보 |

    :::note
    `<resolver>` 요소는 여러 개 둘 수 있지만, 지정된 프로토콜마다 첫 번째
    `<resolver>`만 사용됩니다. 해당 프로토콜의 다른 `<resolver>`
    요소는 모두 무시됩니다. 즉, 로드 밸런싱이 필요하다면
    원격 리졸버에서 구현해야 합니다.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | 필드                   | 설명                                                                                               |
    | -------------------- | ------------------------------------------------------------------------------------------------ |
    | `<endpoint>`         | 프록시 리졸버의 URI                                                                                     |
    | `<proxy_scheme>`     | 최종 프록시 URI의 프로토콜입니다. `http` 또는 `https` 중 하나일 수 있습니다.                                         |
    | `<proxy_port>`       | 프록시 리졸버의 port 번호                                                                                 |
    | `<proxy_cache_time>` | 리졸버의 값을 ClickHouse가 캐시하는 시간(초)입니다. 이 값을 `0`으로 설정하면 ClickHouse는 모든 HTTP 또는 HTTPS 요청마다 리졸버에 연결합니다. |
  </TabItem>
</Tabs>

**precedence**

프록시 설정은 다음 순서로 결정됩니다:

| 순서 | 설정         |
| -- | ---------- |
| 1. | 원격 프록시 리졸버 |
| 2. | 프록시 목록     |
| 3. | 환경 변수      |

ClickHouse는 요청 프로토콜에 대해 가장 우선순위가 높은 리졸버 유형을 확인합니다. 해당 항목이 정의되어 있지 않으면,
환경 리졸버에 도달할 때까지 그다음으로 우선순위가 높은 리졸버 유형을 차례로 확인합니다.
즉, 여러 리졸버 유형을 혼합하여 사용할 수도 있습니다.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

기본적으로 `HTTP` 프록시를 통해 `HTTPS` 요청을 보낼 때는 터널링(즉, `HTTP CONNECT`)이 사용됩니다. 이 설정을 사용하면 이를 비활성화할 수 있습니다.

**no&#95;proxy**

기본적으로 모든 요청은 프록시를 거칩니다. 특정 호스트에 대해서는 이를 비활성화하려면 `no_proxy` 변수를 설정해야 합니다.
이 변수는 목록 리졸버와 원격 리졸버의 `<proxy>` 절 안에서 설정할 수 있으며, 환경 리졸버에서는 환경 변수로 설정할 수 있습니다.
IP 주소, 도메인, 하위 도메인, 그리고 전체 우회를 위한 `'*'` 와일드카드를 지원합니다. 앞의 점은 curl과 동일하게 제거됩니다.

**예시**

아래 구성은 `clickhouse.cloud` 및 그 모든 하위 도메인(예: `auth.clickhouse.cloud`)으로의 요청이 프록시를 우회하도록 합니다.
앞에 점이 있는 GitLab에도 동일하게 적용됩니다. `gitlab.com`과 `about.gitlab.com` 모두 프록시를 우회합니다.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

모든 `CREATE WORKLOAD` 및 `CREATE RESOURCE` 쿼리를 저장하는 디렉터리입니다. 기본적으로 server 작업 디렉터리 아래의 `/workload/` 폴더를 사용합니다.

**예시**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**관련 항목**

* [워크로드 계층 구조](/ko/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

모든 `CREATE WORKLOAD` 및 `CREATE RESOURCE` 쿼리의 저장소로 사용되는 ZooKeeper 노드의 경로입니다. 일관성을 위해 모든 SQL 정의는 이 단일 znode의 값으로 저장됩니다. 기본적으로 ZooKeeper는 사용되지 않으며, 정의는 [디스크](#workload_path)에 저장됩니다.

**예시**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**관련 항목**

* [워크로드 계층 구조](/ko/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

[`zookeeper_log`](/ko/operations/system-tables/zookeeper_log) 시스템 테이블(system table)에 대한 설정입니다.

다음 설정은 하위 태그를 통해 구성할 수 있습니다:

<SystemLogParameters />

**예시**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```