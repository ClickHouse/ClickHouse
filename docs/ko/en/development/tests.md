---
description: 'ClickHouse 테스트 및 테스트 모음 실행 가이드'
sidebar_label: '테스트'
sidebar_position: 40
slug: /development/tests
title: 'ClickHouse 테스트'
doc_type: 'guide'
---

<div id="test-types">
  ## 테스트 유형
</div>

ClickHouse에는 다음과 같은 테스트가 있습니다:

* [기능 테스트](#functional-tests) - 다음과 같이 서로 겹치는 부분 집합으로 구성된 쿼리와 스크립트의 집합입니다
  * [빠른 테스트](#running-fast-tests) - 가장 작은 부분 집합
  * [stateless 테스트](#running-stateless-tests) - 데이터베이스를 데이터로 채울 필요가 없는 테스트
  * 병렬로 실행할 수 없는 순차 테스트
* [통합 테스트](#integration-tests) - 클러스터에서 `pytest`로 실행됩니다
* [단위 테스트](#unit-tests)
* [성능 테스트](#performance-tests)
* [빌드 테스트](#build-tests)
* [새니타이저](#sanitizers)
* [퍼저](#fuzzing)
  그 밖의 테스트도 있으며, 자세한 내용은 아래 섹션을 참조하십시오.

<div id="functional-tests">
  ## 기능 테스트
</div>

기능 테스트는 가장 간단하고 편리하게 사용할 수 있는 테스트입니다.
ClickHouse의 대부분의 기능은 기능 테스트로 검증할 수 있으며, 이러한 방식으로 테스트할 수 있는 ClickHouse 코드 변경에는 반드시 기능 테스트를 사용해야 합니다.

각 기능 테스트는 실행 중인 ClickHouse 서버에 하나 이상의 쿼리를 보내고, 그 결과를 기준 결과(reference)와 비교합니다.

테스트는 `./tests/queries` 디렉터리에 있습니다.

각 테스트는 `.sql`과 `.sh` 두 가지 유형 중 하나입니다.

* `.sql` 테스트는 `clickhouse-client`에 파이프로 전달되는 간단한 SQL 스크립트입니다.
* `.sh` 테스트는 독립적으로 실행되는 스크립트입니다.

일반적으로 SQL 테스트가 `.sh` 테스트보다 더 권장됩니다.
순수 SQL만으로는 검증할 수 없는 기능을 테스트해야 할 때만 `.sh` 테스트를 사용하십시오. 예를 들어 입력 데이터를 `clickhouse-client`에 파이프로 전달하거나 `clickhouse-local`을 테스트하는 경우입니다.

:::note
데이터 타입 `DateTime` 및 `DateTime64`를 테스트할 때 흔히 하는 실수는 서버가 특정 시간대(예: &quot;UTC&quot;)를 사용한다고 가정하는 것입니다. 하지만 실제로는 그렇지 않습니다. CI 테스트 실행에서는 시간대가 의도적으로 무작위로 설정됩니다. 가장 쉬운 해결 방법은 테스트 값에 시간대를 명시적으로 지정하는 것입니다. 예: `toDateTime64(val, 3, 'Europe/Amsterdam')`.
:::

<div id="running-a-test-locally">
  ### 로컬에서 테스트 실행하기
</div>

기본 포트(9000)로 수신 대기하도록 로컬에서 ClickHouse 서버를 시작합니다.
예를 들어 `01428_hash_set_nan_key` 테스트를 실행하려면 리포지토리 폴더로 이동한 후 다음 명령을 실행합니다:

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

테스트 결과(`stderr` 및 `stdout`)는 테스트 파일과 같은 위치에 있는 `01428_hash_set_nan_key.[stderr|stdout]` 파일에 기록됩니다(`queries/0_stateless/foo.sql`의 경우 출력은 `queries/0_stateless/foo.stdout`에 기록됩니다).

`clickhouse-test`의 모든 옵션은 `tests/clickhouse-test --help`를 참조하십시오.
테스트 이름 필터를 지정하여 모든 테스트를 실행하거나 테스트 부분 집합을 실행할 수 있습니다: `./clickhouse-test substring`.
테스트를 병렬로 실행하거나 무작위 순서로 실행하는 옵션도 있습니다.

<div id="running-tests-on-macos">
  #### macOS(Darwin)에서 테스트 실행하기
</div>

많은 기능 테스트는 GNU 명령줄 유틸리티(`timeout`, `head`, `sed`, `grep`, `date` 등)를 셸을 통해 실행합니다. macOS에는 이러한 도구의 BSD 버전이 기본으로 포함되어 있지만, 동작과 옵션이 다릅니다(예를 들어 BSD `head`는 `head -c 1G`를 지원하지 않고, BSD `ps`에는 `--` 형식의 긴 옵션이 없으며, `timeout`은 아예 없습니다). BSD 도구로 테스트를 실행하면 무관한 실패가 발생합니다.

macOS CI 러너는 Homebrew로 GNU 도구를 설치하고, `PATH`에서 BSD 도구보다 앞에 오도록 설정합니다. 로컬에서도 동일하게 재현하십시오:

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils`는 GNU `timeout`, `head`, `date` 등의 도구를 제공하며, `gnu-sed`와 `grep`는 GNU `sed`와 `grep`를 제공합니다. 그다음 `which timeout head sed grep`를 실행하면 `gnubin` 경로를 가리켜야 합니다.

<div id="running-fast-tests">
  ### 빠른 테스트 실행
</div>

일부 테스트(「빠른 테스트」라고 함)를 실행하려면 제법 성능이 좋은 머신이 필요할 수 있습니다. 아래 내용은 100 GB 스토리지를 갖춘 `t3.2xlarge` AWS amd64 Ubuntu 인스턴스에서 동작합니다.

1. 필수 구성 요소를 설치한 후 다시 로그인하세요.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. 소스 코드를 다운로드하세요.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. 코드를 빌드한 후 &quot;빠른 테스트&quot;를 실행하십시오.

```sh
python -m ci.praktika run fast
```

다음과 같은 결과가 출력됩니다

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

실행을 unattended 상태로 둘 경우, `ssh` 연결이 끊어진 후에도 계속 실행되도록 `nohup` 또는 `disown`을 사용할 수 있습니다.

<div id="running-stateless-tests">
  ### stateless 테스트 실행
</div>

stateless 테스트를 실행하려면 어느 정도 성능이 좋은 머신이 필요할 수 있습니다. 아래 구성은 200 GB 스토리지를 갖춘 `m7i.8xlarge` AWS amd64 Ubuntu 인스턴스에서 동작합니다.

1. 필수 구성 요소를 설치하고 다시 로그인하세요.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

2. 소스 코드를 다운로드하세요.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. 코드를 빌드하세요.

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. 병렬 실행이 가능한 stateless tests를 실행하세요.

```sh
python -m ci.praktika run functional
```

다음과 같은 결과가 나와야 합니다

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

참고. `python -m ci.praktika run` 호출은 특정 CI 작업을 실행합니다. ClickHouse CI에 관한 자세한 내용은 [여기](continuous-integration.md#running-stateless-tests)에서 확인하십시오.

<div id="adding-a-new-test">
  ### 새 테스트 추가
</div>

새 테스트를 추가하려면 먼저 `queries/0_stateless` 디렉터리에 `.sql` 또는 `.sh` 파일을 만드십시오.
그런 다음 `clickhouse-client < 12345_test.sql > 12345_test.reference` 또는 `./12345_test.sh > ./12345_test.reference`를 사용해 해당 `.reference` 파일을 생성합니다.

테스트에서는 미리 자동 생성되는 데이터베이스(database) `test` 안의 테이블(table)에 대해서만 생성, 삭제, 조회 등의 작업을 수행해야 합니다.
임시 테이블을 사용해도 됩니다.

로컬에서 CI와 동일한 환경을 설정하려면 테스트 구성을 설치하십시오(이 구성은 ZooKeeper mock 구현을 사용하며 일부 설정도 조정합니다)

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
테스트는 다음 조건을 충족해야 합니다.

* 최소화: 꼭 필요한 최소한의 테이블, 컬럼, 복잡도만 생성합니다.
* 신속함: 몇 초 이상 걸리지 않아야 하며(가능하면 1초 미만이 바람직함),
* 정확하고 결정적임: 테스트 대상 기능이 동작하지 않을 때에만 실패합니다.
* 격리됨/stateless: 환경이나 타이밍에 의존하지 않습니다.
* 포괄적임: 0, NULL 값, 빈 집합, 예외 같은 코너 케이스를 다룹니다(음성 테스트의 경우 이를 위해 `-- { serverError xyz }` 및 `-- { clientError xyz }` 구문을 사용하십시오).
* 테스트가 끝나면 테이블을 정리합니다(남은 항목이 있을 경우에 대비).
* 다른 테스트가 동일한 내용을 검증하지 않는지 확인합니다(즉, 먼저 grep으로 확인하십시오).
  :::

<div id="templated-tests-with-jinja">
  ### Jinja를 사용한 템플릿 테스트
</div>

파일 이름에 `.j2` 접미사를 추가하면 `.sql` 테스트를 [Jinja2](https://jinja.palletsprojects.com/) 템플릿으로 작성할 수 있습니다. 즉, `foo.sql`은 `foo.sql.j2`가 됩니다. 테스트를 실행하기 전에 `clickhouse-test`가 템플릿을 일반 `.sql` 스크립트로 렌더링한 뒤, 그 결과를 실행합니다.

이 기능은 테스트에서 약간만 달라지는 동일한 쿼리를 반복할 때 유용합니다. 각 쿼리를 일일이 직접 작성하는 대신, 루프를 사용해 간결한 템플릿으로부터 쿼리를 생성할 수 있습니다. 가장 자주 사용되는 구문은 다음과 같습니다.

* 블록을 반복하는 `{% for ... %} ... {% endfor %}`,
* 출력에 값을 삽입하는 `{{ expression }}`,
* 생성된 스크립트를 깔끔하게 유지하도록 인접한 공백을 제거하는 `-%}` 및 `{%-`.

예를 들어, 다음 템플릿이 있습니다.

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

다음과 같이 렌더링됩니다:

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

예상 출력은 완전히 확장된 결과를 담은 일반 `<name>.reference` 파일로 제공하거나, `clickhouse-test`가 비교 전에 동일한 방식으로 렌더링하는 `<name>.reference.j2` 템플릿으로 제공할 수 있습니다. 예상 출력에도 반복되는 패턴이 있는 경우에는 템플릿 형식을 사용하십시오. 더 많은 예시는 `tests/queries/0_stateless/`의 기존 `*.sql.j2` 파일을 참조하십시오.

<div id="restricting-test-runs">
  ### 테스트 실행 제한
</div>

테스트에는 CI에서 어떤 조건으로 실행할지 제한하는 *태그*를 0개 이상 지정할 수 있습니다.

`.sql` 테스트에서는 태그를 첫 번째 줄의 SQL 주석에 배치합니다:

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

`.sh` 테스트에서는 둘째 줄에 주석으로 태그를 작성합니다:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

사용 가능한 태그 목록:

| 태그 이름                          | 설명                                                        | 사용 예시                                               |
| ------------------------------ | --------------------------------------------------------- | --------------------------------------------------- |
| `disabled`                     | 테스트를 실행하지 않습니다                                            |                                                     |
| `long`                         | 테스트 실행 시간이 1분에서 10분으로 늘어납니다                               |                                                     |
| `deadlock`                     | 테스트를 장시간 루프에서 실행합니다                                       |                                                     |
| `race`                         | `deadlock`와 동일합니다. `deadlock` 사용을 권장합니다                   |                                                     |
| `shard`                        | 서버가 `127.0.0.*` 주소에서 수신 대기해야 합니다                          |                                                     |
| `distributed`                  | `shard`와 동일합니다. `shard` 사용을 권장합니다                         |                                                     |
| `global`                       | `shard`와 동일합니다. `shard` 사용을 권장합니다                         |                                                     |
| `zookeeper`                    | 테스트를 실행하려면 ZooKeeper 또는 ClickHouse Keeper가 필요합니다          | 테스트에서 `ReplicatedMergeTree`를 사용합니다                  |
| `replica`                      | `zookeeper`와 동일합니다. `zookeeper` 사용을 권장합니다                 |                                                     |
| `no-fasttest`                  | 테스트는 [빠른 테스트](#test-types)에서 실행되지 않습니다                    | 테스트에서 빠른 테스트에 비활성화된 `MySQL` 테이블 엔진을 사용합니다           |
| `fasttest-only`                | 테스트는 [빠른 테스트](#test-types)에서만 실행됩니다                       |                                                     |
| `no-[asan, tsan, msan, ubsan]` | [새니타이저](#sanitizers)가 포함된 빌드에서는 테스트를 비활성화합니다              | 테스트는 QEMU에서 실행되며, QEMU는 새니타이저와 함께 작동하지 않습니다         |
| `no-replicated-database`       | 기본 데이터베이스가 `ReplicatedDatabaseEngine`를 사용할 때 테스트를 비활성화합니다 |                                                     |
| `no-ordinary-database`         | 기본 데이터베이스 엔진이 `Ordinary`일 때 테스트를 비활성화합니다                  |                                                     |
| `no-parallel`                  | 이 테스트와 다른 테스트가 병렬로 실행되지 않도록 합니다                           | 테스트가 `system` 테이블에서 읽기를 수행하므로 불변 조건이 깨질 수 있습니다      |
| `no-parallel-replicas`         | 병렬 레플리카가 활성화된 경우 테스트를 비활성화합니다                             |                                                     |
| `no-debug`                     | Debug 빌드에서는 테스트를 비활성화합니다                                  |                                                     |
| `no-release`                   | Release 빌드에서는 테스트를 비활성화합니다                                |                                                     |
| `no-darwin`                    | macOS(Darwin)에서는 테스트를 비활성화합니다                             | 테스트가 분산 쿼리, `procfs`, HTTP 서버 같은 Linux 전용 기능에 의존합니다 |

다음 옵션도 지원합니다: `no-polymorphic-parts`, `no-random-settings`, `no-random-merge-tree-settings`, `no-backward-compatibility-check`, `no-cpu-x86_64`, `no-cpu-aarch64`, `no-cpu-ppc64le`, `no-s3-storage`.

위 설정 외에도 특정 ClickHouse 기능의 사용 여부를 정의하기 위해 `system.build_options`의 `USE_*` 플래그를 사용할 수 있습니다.
예를 들어 테스트에서 MySQL 테이블을 사용하는 경우 `use-mysql` 태그를 추가해야 합니다.

<div id="specifying-limits-for-random-settings">
  ### 무작위 설정의 제한 지정
</div>

테스트 실행 중 무작위로 설정할 수 있는 값에 대해 테스트에서 허용되는 최솟값과 최댓값을 지정할 수 있습니다.

`.sh` 테스트에서는 제한을 태그 옆 줄의 주석에 작성하거나, 태그가 지정되지 않은 경우 둘째 줄에 작성합니다:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

`.sql` 테스트에서는 태그를 `tags` 다음 줄이나 첫 번째 줄에 SQL 주석으로 배치합니다:

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

하나의 제한값만 지정해야 하는 경우, 나머지 제한값에는 `None`을 사용할 수 있습니다.

<div id="choosing-the-test-name">
  ### 테스트 이름 선택
</div>

테스트 이름은 `00422_hash_function_constexpr.sql`처럼 5자리 접두사 뒤에 설명적인 이름이 오는 형식입니다.
접두사를 정하려면 디렉터리에 이미 있는 가장 큰 접두사를 찾은 다음 1을 더하십시오.

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

그 사이에 같은 숫자 접두사를 가진 다른 테스트가 추가될 수도 있지만, 이는 문제없으며 아무런 문제를 일으키지 않으므로 나중에 변경할 필요는 없습니다.

<div id="checking-for-an-error-that-must-occur">
  ### 반드시 발생해야 하는 오류 확인
</div>

잘못된 쿼리에서 서버 오류가 발생하는지 테스트해야 할 때가 있습니다. 이를 위해 SQL 테스트에서는 다음 형식의 특수 어노테이션을 지원합니다:

```sql
SELECT x; -- { serverError 49 }
```

이 테스트는 서버가 알 수 없는 컬럼 `x`에 대해 코드 49 오류를 반환하는지 확인합니다.
오류가 발생하지 않거나 다른 오류가 발생하면 테스트는 실패합니다.
오류가 클라이언트 측에서 발생하는지 확인하려면 대신 `clientError` annotation을 사용하십시오.

오류 메시지의 특정 문구는 확인하지 마십시오. 향후 변경될 수 있으며, 그 때문에 테스트가 불필요하게 실패할 수 있습니다.
오류 코드만 확인하십시오.
기존 오류 코드가 필요한 만큼 정확하지 않다면 새 코드를 추가하는 것을 고려하십시오.

<div id="testing-a-distributed-query">
  ### 분산 쿼리 테스트하기
</div>

기능 테스트에서 분산 쿼리를 사용하려면 `127.0.0.{1..2}` 주소와 `remote` 테이블 함수를 사용해 서버가 자기 자신에게 쿼리하도록 할 수 있습니다. 또는 `test_shard_localhost`처럼 서버 구성 파일에 미리 정의된 테스트 cluster를 사용할 수도 있습니다.
서버가 분산 쿼리를 지원하도록 구성된 올바른 환경에서 CI가 테스트를 실행할 수 있도록 테스트 이름에 `shard` 또는 `distributed`라는 단어를 추가해야 합니다.

<div id="working-with-temporary-files">
  ### 임시 파일 작업하기
</div>

셸 테스트에서는 작업 중 즉시 파일을 만들어야 할 때가 있습니다.
일부 CI 검사에서는 테스트를 병렬로 실행하므로, 스크립트에서 고유한 이름 없이 임시 파일을 만들거나 삭제하면 Flaky와 같은 일부 CI 검사가 실패할 수 있다는 점에 유의하십시오.
이 문제를 피하려면 환경 변수 `$CLICKHOUSE_TEST_UNIQUE_NAME`을 사용해 현재 실행 중인 테스트에만 고유한 이름으로 임시 파일을 지정해야 합니다.
그러면 setup 중에 생성하거나 cleanup 중에 삭제하는 파일이 해당 테스트에서만 사용하는 파일이며, 병렬로 실행 중인 다른 테스트에서 사용하는 파일이 아님을 확실히 할 수 있습니다.

<div id="known-bugs">
  ## 알려진 버그
</div>

기능 테스트로 쉽게 재현할 수 있는 버그를 알고 있는 경우, 미리 준비한 기능 테스트를 `tests/queries/bugs` 디렉터리에 둡니다.
이 테스트는 버그가 수정되면 `tests/queries/0_stateless`로 이동됩니다.

<div id="integration-tests">
  ## 통합 테스트
</div>

통합 테스트를 사용하면 클러스터 구성의 ClickHouse와 MySQL, Postgres, MongoDB 같은 다른 서버와의 상호작용을 테스트할 수 있습니다.
이 테스트는 네트워크 분할, packet 삭제 등을 에뮬레이션하는 데 유용합니다.
이 테스트는 Docker에서 실행되며 다양한 소프트웨어가 포함된 여러 컨테이너를 생성합니다.

이 테스트를 실행하는 방법은 `tests/integration/README.md`를 참조하십시오.

ClickHouse와 타사 드라이버의 통합은 테스트되지 않는다는 점에 유의하십시오.
또한 현재는 JDBC 및 ODBC 드라이버에 대한 통합 테스트도 없습니다.

<div id="unit-tests">
  ## 단위 테스트
</div>

단위 테스트는 ClickHouse 전체가 아니라, 분리된 단일 라이브러리나 클래스를 테스트하려는 경우에 유용합니다.
`ENABLE_TESTS` CMake 옵션으로 테스트 빌드를 활성화하거나 비활성화할 수 있습니다.
단위 테스트(및 기타 테스트 프로그램)는 코드 전반의 `tests` 하위 디렉터리에 있습니다.
단위 테스트를 실행하려면 `ninja test`를 입력하십시오.
일부 테스트는 `gtest`를 사용하지만, 일부는 테스트가 실패하면 0이 아닌 종료 코드를 반환하는 단순한 프로그램입니다.

코드가 이미 기능 테스트로 충분히 검증된다면 단위 테스트는 꼭 필요하지 않습니다(그리고 기능 테스트가 일반적으로 사용하기가 훨씬 더 간단합니다).

개별 `gtest` 검사는 실행 파일을 직접 호출하여 실행할 수 있습니다. 예를 들면 다음과 같습니다:

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## 성능 테스트
</div>

성능 테스트를 사용하면 합성 쿼리로 ClickHouse의 특정 독립 구성 요소에 대한 성능을 측정하고 비교할 수 있습니다.
성능 테스트는 `tests/performance/`에 있습니다.
각 테스트는 테스트 케이스 설명이 담긴 `.xml` 파일로 구성됩니다.
테스트는 `docker/test/performance-comparison` 도구로 실행합니다. 실행 방법은 readme 파일을 참고하십시오.

각 테스트는 하나 이상의 쿼리(매개변수 조합을 포함할 수 있음)를 반복해서 실행합니다.

특정 시나리오에서 ClickHouse의 성능을 개선하려고 하고, 그 개선 효과를 단순한 쿼리로 확인할 수 있다면 성능 테스트를 작성하는 것을 강력히 권장합니다.
또한 비교적 독립적이고 지나치게 난해하지 않은 SQL 함수를 추가하거나 수정할 때도 성능 테스트를 작성하는 것이 좋습니다.
테스트 중에는 항상 `perf top` 또는 다른 `perf` 도구를 사용하는 것이 유용합니다.

<div id="test-tools-and-scripts">
  ## 테스트 도구와 스크립트
</div>

`tests` 디렉터리의 일부 프로그램은 미리 작성된 테스트가 아니라 테스트 도구입니다.
예를 들어 `Lexer`에는 stdin을 토큰화하고 그 결과를 색상 적용된 형태로 stdout에 출력만 하는 `src/Parsers/tests/lexer` 도구가 있습니다.
이러한 도구는 code 예시로 활용하거나, 동작을 살펴보고 수동으로 테스트할 때 사용할 수 있습니다.

<div id="miscellaneous-tests">
  ## 기타 테스트
</div>

`tests/external_models`에는 머신러닝 모델용 테스트가 있습니다.
이 테스트는 더 이상 업데이트되지 않으므로 통합 테스트로 옮겨야 합니다.

quorum inserts를 위한 별도의 테스트도 있습니다.
이 테스트는 별도의 서버에서 ClickHouse 클러스터를 실행하고, [Jepsen](https://aphyr.com/tags/Jepsen)처럼 네트워크 분할, 패킷 드롭(ClickHouse 노드 간, ClickHouse와 ZooKeeper 간, ClickHouse 서버와 클라이언트 간 등), `kill -9`, `kill -STOP`, `kill -CONT` 등 다양한 장애 상황을 재현합니다. 그런 다음 확인 응답을 받은 모든 삽입은 기록되었고, 거부된 모든 삽입은 기록되지 않았는지 검사합니다.

<div id="manual-testing">
  ## 수동 테스트
</div>

새 기능을 개발할 때는 수동 테스트도 함께 수행하는 것이 좋습니다.
다음 단계에 따라 진행할 수 있습니다:

ClickHouse를 빌드합니다. 터미널에서 ClickHouse를 실행합니다. `programs/clickhouse-server` 디렉터리로 이동한 뒤 `./clickhouse-server`를 실행합니다. 기본적으로 현재 디렉터리의 구성(`config.xml`, `users.xml`, 그리고 `config.d` 및 `users.d` 디렉터리 내 파일)을 사용합니다. ClickHouse 서버에 연결하려면 `programs/clickhouse-client/clickhouse-client`를 실행합니다.

모든 clickhouse 도구(server, client 등)는 실제로 `clickhouse`라는 단일 binary에 대한 심볼릭 링크일 뿐이라는 점에 유의하십시오.
이 binary는 `programs/clickhouse`에서 찾을 수 있습니다.
모든 도구는 `clickhouse-tool` 대신 `clickhouse tool` 형태로도 호출할 수 있습니다.

또는 ClickHouse package를 설치할 수도 있습니다. ClickHouse repository의 안정 release를 설치하거나, ClickHouse sources 루트에서 `./release`를 실행해 직접 package를 빌드할 수 있습니다.
그런 다음 `sudo clickhouse start`로 서버를 시작합니다(서버를 중지하려면 `stop` 사용).
로그는 `/etc/clickhouse-server/clickhouse-server.log`에서 확인하십시오.

시스템에 ClickHouse가 이미 설치되어 있다면 새 `clickhouse` binary를 빌드한 뒤 기존 binary를 대체할 수 있습니다:

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

또한 시스템의 clickhouse-server를 중지한 뒤, 동일한 구성을 사용하되 로그가 터미널에 출력되도록 직접 실행할 수 있습니다:

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

gdb를 사용한 예시:

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

시스템에서 `clickhouse-server`가 이미 실행 중이고 이를 중지하고 싶지 않다면, `config.xml`에서 포트 번호를 변경하거나(`config.d` 디렉터리의 파일에서 재정의할 수도 있습니다) 적절한 데이터 경로를 지정한 다음 실행할 수 있습니다.

`clickhouse` 바이너리는 의존성이 거의 없으며 다양한 Linux 배포판에서 작동합니다.
서버에서 변경 사항을 빠르게 간단히 테스트하려면, 새로 빌드한 `clickhouse` 바이너리를 서버로 `scp`한 다음 위의 예시와 같이 실행하면 됩니다.

<div id="build-tests">
  ## 빌드 테스트
</div>

빌드 테스트는 다양한 대체 구성과 일부 다른 시스템에서 빌드가 깨지지 않았는지 확인할 수 있게 해줍니다.
이 테스트 역시 자동화되어 있습니다.

예시:

* Darwin x86&#95;64(macOS)용 교차 컴파일
* FreeBSD x86&#95;64용 교차 컴파일
* Linux AArch64용 교차 컴파일
* 시스템 패키지의 라이브러리를 사용해 Ubuntu에서 빌드(권장되지 않음)
* 라이브러리를 공유 링크 방식으로 빌드(권장되지 않음)

예를 들어, 시스템 패키지를 사용한 빌드는 시스템에 어떤 정확한 버전의 패키지가 설치되어 있을지 보장할 수 없기 때문에 바람직하지 않은 방식입니다.
하지만 Debian 유지 관리자에게는 이것이 꼭 필요합니다.
이 때문에 최소한 이 빌드 방식은 지원해야 합니다.
또 다른 예로, 공유 링크는 흔히 문제를 일으키지만 일부 사용자에게는 필요합니다.

모든 빌드 방식에서 모든 테스트를 실행할 수는 없지만, 적어도 다양한 빌드 방식이 깨지지 않았는지는 확인하고자 합니다.
이 목적을 위해 빌드 테스트를 사용합니다.

또한 컴파일하기에 너무 길거나 RAM을 지나치게 많이 요구하는 translation unit이 없는지도 테스트합니다.

또한 지나치게 큰 stack frame이 없는지도 테스트합니다.

<div id="testing-for-protocol-compatibility">
  ## 프로토콜 호환성 테스트
</div>

ClickHouse 네트워크 프로토콜을 확장할 때는 기존 clickhouse-client가 새 clickhouse-server와 함께 작동하는지, 그리고 새 clickhouse-client가 기존 clickhouse-server와 함께 작동하는지를 수동으로 테스트합니다(해당 패키지의 바이너리를 실행하기만 하면 됩니다).

또한 일부 사례는 통합 테스트를 통해 자동으로 검증합니다.

* 이전 버전의 ClickHouse에서 기록한 데이터를 새 버전에서 성공적으로 읽을 수 있는지;
* 서로 다른 ClickHouse 버전으로 구성된 클러스터에서 분산 쿼리가 작동하는지.

<div id="help-from-the-compiler">
  ## 컴파일러의 도움
</div>

주요 ClickHouse 코드(`src` 디렉터리에 있음)는 `-Wall -Wextra -Werror`와 몇 가지 추가 경고를 활성화한 상태로 빌드됩니다.
다만 이러한 옵션은 타사 라이브러리에는 활성화되지 않습니다.

Clang에는 이보다 더 유용한 경고가 더 있으며, `-Weverything`으로 이를 확인한 뒤 기본 빌드에 포함할 항목을 선택할 수 있습니다.

ClickHouse는 개발 환경과 프로덕션 환경 모두에서 항상 clang으로 빌드합니다.
로컬 머신에서는 debug mode로 빌드할 수 있으며(노트북 배터리를 절약하기 위해), 더 나은 제어 흐름(control flow) 및 프로시저 간 분석 덕분에 컴파일러가 `-O3`에서 더 많은 경고를 생성할 수 있다는 점에 유의하십시오.
clang의 debug mode로 빌드하면 런타임에 더 많은 오류를 포착할 수 있도록 `libc++`의 debug 버전이 사용됩니다.

<div id="sanitizers">
  ## 새니타이저
</div>

:::note
로컬에서 실행할 때 프로세스(ClickHouse 서버 또는 클라이언트)가 시작할 때 크래시가 발생하면 주소 공간 배치 난수화(address space layout randomization)를 비활성화해야 할 수 있습니다: `sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Address 새니타이저
</div>

커밋마다 ASan으로 기능 테스트, 통합 테스트, 스트레스 테스트 및 단위 테스트를 실행합니다.

<div id="thread-sanitizer">
  ### 스레드 새니타이저
</div>

커밋마다 TSan에서 기능 테스트, 통합 테스트, 스트레스 테스트, 단위 테스트를 실행합니다.

<div id="memory-sanitizer">
  ### 메모리 새니타이저
</div>

각 commit 단위로 MSan 환경에서 functional, 통합, 스트레스 및 단위 테스트를 실행합니다.

<div id="undefined-behaviour-sanitizer">
  ### 정의되지 않은 동작 새니타이저
</div>

각 커밋마다 UBSan 환경에서 functional 테스트, 통합 테스트, 스트레스 테스트 및 단위 테스트를 실행합니다.
일부 타사 라이브러리 코드는 UB에 대한 새니타이저가 적용되지 않습니다.

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

예전에는 기능 테스트를 Valgrind에서 밤새 실행했지만, 지금은 더 이상 그렇게 하지 않습니다.
실행에 몇 시간씩 걸립니다.
현재 `re2` 라이브러리에서 알려진 오탐(false positive)이 1건 있으며, [이 문서](https://research.swtch.com/sparse)를 참조하십시오.

<div id="fuzzing">
  ## 퍼징
</div>

ClickHouse 퍼징은 [libFuzzer](https://llvm.org/docs/LibFuzzer.html)와 무작위 SQL 쿼리를 모두 사용해 구현됩니다.
모든 퍼즈 테스트는 새니타이저(Address 및 Undefined)와 함께 수행해야 합니다.

libFuzzer는 라이브러리 코드에 대한 격리된 퍼즈 테스트에 사용됩니다.
퍼저는 테스트 코드의 일부로 구현되며, 이름 접미사로 &quot;&#95;fuzzer&quot;를 사용합니다.
퍼저 예시는 `src/Parsers/fuzzers/lexer_fuzzer.cpp`에서 확인할 수 있습니다.
libFuzzer 전용 구성, 사전 파일 및 코퍼스는 `tests/fuzz`에 저장됩니다.
사용자 입력을 처리하는 모든 기능에 대해 퍼즈 테스트를 작성할 것을 권장합니다.

퍼저는 기본적으로 빌드되지 않습니다.
퍼저를 빌드하려면 `-DENABLE_FUZZING=1` 및 `-DENABLE_TESTS=1` 옵션을 모두 설정해야 합니다.
퍼저를 빌드할 때는 Jemalloc을 비활성화하는 것을 권장합니다.
ClickHouse 퍼징을
Google OSS-Fuzz에 통합하는 데 사용되는 구성은 `docker/fuzz`에서 확인할 수 있습니다.

또한 무작위 SQL 쿼리를 생성하고 이를 실행하는 동안 서버가 종료되지 않는지 확인하는 단순한 퍼즈 테스트도 사용합니다.
이는 `00746_sql_fuzzy.pl`에서 찾을 수 있습니다.
이 테스트는 지속적으로(하룻밤 이상 장시간) 실행해야 합니다.

또한 대량의 코너 케이스를 찾아낼 수 있는 정교한 AST 기반 쿼리 퍼저도 사용합니다.
이 퍼저는 쿼리 AST에서 무작위 순열과 치환을 수행합니다.
이전 테스트의 AST 노드를 기억해 두었다가, 이후 테스트를 무작위 순서로 처리하면서 다음 테스트의 퍼징에 활용합니다.
이 퍼저에 대해 자세히 알아보려면 [이 블로그 글](https://clickhouse.com/blog/fuzzing-click-house)을 참고하십시오.

<div id="stress-test">
  ## 스트레스 테스트
</div>

스트레스 테스트는 퍼징의 또 다른 형태입니다.
단일 서버에서 모든 기능 테스트를 무작위 순서로 병렬 실행합니다.
테스트 결과는 확인하지 않습니다.

다음 사항을 확인합니다:

* 서버가 크래시하지 않고, 디버그 또는 새니타이저 트랩이 트리거되지 않습니다.
* 교착 상태가 없습니다.
* 데이터베이스 구조의 일관성이 유지됩니다.
* 테스트 후 서버를 정상적으로 중지할 수 있으며, 예외 없이 다시 시작할 수 있습니다.

변형은 5가지(Debug, ASan, TSan, MSan, UBSan)입니다.

<div id="thread-fuzzer">
  ## Thread 퍼저
</div>

Thread 퍼저(Thread Sanitizer와 혼동하지 마십시오)는 스레드의 실행 순서를 무작위로 바꿀 수 있게 해 주는 또 다른 종류의 퍼징입니다.
이를 통해 더 많은 특이한 경우를 찾아내는 데 도움이 됩니다.

<div id="security-audit">
  ## 보안 감사
</div>

당사 보안 팀은 보안 관점에서 ClickHouse의 기능을 개괄적으로 검토했습니다.

<div id="static-analyzers">
  ## 정적 분석기
</div>

각 커밋마다 `clang-tidy`를 실행합니다.
`clang-static-analyzer` 검사도 활성화되어 있습니다.
`clang-tidy`는 일부 스타일 검사에도 사용됩니다.

`clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`도 평가했습니다.
사용 방법은 `tests/instructions/` 디렉터리에서 확인할 수 있습니다.

IDE로 `CLion`을 사용하는 경우, 일부 `clang-tidy` 검사를 별도 설정 없이 활용할 수 있습니다.

셸 스크립트의 정적 분석에도 `shellcheck`를 사용합니다.

<div id="hardening">
  ## 하드닝
</div>

디버그 빌드에서는 사용자 수준 메모리 할당에 ASLR을 적용하는 사용자 지정 allocator를 사용합니다.

또한 할당 후 readonly 상태여야 하는 메모리 영역을 수동으로 보호합니다.

디버그 빌드에서는 &quot;유해한&quot;(obsolete, insecure, not thread-safe) 함수가 호출되지 않도록 libc를 추가로 커스터마이징합니다.

디버그 assertion을 광범위하게 사용합니다.

디버그 빌드에서는 &quot;logical error&quot; 코드의 예외(버그를 의미함)가 발생하면 프로그램을 즉시 종료합니다.
이렇게 하면 release 빌드에서는 예외를 사용할 수 있고, 디버그 빌드에서는 이를 assertion처럼 동작하게 할 수 있습니다.

디버그 빌드에는 jemalloc의 디버그 버전을 사용합니다.
디버그 빌드에는 libc++의 디버그 버전을 사용합니다.

<div id="runtime-integrity-checks">
  ## 런타임 무결성 검사
</div>

디스크에 저장된 데이터에는 체크섬이 계산됩니다.
MergeTree 테이블의 데이터에는 세 가지 방식으로 동시에 체크섬이 계산됩니다* (압축 데이터 블록, 비압축 데이터 블록, 블록 전체에 대한 총 체크섬).
클라이언트와 서버 사이 또는 서버 간에 네트워크를 통해 전송되는 데이터에도 체크섬이 계산됩니다.
복제를 통해 레플리카 간 데이터가 비트 단위까지 동일하게 유지됩니다.

이는 결함이 있는 하드웨어로부터 보호하기 위해 필요합니다 (저장 매체의 비트 로트(bit rot), 서버 RAM에서의 비트 반전, 네트워크 컨트롤러 RAM에서의 비트 반전, 네트워크 스위치 RAM에서의 비트 반전, 클라이언트 RAM에서의 비트 반전, 전송 구간에서의 비트 반전).
비트 반전은 흔하며, ECC RAM을 사용하고 TCP 체크섬이 있어도 충분히 발생할 수 있다는 점에 유의하십시오 (매일 페타바이트 규모의 데이터를 처리하는 수천 대의 서버를 운영하는 경우).
[동영상 보기 (러시아어)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse는 운영 엔지니어가 결함이 있는 하드웨어를 찾는 데 도움이 되는 진단 기능을 제공합니다.

* 그리고 성능 저하도 거의 없습니다.

<div id="code-style">
  ## 코드 스타일
</div>

코드 스타일 규칙은 [여기](style.md)에 설명되어 있습니다.

일반적인 스타일 위반 사항 몇 가지를 확인하려면 `utils/check-style` 스크립트를 사용할 수 있습니다.

코드 스타일을 올바르게 맞추려면 `clang-format`을 사용할 수 있습니다.
`.clang-format` 파일은 소스 루트에 있습니다.
이 파일은 대체로 현재 코드 스타일과 일치합니다.
하지만 기존 파일에 `clang-format`을 적용하면 포맷팅이 오히려 나빠질 수 있으므로 권장하지 않습니다.
clang 소스 리포지토리에서 찾을 수 있는 `clang-format-diff` 도구를 사용할 수도 있습니다.

또는 `uncrustify` 도구를 사용해 코드를 다시 포맷할 수도 있습니다.
구성은 소스 루트의 `uncrustify.cfg`에 있습니다.
이 도구는 `clang-format`보다 테스트가 덜 되었습니다.

`CLion`에는 자체 코드 포매터가 있으며, 우리 코드 스타일에 맞게 조정해야 합니다.

<div id="test-coverage">
  ## 테스트 커버리지
</div>

테스트 커버리지도 추적하지만, 기능 테스트와 clickhouse-server에 대해서만 추적합니다.
이 작업은 매일 수행됩니다.

<div id="tests-for-tests">
  ## 테스트를 검증하는 테스트
</div>

불안정한 테스트를 감지하는 자동화된 check가 있습니다.
새로 추가된 모든 테스트를 100회(기능 테스트) 또는 10회(통합 테스트) 실행합니다.
테스트가 단 한 번이라도 Failed 되면 불안정한 테스트로 간주됩니다.

<div id="test-automation">
  ## 테스트 자동화
</div>

테스트는 [GitHub Actions](https://github.com/features/actions)로 실행합니다.

빌드 작업과 테스트는 커밋마다 Sandbox에서 실행됩니다.
생성된 패키지와 테스트 결과는 GitHub에 게시되며, 직접 링크로 다운로드할 수 있습니다.
빌드 산출물은 몇 개월 동안 저장됩니다.
GitHub에 pull request를 보내면 「can be tested」 태그를 지정하고, CI 시스템이 ClickHouse 패키지(release, debug, address 새니타이저 포함 등)를 빌드합니다.