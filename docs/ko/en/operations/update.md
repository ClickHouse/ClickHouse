---
description: '업그레이드 관련 문서'
sidebar_title: '자가 관리형 업그레이드'
slug: /operations/update
title: '자가 관리형 업그레이드'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## ClickHouse 업그레이드 개요
</div>

이 문서에는 다음 내용이 포함되어 있습니다:

* 일반 지침
* 권장 업그레이드 계획
* 시스템의 바이너리를 업그레이드하는 구체적인 방법

<div id="general-guidelines">
  ## 일반 지침
</div>

다음 참고 사항은 계획을 세우는 데 도움이 되며, 이 문서 후반부에서 제시하는 권장 사항의 배경을 이해하는 데도 도움이 됩니다.

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### ClickHouse 서버는 ClickHouse Keeper 또는 ZooKeeper와 별도로 업그레이드하십시오
</div>

ClickHouse Keeper 또는 Apache ZooKeeper에 보안 수정이 필요한 경우가 아니라면, ClickHouse 서버를 업그레이드할 때 Keeper까지 함께 업그레이드할 필요는 없습니다. 업그레이드 과정에서는 Keeper의 안정성을 유지해야 하므로, Keeper 업그레이드를 검토하기 전에 ClickHouse 서버 업그레이드를 먼저 완료하십시오.

<div id="minor-version-upgrades-should-be-adopted-often">
  ### 마이너 버전 업그레이드는 자주 적용해야 합니다
</div>

새로운 마이너 버전이 출시되면 가능한 한 빨리 항상 최신 마이너 버전으로 업그레이드할 것을 강력히 권장합니다. 마이너 릴리스에는 호환되지 않는 변경 사항이 없지만, 중요한 버그 수정(경우에 따라 보안 수정도 포함될 수 있음)은 포함됩니다.

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### 대상 버전이 실행되는 별도의 ClickHouse 서버에서 실험적 기능을 테스트하십시오
</div>

실험적 기능의 호환성은 언제든 어떤 형태로든 깨질 수 있습니다. 실험적 기능을 사용 중이라면 changelog를 확인하고, 대상 버전이 설치된 별도의 ClickHouse 서버를 구성해 그 환경에서 실험적 기능 사용을 테스트하는 것을 고려하십시오.

<div id="downgrades">
  ### 다운그레이드
</div>

업그레이드한 후 새 버전이 의존하는 일부 기능과 호환되지 않는다는 사실을 알게 된 경우, 새 기능을 아직 사용하기 시작하지 않았다면 비교적 최근 버전(출시 후 1년이 지나지 않은 버전)으로 다운그레이드할 수 있을 수 있습니다. 새 기능을 사용한 이후에는 다운그레이드가 작동하지 않습니다.

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### 클러스터에서 여러 ClickHouse 서버 버전 사용
</div>

ClickHouse는 1년의 호환성 유지 기간(여기에는 2개의 LTS 버전이 포함됩니다)을 유지하기 위해 노력합니다. 즉, 두 버전 간 차이가 1년 미만이거나(또는 그 사이에 LTS 버전이 2개 미만인 경우) 어떤 두 버전이든 클러스터에서 함께 동작할 수 있어야 합니다. 그러나 일부 사소한 문제가 발생할 수 있으므로(예: 분산 쿼리 성능 저하, ReplicatedMergeTree의 일부 백그라운드 작업에서 재시도 가능한 오류 등) 클러스터의 모든 구성원을 가능한 한 빨리 동일한 버전으로 업그레이드하는 것이 좋습니다.

릴리스 날짜 차이가 1년을 넘는 서로 다른 버전을 같은 클러스터에서 실행하는 것은 절대 권장하지 않습니다. 데이터 손실이 발생할 것으로 예상되지는 않지만, 클러스터를 사용할 수 없게 될 수 있습니다. 버전 차이가 1년을 초과할 경우 예상되는 문제는 다음과 같습니다.

* 클러스터가 동작하지 않을 수 있습니다
* 일부 또는 모든 쿼리가 예기치 않은 오류와 함께 실패할 수 있습니다
* 로그에 예기치 않은 오류/경고가 나타날 수 있습니다
* 이전 버전으로 다운그레이드하지 못할 수 있습니다

<div id="incremental-upgrades">
  ### 단계적 업그레이드
</div>

현재 버전과 대상 버전의 차이가 1년을 초과하면 다음 중 하나를 권장합니다.

* 다운타임이 발생하는 방식으로 업그레이드합니다(모든 서버를 중지한 후 업그레이드하고, 다시 모든 서버를 실행합니다).
* 또는 중간 버전(현재 버전보다 1년 미만 최신인 버전)을 거쳐 업그레이드합니다.

<div id="recommended-plan">
  ## 권장 계획
</div>

다음은 무중단 ClickHouse 업그레이드를 위한 권장 절차입니다.

1. 구성 변경 사항이 기본 `/etc/clickhouse-server/config.xml` 파일이 아니라 `/etc/clickhouse-server/config.d/`에 있는지 확인하십시오. 업그레이드 중 `/etc/clickhouse-server/config.xml`이 덮어써질 수 있기 때문입니다.
2. [changelog](/ko/whats-new/changelog/index.md)를 검토하여 호환되지 않는 변경 사항을 확인하십시오(대상 릴리스부터 현재 사용 중인 릴리스까지 거슬러 올라가며 확인).
3. 호환되지 않는 변경 사항 중 업그레이드 전에 적용할 수 있는 수정 사항은 미리 반영하고, 업그레이드 후 적용해야 할 변경 사항 목록도 작성하십시오.
4. 각 세그먼트에서 나머지 레플리카를 업그레이드하는 동안 계속 운영할 하나 이상의 레플리카를 선정하십시오.
5. 업그레이드할 레플리카에서 한 번에 하나씩 다음 작업을 수행하십시오.

* ClickHouse 서버 종료
* 서버를 대상 버전으로 업그레이드
* ClickHouse 서버 시작
* 시스템이 안정 상태가 되었음을 나타내는 Keeper 메시지가 나올 때까지 대기
* 다음 레플리카로 진행

6. Keeper 로그와 ClickHouse 로그에서 오류를 확인하십시오

7. 4단계에서 선정한 레플리카를 새 버전으로 업그레이드하십시오

8. 1~3단계에서 작성한 변경 사항 목록을 참고하여 업그레이드 후 필요한 변경 사항을 적용하십시오.

:::note
복제된 환경에서 여러 버전의 ClickHouse가 동시에 실행 중이면 이 오류 메시지가 나타나는 것이 정상입니다. 모든 레플리카가 동일한 버전으로 업그레이드되면 더 이상 이 메시지가 표시되지 않습니다.

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## ClickHouse 서버 바이너리 업그레이드 절차
</div>

ClickHouse가 `deb` 패키지로 설치된 경우 서버에서 다음 명령을 실행하십시오:

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

권장되는 `deb` 패키지 이외의 방법으로 ClickHouse를 설치한 경우, 해당 방식에 맞는 업데이트 방법을 사용하십시오.

:::note
하나의 세그먼트에 속한 모든 레플리카가 동시에 오프라인이 되는 상황만 없다면, 여러 서버를 한 번에 업데이트할 수 있습니다.
:::

이전 버전의 ClickHouse를 특정 버전으로 업그레이드하는 경우:

예시:

`xx.yy.a.b`는 현재 안정 버전입니다. 최신 안정 버전은 [여기](https://github.com/ClickHouse/ClickHouse/releases)에서 확인할 수 있습니다.

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```