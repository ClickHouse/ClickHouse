---
description: 'ClickHouse의 백포트 정책과 이를 구현하는 자동화 시스템의 개요'
sidebar_label: 'Backport System'
sidebar_position: 56
slug: /development/backports
title: 'Backport System'
doc_type: 'reference'
---

이 문서에서는 ClickHouse의 백포트 정책과 이를 구현하는 자동화 시스템을 설명합니다.

<div id="release-model">
  ## 릴리스 모델
</div>

ClickHouse 버전은 `YY.M.patch.build-type` 체계를 따릅니다. 여기서 `YY`는 두 자리 연도, `M`은 릴리스 월(앞에 0 없음), `patch`는 브랜치 내 패치 번호, `build`는 순차적으로 증가하는 빌드 번호이며, `type`은 `stable` 또는 `lts`입니다.

예시: `25.3.8.23-lts` — 2025년 3월 LTS, 패치 8, 빌드 23.

릴리스 트랙은 두 가지입니다.

* **안정** 릴리스는 대체로 매월 배포됩니다. 가장 최근의 안정 릴리스 3개에 패치가 제공되므로, 각 릴리스는 약 3개월 동안 활발히 지원됩니다.
* **LTS (Long-Term Support)** 릴리스는 매년 3월과 8월에 배포됩니다. 2개의 LTS 버전이 동시에 지원되며, 각각 최소 12개월 동안 지원됩니다.

프로덕션 workloads를 운영하는 사용자는 최신 안정 릴리스 또는 LTS 릴리스 중 하나를 사용하는 것이 권장되며, 패치 릴리스에는 호환되지 않는 변경 사항이 포함되지 않으므로 새 패치 버전으로 신속히 업그레이드하는 것이 좋습니다.

<div id="backport-policy">
  ## 백포트 정책
</div>

모든 변경 사항이 백포트되는 것은 아닙니다. 목표는 릴리스 브랜치를 안정적으로 유지하는 것이므로, 백포트 범위는 의도적으로 제한합니다.

* **보안 수정** — 항상 백포트됩니다.
* **치명적인 버그 수정** (예외(Exception)(logical error), 데이터 손실, 잘못된 결과, RBAC 문제) — 일반 백포트 규칙에 따라 자동으로 백포트 대상으로 선정됩니다. `pr-critical-bugfix` 레이블로 식별되며, 이 경우 `pr-must-backport`가 자동으로 추가됩니다.
* **안정성 및 회귀 수정** — 변경으로 인한 위험이 버그를 그대로 두는 위험보다 낮을 때 백포트됩니다. 메인테이너가 수동으로 추가하는 `pr-must-backport`로 식별됩니다.
* **우회 방법이 있는 경미한 버그 수정** — 일반적으로 릴리스 브랜치의 안정성을 해치지 않기 위해 백포트하지 않습니다.
* **새 기능, 개선 사항, 성능 관련 작업** — 백포트하지 않습니다.

`pr-must-backport` 레이블은 메인테이너가 PR을 백포트 대상으로 지정할 때 사용하는 수동 재정의입니다. `pr-critical-bugfix` 레이블이 있으면 CI 후크가 `pr-must-backport`를 자동으로 추가합니다 (`pr_labels_and_category.py` 참조).

**충돌 에스컬레이션.** 자동 백포트로 머지 충돌을 해결할 수 없는 경우에도 체리픽 PR은 반드시 생성해야 하며, 사람이 충돌을 해결하고 백포트를 완료할 수 있도록 원본 PR의 작성자, 머지한 사용자 및 기존 담당자에게 할당해야 합니다.

<div id="backport-tool">
  ## 백포트 도구
</div>

위에서 설명한 백포트 정책은 `tests/ci/cherry_pick.py`의 자동화 도구로 구현되어 있습니다. 이 도구는 ClickHouse 인프라에서 GitHub Actions 워크플로로 실행되며, 활성 릴리스 브랜치를 찾고, 백포트 대상 PR을 선별하고, 2단계 체리픽 및 백포트 절차를 수행하며, 충돌을 관리하고, 지연 정책을 적용하고, 레이블 동기화를 유지하는 등 모든 요구 사항을 충족합니다.

장기적인 목표는 이 구현을 다른 프로젝트에서도 도입할 수 있는 독립형 오픈 소스 Python 도구로 분리하는 것입니다. 목표 설계는 다음과 같습니다.

* **구성 가능** — 모든 정책 매개변수(적격 레이블, 지연 기간, 오래된 PR 임계값, rolling-out 동작 등)를 설정 파일로 표현하여, 코드 변경 없이도 어떤 프로젝트의 백포트 요구 사항에도 맞게 도구를 조정할 수 있도록 합니다.
* **배포 가능** — ClickHouse의 CI 인프라에 의존하지 않는 독립형 Python wheel 패키지로 제공되어 PyPI에서 설치할 수 있습니다.
* **프로그래밍 가능** — pull request, 레이블, 릴리스 브랜치에 대한 명확한 객체 모델을 제공하여, 사용자가 핵심 엔진 위에서 사용자 지정 워크플로를 스크립트로 작성할 수 있도록 합니다.

<div id="testing">
  ### 테스트
</div>

독립형 도구에 계획된 구성 요소 중 하나는 전용 테스트 스위트와 경량 테스트 인프라입니다. 이 인프라는 다음 항목이 미리 준비된 임시 GitHub 리포지토리(또는 이에 상응하는 로컬 환경)를 생성할 수 있습니다.

* 릴리스 라인을 나타내는 구성 가능한 브랜치 집합,
* 다양한 조합의 백포트 레이블이 지정된 pull request,
* 릴리스 브랜치를 대상으로 하는 `release` 레이블이 있는 release PR.

이를 통해 테스트는 프로덕션 상태를 변경하지 않고도, 실제처럼 동작하지만 폐기 가능한 리포지토리를 대상으로 레이블 감지, 체리픽 브랜치 생성, 충돌 처리, 백포트 PR 생성, 담당자 할당 로직, rolling-out 상태 건너뛰기, 지연 정책까지 전체 자동화 루프를 검증할 수 있습니다. 동일한 인프라는 정책 변경 사항을 배포하기 전에 회귀 테스트하는 데에도 재사용할 수 있습니다.

<div id="active-release-branches">
  ## 활성 릴리스 브랜치
</div>

활성 릴리스 브랜치는 해당 릴리스 PR(`release` 레이블 포함)이 GitHub에서 아직 열려 있는 모든 브랜치를 의미합니다. 백포트 자동화는 실행할 때마다 이를 동적으로 감지하므로, 새 릴리스가 생성되거나 기존 릴리스가 지원 종료되더라도 구성을 변경할 필요가 없습니다.

새 릴리스가 배포되는 동안 릴리스 브랜치는 **롤아웃** 상태(릴리스 PR에 `롤아웃` 레이블 포함)일 수 있습니다. rollout을 복잡하게 만들지 않기 위해 롤아웃 브랜치에서는 일반 백포트가 일시 중지됩니다. 버전별 레이블(예: `v25.3-must-backport`)은 이 동작을 재정의하여 rollout 중에도 백포트를 강제합니다.

버전별 레이블은 PR이 도달해야 하는 *가장 오래된* 릴리스를 지정합니다. 즉, 지정된 릴리스에만 백포트되는 것이 아니라 해당 릴리스와 **그보다 최신인 모든 활성 릴리스 브랜치**에도 백포트됩니다. 예를 들어, 개발 브랜치에 머지된 PR에 `v25.3-must-backport`가 있으면 `25.3`과 그 이후의 모든 활성 릴리스(`25.4`, `25.5`, …)에 백포트됩니다. 여러 버전별 레이블이 있으면 가장 낮은 버전이 우선하는데, 더 최신 버전까지 이미 포괄하기 때문입니다.

지정된 릴리스 자체가 활성 상태일 필요는 없습니다. 지원 종료된 릴리스(열려 있는 릴리스 PR이 없는 릴리스)에 대한 레이블도 수정 사항을 그 이후의 모든 활성 릴리스에 계속 반영하므로, 해당 릴리스에서 업그레이드할 때 수정 사항이 누락되는 일이 없습니다. 예를 들어, PR에 `v25.12-must-backport`가 있으면 `25.12` 자체가 지원 종료된 후에도 계속 `26.1`, `26.2`, …에 백포트됩니다.

<div id="implementation">
  ## 구현
</div>

<div id="overview">
  ### 개요
</div>

백포트 자동화는 `CherryPick` GitHub Actions 워크플로(`.github/workflows/cherry_pick.yml`)로 매시간 실행되며, `tests/ci/cherry_pick.py`에 구현되어 있습니다. 이 자동화는 GitHub API와 자체 호스팅 `style-checker-aarch64` 러너에서 수행되는 로컬 git 작업을 통해 동작합니다.

이 프로세스는 각 (원본 PR, 릴리스 브랜치) 쌍마다 2단계로 진행됩니다:

1. 실제 머지 대상에서 충돌 해결을 분리하기 위해 **체리픽 PR**이 생성됩니다. 충돌이 없으면 자동으로 머지됩니다.
2. 실제 릴리스 브랜치를 대상으로 **백포트 PR**이 생성되며, 체리픽된 변경 사항은 하나의 커밋으로 스쿼시됩니다.

<div id="labels">
  ### 레이블
</div>

원본 PR의 레이블은 백포트 수행 여부와 백포트 대상 위치를 제어합니다.

| 레이블                                               | 효과                                                                                                                                                                                      |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pr-must-backport`                                | 모든 활성 릴리스 브랜치로 백포트합니다 (`롤아웃`로 표시된 브랜치는 스키핑)                                                                                                                                     |
| `pr-must-backport-force`                          | `롤아웃` 제한을 무시하고 모든 활성 릴리스 브랜치로 백포트합니다                                                                                                                                            |
| `pr-critical-bugfix`                              | `pr-must-backport`를 자동으로 트리거합니다 (`pr_labels_and_category.py`의 `AUTO_BACKPORT`를 통해)                                                                                                      |
| `v{VER}-must-backport` (예: `v25.3-must-backport`) | 해당 릴리스 브랜치와 **그보다 최신인 모든 활성 릴리스 브랜치**로 백포트합니다 — 지정된 릴리스 자체가 지원 종료 상태이더라도, 이 버전은 PR이 도달해야 하는 *가장 오래된* 릴리스를 나타냅니다. 이러한 레이블이 여러 개 있으면 가장 낮은 버전이 우선합니다. 해당 브랜치들에는 `롤아웃` 스키핑을 재정의합니다 |
| `pr-backports-created`                            | 필요한 모든 백포트 PR이 생성되면 봇이 설정하며, 체리픽 PR이 다시 열리면 해제됩니다                                                                                                                                       |
| `pr-cherrypick`                                   | 봇이 생성한 체리픽 PR에 적용됩니다                                                                                                                                                                    |
| `pr-backport`                                     | 봇이 생성한 백포트 PR에 적용됩니다                                                                                                                                                                    |
| `do not test`                                     | CI가 실행되지 않도록 체리픽 PR에 적용됩니다                                                                                                                                                              |
| `롤아웃`                                     | 브랜치가 현재 롤아웃 중임을 나타내기 위해 **릴리스 PR**에 설정되며, 일반 백포트에서는 이를 건너뜁니다                                                                                                                            |

<div id="branch-and-pr-naming">
  ### 브랜치 및 PR 명명 규칙
</div>

각 원본 PR 번호 `N`과 릴리스 브랜치 `release/X.Y`에 대해 다음과 같이 지정합니다.

* 체리픽 브랜치: `cherrypick/release/X.Y/N`
* 백포트 브랜치: `backport/release/X.Y/N`
* 체리픽 PR 제목: `Cherry pick #N to release/X.Y: <original title>`
* 백포트 PR 제목: `Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### 단계별 절차
</div>

<div id="discover-active-releases">
  #### 1. 활성 릴리스 확인
</div>

`BackportPRs.receive_release_prs`는 GitHub에서 `release` 레이블이 붙은 모든 열린 PR을 조회합니다. 이 PR들의 head ref는 릴리스 브랜치 이름(예: `release/25.3`)입니다. 이를 바탕으로 검색할 버전별 레이블 집합을 도출합니다. 즉, 리포지토리에 존재하는 모든 `v{VER}-must-backport` 레이블 가운데 가장 최신 활성 릴리스보다 버전이 새롭지 않은 레이블을 찾습니다. 이전 레이블은 해당 릴리스가 더 이상 활성 상태가 아니더라도 포함됩니다(모든 활성 릴리스보다 새로운 레이블은 어떤 활성 브랜치로도 확장될 수 없으므로 건너뜁니다). 따라서 지원 종료된 릴리스용 레이블이 붙은 PR도, 그보다 최신 릴리스가 활성 상태인 한 계속 검색됩니다.

<div id="find-prs-to-backport">
  #### 2. 백포트할 PR 찾기
</div>

`BackportPRs.receive_prs_for_backport`는 GitHub 검색 API를 사용하여 다음 조건을 만족하는 머지된 PR을 찾습니다.

* 하나 이상의 백포트 레이블(`pr-must-backport`, `pr-must-backport-force`, `pr-critical-bugfix` 또는 버전별 레이블)이 있고,
* 아직 `pr-backports-created` 레이블이 없고,
* 모든 릴리스 브랜치에서 확인된 가장 오래된 커밋 날짜 이후에 머지되었고,
* 최근 90일 이내에 업데이트되었습니다(검색 쿼리의 효율성을 유지하기 위해).

<div id="rolling-out-branch-handling">
  #### 3. 롤링아웃 중인 브랜치 처리
</div>

릴리스 PR에 `rolling-out` 레이블이 있으면 일반 백포트 레이블(`pr-must-backport`, `pr-critical-bugfix`)은 해당 브랜치를 건너뜁니다. 봇은 해당 브랜치에 대해 이전에 생성된 체리픽 또는 백포트 PR을 설명 댓글과 함께 종료합니다. 버전별 레이블(예: `v25.3-must-backport`)은 항상 이 규칙보다 우선합니다. 즉, 지정된 릴리스와 이 레이블이 확장되는 그보다 최신의 모든 활성 릴리스 브랜치에 적용됩니다. `pr-must-backport-force`는 모든 브랜치에서 `rolling-out` 검사를 우회합니다.

<div id="cherry-pick-stage">
  #### 4. 체리픽 단계 (`ReleaseBranch.create_cherrypick`)
</div>

아직 체리픽 PR이 없는 각 (원본 PR, 릴리스 브랜치) 쌍에 대해 다음을 수행합니다.

1. 릴리스 브랜치를 체크아웃하고, 이를 기준으로 **백포트 브랜치**(`backport/release/X.Y/N`)를 생성합니다.
2. 머지 커밋의 첫 번째 부모를 대상으로 `git merge -s ours`를 수행하여, 내용 변경이 없는 합성 머지 베이스를 만듭니다.
3. 원본 PR의 머지 커밋을 직접 가리키도록 **체리픽 브랜치**(`cherrypick/release/X.Y/N`)를 강제 생성합니다.
4. 백포트 브랜치에 체리픽 브랜치를 머지하는 `git merge --no-commit --no-ff`를 시도합니다.
   * 이미 최신 상태라면 해당 변경은 이미 릴리스 브랜치에 포함되어 있는 것이므로, 완료로 표시하고 건너뜁니다.
   * 그렇지 않으면(충돌 여부와 관계없이) 리셋한 뒤 두 브랜치를 모두 푸시합니다.
5. `cherrypick/release/X.Y/N`에서 `backport/release/X.Y/N`으로 보내는 체리픽 PR을 생성하고, `pr-cherrypick` 및 `do not test` 레이블을 지정합니다.
6. 해당하는 경우 원본 PR의 `pr-bugfix` 또는 `pr-critical-bugfix` 레이블도 전달합니다.
7. 이 시점에서는 담당자를 설정하지 않으며, 충돌이 감지된 경우에만 추가합니다.

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. 충돌 없는 체리픽 PR의 자동 머지
</div>

체리픽 PR이 머지 가능한 상태라면(충돌이 없으면), 봇이 GitHub API를 통해 자동으로 머지한 뒤 즉시 백포트 단계로 넘어갑니다.

<div id="backport-stage">
  #### 6. 백포트 단계 (`ReleaseBranch.create_backport`)
</div>

체리픽 PR이 머지된 후:

1. 백포트 브랜치를 체크아웃하고 pull합니다.
2. 릴리스 브랜치와 백포트 브랜치 사이의 merge-base를 찾습니다.
3. merge-base로 `git reset --soft`를 실행해 체리픽한 모든 커밋을 하나로 스쿼시합니다.
4. 백포트 PR 제목을 메시지로 사용해 커밋합니다.
5. 백포트 브랜치를 force-push한 뒤 실제 릴리스 브랜치를 대상으로 백포트 PR을 엽니다.
6. PR에 `pr-backport` 레이블을 추가합니다(해당하는 경우 `pr-bugfix` / `pr-critical-bugfix`도 추가합니다).
7. PR을 원본 PR의 작성자, 머지한 사용자, 기존 담당자에게 할당합니다(로봇 계정 제외).

<div id="completion">
  #### 7. 완료
</div>

특정 원본 PR에 대한 모든 릴리스 브랜치의 백포트가 완료되면, 봇이 원본 PR에 `pr-backports-created`를 추가합니다.

<div id="pre-check">
  #### 8. 사전 확인
</div>

PR 작업을 시작하기 전에 `ReleaseBranch.pre_check`는 `git merge-base --is-ancestor`를 실행하여 해당 머지 커밋이 릴리스 브랜치에서 이미 도달 가능한지 확인합니다. 이미 도달 가능하면 해당 PR은 이미 백포트된 것으로 간주하고 건너뜁니다.

<div id="stale-cherry-pick-pr-handling">
  ### 오래된 체리픽 PR 처리
</div>

`CherryPickPRs` 클래스는 매시간 실행이 시작될 때 다음 두 가지 시나리오를 처리합니다:

* **고아 체리픽 PR**: 체리픽 PR의 릴리스 브랜치에 더 이상 열려 있는 릴리스 PR이 없으면(즉, 릴리스가 종료되면), 해당 체리픽 PR은 자동으로 닫힙니다.
* **다시 열린 체리픽 PR**: 원본 PR에 이미 `pr-backports-created` 레이블이 있지만 해당 체리픽 PR이 아직 열려 있으면, 원본 PR에서 `pr-backports-created` 레이블을 제거하여 다시 처리할 수 있도록 합니다.

수동 충돌 해결을 기다리는 체리픽 PR의 경우:

* **3일** 동안 업데이트가 없으면, bot이 담당자를 멘션하는 Ping 댓글을 게시합니다.
* **7일** 동안 업데이트가 없으면, bot이 종료 안내 댓글을 게시하고 PR을 닫습니다.

<div id="conflict-resolution">
  ### 충돌 해결
</div>

체리픽에 충돌이 발생하면 사람이 해결할 수 있도록 체리픽 PR이 열린 상태로 유지됩니다. 봇은 이를 원본 PR의 작성자, 머지한 사용자, 그리고 담당자에게 할당합니다. 충돌이 해결되고 체리픽 PR이 머지되면 봇은 다음 시간별 실행 시 백포트 PR을 생성합니다.

백포트를 완전히 폐기하려면 체리픽 PR을 닫으세요. 봇은 이를 의도적으로 건너뛴 것으로 처리합니다.

손상된 체리픽 PR을 처음부터 다시 생성하려면:

1. 체리픽 PR에서 `pr-cherrypick` 레이블을 제거합니다.
2. `cherrypick/...` 브랜치를 삭제합니다.
3. 원본 PR에 `pr-backports-created`가 있으면 제거합니다.

<div id="ci-for-backport-prs">
  ### 백포트 PR용 CI
</div>

백포트 PR는 릴리스 브랜치를 대상으로 하므로, 표준 pull request 워크플로가 아니라 전용 CI 워크플로(`BackportPR`, `ci/workflows/backport_branches.py`에 정의)를 사용합니다. 이 워크플로는 CI의 주요 항목만 선별해 실행합니다. 여기에는 ASan/UBSan 및 TSan 빌드, 릴리스 빌드, macOS 빌드, ASan에서 실행되는 기능 테스트, TSan에서 실행되는 스트레스 테스트, 그리고 통합 테스트가 포함됩니다. 또한 백포트 브랜치에 커밋이 1개 이상 50개 이하이고 변경된 파일이 최소 1개 이상 있는지 검증하며, 이는 `check_backport_branch.py`에서 강제됩니다.

<div id="authentication">
  ### 인증
</div>

이 워크플로는 git push 작업에 SSH key(`ROBOT_CLICKHOUSE_SSH_KEY`)를 사용합니다. GitHub API 호출은 `get_best_robot_token`을 통해 인증되며, 이 함수는 SSM(`/github-tokens`)에 저장된 풀에서 남은 QUOTA가 가장 많은 token을 선택합니다. `ROBOT_CLICKHOUSE_COMMIT_TOKEN`은 API 호출용이 아니라 Actions 워크플로의 checkout 단계에서 사용됩니다. 담당자를 지정할 때는 로봇 account(`robot-clickhouse`, `clickhouse-gh`)를 제외합니다.

<div id="github-api-cache">
  ### GitHub API 캐시
</div>

`cache_utils.py`의 `GitHubCache`는 PyGithub 객체 캐시를 S3에 저장해 시간 단위로 실행될 때마다 API 호출 수를 줄입니다. 캐시는 각 실행이 시작될 때 다운로드되고, 종료될 때 업로드됩니다.

<div id="error-handling">
  ### 오류 처리
</div>

개별 PR 처리 중 발생한 오류는 포착되어 로그에 기록되지만 실행은 중단되지 않습니다. 모든 PR 처리가 완료된 후 오류가 하나라도 발생했다면 `BackportException`이 발생합니다. CI에서는 이로 인해 `CIBuddy`를 통해 팀 채팅에 알림이 전송됩니다.