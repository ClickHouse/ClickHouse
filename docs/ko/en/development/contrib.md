---
description: 'ClickHouse에서 타사 라이브러리를 사용하는 방식과 타사 라이브러리를 추가하고 유지 관리하는 방법을 설명하는 페이지입니다.'
sidebar_label: '타사 라이브러리'
sidebar_position: 60
slug: /development/contrib
title: '타사 라이브러리'
doc_type: 'reference'
---

ClickHouse는 다양한 용도로 타사 라이브러리를 사용합니다. 예를 들어 다른 데이터베이스에 연결하거나, 디스크에서 데이터를 읽고 저장할 때 데이터를 디코딩/인코딩하거나, 일부 특수한 SQL 함수를 구현할 때 사용합니다.
대상 시스템에서 사용할 수 있는 라이브러리에 의존하지 않도록 각 타사 라이브러리는 Git submodule로 ClickHouse의 소스 트리에 가져오고, ClickHouse와 함께 컴파일 및 링크됩니다.
타사 라이브러리 목록과 해당 라이선스는 다음 쿼리로 확인할 수 있습니다.

```sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

나열된 라이브러리는 ClickHouse 리포지토리의 `contrib/` 디렉터리에 있는 라이브러리입니다.
build option에 따라 일부 라이브러리는 컴파일되지 않았을 수 있으며, 그 결과 해당 기능을 런타임에서 사용할 수 없을 수 있습니다.

[예시](https://sql.clickhouse.com?query_id=478GCPU7LRTSZJBNY3EJT3)

<div id="adding-and-maintaining-third-party-libraries">
  ## 타사 라이브러리 추가 및 유지 관리
</div>

각 타사 라이브러리는 ClickHouse 리포지토리의 `contrib/` 디렉터리 아래에 있는 전용 디렉터리에 있어야 합니다.
외부 코드 사본을 라이브러리 디렉터리에 그대로 넣는 방식은 피하십시오.
대신 Git submodule을 생성하여 외부 upstream 리포지토리에서 타사 코드를 가져오십시오.

ClickHouse에서 사용하는 모든 submodule은 `.gitmodule` 파일에 나열되어 있습니다.

* 라이브러리를 수정 없이 그대로 사용할 수 있다면(기본적인 경우), upstream 리포지토리를 직접 참조할 수 있습니다.
* 라이브러리에 patch가 필요하다면 [GitHub의 ClickHouse organization](https://github.com/ClickHouse)에 upstream 리포지토리의 fork를 만드십시오.

후자의 경우에는 사용자 지정 patch를 upstream commits와 가능한 한 분리하는 것을 목표로 합니다.
이를 위해 통합하려는 branch 또는 tag를 기준으로 `ClickHouse/` prefix가 붙은 브랜치를 만드십시오. 예를 들어 `ClickHouse/2024_2`(branch `2024_2`용) 또는 `ClickHouse/release/vX.Y.Z`(tag `release/vX.Y.Z`용)와 같습니다.
upstream 개발 branch인 `master`/ `main` / `dev`를 그대로 따라가는 것은 피하십시오(즉, fork 리포지토리에서 `ClickHouse/master` / `ClickHouse/main` / `ClickHouse/dev`와 같은 prefix branch를 만들지 마십시오).
이러한 branch는 계속 변경되므로 적절한 version 관리가 더 어려워집니다.
&quot;Prefix branches&quot;를 사용하면 upstream 리포지토리에서 fork로 pull하더라도 사용자 지정 `ClickHouse/` branches는 영향을 받지 않습니다.
`contrib/`의 submodule은 fork된 타사 리포지토리의 `ClickHouse/` branches만 추적해야 합니다.

Patches는 외부 라이브러리의 `ClickHouse/` branches에만 적용됩니다.

이를 수행하는 방법은 두 가지입니다.

* fork된 리포지토리의 `ClickHouse/`-prefix branch에 새로운 수정 사항(예: 새니타이저 수정)을 만들 수 있습니다. 이 경우 수정 사항을 `ClickHouse/` prefix가 붙은 branch(예: `ClickHouse/fix-sanitizer-disaster`)로 push하십시오. 그런 다음 새 branch에서 사용자 지정 추적 branch로 PR을 생성하십시오. 예: `ClickHouse/2024_2 <-- ClickHouse/fix-sanitizer-disaster`, 그리고 PR을 머지하십시오.
* submodule을 업데이트하면서 이전 patches를 다시 적용해야 할 수도 있습니다. 이 경우 오래된 PR을 다시 만드는 것은 과한 작업입니다. 대신 이전 commits를 새 `ClickHouse/` branch(새 version에 해당)로 체리픽하면 됩니다. 여러 commit으로 이루어진 PR의 commits는 필요에 따라 squash해도 됩니다. 가장 이상적인 경우에는 사용자 지정 patches를 upstream에 다시 기여해 두었으므로 새 version에서는 patches를 생략할 수 있습니다.

submodule 업데이트가 완료되면 ClickHouse에서 submodule이 fork의 새 hash를 가리키도록 갱신하십시오.

타사 라이브러리의 patches는 공식 리포지토리를 염두에 두고 작성하고, 해당 patch를 upstream 리포지토리에 다시 기여하는 것도 고려하십시오.
이렇게 하면 다른 사용자도 해당 patch의 혜택을 받을 수 있고, ClickHouse team의 유지 관리 부담도 줄일 수 있습니다.