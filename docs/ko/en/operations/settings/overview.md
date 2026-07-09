---
description: '설정 개요 페이지입니다.'
sidebar_position: 1
slug: /operations/settings/overview
title: '설정 개요'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

:::note
XML 기반 설정 프로필과 [설정 파일](/ko/operations/configuration-files)은 현재 ClickHouse Cloud에서 지원되지
않습니다. ClickHouse Cloud
서비스의 설정을 지정하려면 [SQL 기반 설정 프로필](/ko/operations/access-rights#settings-profiles-management)을 사용해야
합니다.
:::

ClickHouse 설정의 주요 그룹은 다음과 같습니다:

* 전역 서버 설정
* 세션 설정
* 쿼리 설정
* 백그라운드 작업 설정

전역 설정은 더 하위 수준에서 재정의되지 않는 한 기본적으로 적용됩니다. 세션 설정은 프로필, 사용자 구성 및 SET 명령을 통해 지정할 수 있습니다. 쿼리 설정은 SETTINGS 절을 통해 지정할 수 있으며 개별 쿼리에 적용됩니다. 백그라운드 작업 설정은 백그라운드에서 비동기로 실행되는 뮤테이션, 머지 및 기타 작업에 적용됩니다.

<div id="see-non-default-settings">
  ## 기본값이 아닌 설정 확인하기
</div>

기본값에서 변경된 설정을 확인하려면 `system.settings` 테이블을 쿼리하면 됩니다:

```sql
SELECT name, value FROM system.settings WHERE changed
```

설정이 모두 기본값으로 유지된 경우 ClickHouse는
아무것도 반환하지 않습니다.

특정 설정의 값을 확인하려면 쿼리에서 해당 설정의 `name`을
지정하면 됩니다:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

다음과 비슷한 결과가 반환됩니다:

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## 추가 자료
</div>

* [전역 서버 설정](/ko/operations/server-configuration-parameters/settings.md)을 참조하여 전역 서버 수준에서
  ClickHouse 서버를 구성하는 방법을 자세히 알아보십시오.
* [세션 설정](/ko/operations/settings/settings-query-level.md)을 참조하여 세션 수준에서 ClickHouse
  서버를 구성하는 방법을 자세히 알아보십시오.
* [Context 계층 구조](/ko/development/architecture.md#context)를 참조하여 ClickHouse에서 구성이 처리되는 방식에 대해 자세히 알아보십시오.