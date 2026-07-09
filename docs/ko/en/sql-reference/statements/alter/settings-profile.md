---
description: 'SETTINGS PROFILE 문서'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

설정 프로필을 변경합니다.

구문:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` 절을 사용하면 클러스터 전체에서 설정 프로필을 변경할 수 있습니다. 자세한 내용은 [분산 DDL](../../../sql-reference/distributed-ddl.md)을 참조하십시오.

<div id="replacing-vs-modifying">
  ## 설정 교체와 수정
</div>

`ALTER SETTINGS PROFILE`는 프로필의 설정과 상위(`inherited`) 프로필을 변경하는 두 가지 방식을 지원합니다. 두 방식의 동작이 크게 다르므로, 상황에 맞는 방식을 선택하는 것이 중요합니다.

<div id="replacing-form">
  ### 대체 형식: 단독 `SETTINGS` / `INHERIT`
</div>

단독 `SETTINGS` 절(`ADD`, `MODIFY`, `DROP` 없음)은 프로필의 **전체 설정 목록과 모든 상위 프로필을** 여기에 명시한 내용으로 완전히 대체합니다. 이전에 있던 항목이라도 여기에 명시되지 않으면 경고 없이 삭제됩니다.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
`SETTINGS`의 단독 형식은 전체 재정의이므로, 설정이 들어 있는 기본 프로필에서 &quot;설정 하나만 재정의&quot;하려고 이 형식을 사용하면 해당 프로필의 다른 모든 설정(및 모든 상위 프로필)이 삭제됩니다. 나머지 설정은 그대로 유지한 채 하나의 설정만 변경하려면 아래에 설명된 증분 `MODIFY`/`ADD`/`DROP` 형식을 사용하세요.
:::

이 동작은 [`CREATE SETTINGS PROFILE`](../create/settings-profile.md)의 `SETTINGS`와 동일합니다. 즉, 이 절은 전체 설정 목록을 정의합니다.

<div id="incremental-form">
  ### 증분 형식: `ADD` / `MODIFY` / `DROP`
</div>

`ADD`, `MODIFY`, `DROP` 키워드는 프로필의 나머지 부분은 건드리지 않고 개별 항목만 변경합니다:

* `ADD SETTINGS variable = value [constraints]` — 아직 존재하지 않는 설정을 추가합니다.
* `MODIFY SETTINGS variable = value [constraints]` — 단일 설정 항목을 대체합니다. 항목 전체(값과 제약 조건)가 덮어써지므로, 이를 유지하려면 `MIN`/`MAX`/`READONLY`/등을 다시 지정하십시오.
* `DROP SETTINGS variable [,...]` — 나열된 설정을 삭제합니다.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — 부모(상속된) 프로필을 추가하거나 제거합니다.
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — 모든 설정 또는 모든 부모 프로필을 제거합니다.

이러한 절은 여러 개를 하나의 statement에서 함께 사용할 수 있습니다. 예를 들어 `DROP SETTINGS a ADD SETTINGS b = 1`과 같습니다.

`SET variable = value`는 `MODIFY SETTINGS variable = value`의 별칭입니다. `SET`이 더 자연스럽게 느껴지고, 증분 변경을 의도했을 때 전체를 대체하는 `SETTINGS` 절을 입력하는 실수가 흔하기 때문에 이를 제공합니다.

<div id="examples">
  ## 예시
</div>

설정이 채워진 프로필의 나머지는 유지하면서, 단일 설정만 재정의합니다:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

새로운 제한 설정을 추가하고 다른 설정 하나를 삭제합니다:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

상위 프로필을 증분 방식으로 관리합니다:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

항상 [`SHOW CREATE SETTINGS PROFILE`](../show.md) 명령으로 결과를 확인하십시오:

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## 증분 변경과 전체 대체
</div>

:::warning
단독 `SETTINGS` 절은 새 설정을 적용하기 전에 프로필에서 **기존의 모든 설정과 상속된(상위) 모든 프로필을 제거합니다**.
:::

나머지 설정은 유지하면서 하나의 설정만 변경하려면 `ADD SETTINGS` 또는 `MODIFY SETTINGS`를 사용하십시오(아래 예시 참조).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

`ADD SETTINGS`와 `MODIFY SETTINGS`는 모두 프로필의 다른 설정은 유지하지만, *같은* 설정에 대한 기존 항목은 서로 다르게 처리합니다.

* `ADD SETTINGS variable = value ...`는 먼저 `variable`에 대한 기존 항목을 삭제한 다음 새 항목을 삽입합니다. 따라서 해당 설정의 **값뿐 아니라 모든 제약 조건까지 함께 대체합니다**. `variable`에 대해 이전에 정의된 `MIN`, `MAX`, 또는 쓰기 가능 여부(`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) 중 다시 지정하지 않은 것은 모두 제거됩니다.
* `MODIFY SETTINGS variable = value ...`는 **필드별로 머지합니다**. 즉, 실제로 지정한 필드(값, `MIN`, `MAX`, 또는 쓰기 가능 여부)만 재정의하고, 해당 설정의 나머지 필드는 기존 상태를 유지합니다.

:::tip
간단히 말해, 설정의 한 측면만 조정하려는 경우(예: 기존 `MAX`는 유지하고 값만 변경)에는 `MODIFY SETTINGS`를 사용하고, 설정을 처음부터 다시 정의하려는 경우에는 `ADD SETTINGS`를 사용하십시오.
:::

<div id="examples">
  ## 예시
</div>

아래 예시에서 사용할 프로필을 생성합니다:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

다른 설정은 그대로 유지한 채 설정 하나를 추가하거나 변경합니다:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

`MODIFY`는 각 필드별로 머지되므로 설정 값만 변경하면 기존 제약 조건이 유지됩니다:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

설정을 추가합니다(기존의 다른 설정은 유지됨). 이미 존재하는 설정은 완전히 다시 정의합니다:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

`MODIFY`와 달리 값만 지정해 `ADD`를 다시 실행하면 해당 설정에 이전에 정의된 제약 조건이 삭제됩니다:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

이름으로 지정한 설정을 하나 이상 제거합니다:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

모든 설정을 한꺼번에 제거합니다:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### 상속된 프로필 작업하기
</div>

프로필 자체 설정에는 영향을 주지 않고 상위(상속된) 프로필을 추가하거나 제거합니다:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```