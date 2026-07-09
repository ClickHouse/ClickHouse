---
description: '`user.xml` 설정 파일의 `profiles` 섹션에서 설정 제약 조건을 정의할 수 있으며,
  사용자가 `SET` 쿼리로 일부 설정을 변경하지 못하도록 할 수 있습니다.'
sidebar_label: '설정 제약 조건'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: '설정 제약 조건'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

ClickHouse에서 설정의 &quot;제약 조건&quot;은 설정에 지정할 수 있는 제한 사항과 규칙을 의미합니다. 이러한 제약 조건은 데이터베이스의
안정성, 보안, 예측 가능한 동작을 유지하는 데 사용할 수 있습니다.

<div id="defining-constraints">
  ## 제약 조건 정의
</div>

설정에 대한 제약 조건은 `user.xml`
설정 파일의 `profiles` 섹션에서 정의할 수 있습니다. 이러한 제약 조건은 사용자가
[`SET`](/ko/sql-reference/statements/set) SQL 문으로 일부 설정을 변경하지 못하도록 합니다.

제약 조건은 다음과 같이 정의합니다:

```xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
      <setting_name_6>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <disallowed>value1</disallowed>
        <disallowed>value2</disallowed>
        <disallowed>value3</disallowed>
        <changeable_in_readonly/>
      </setting_name_6>
    </constraints>
  </user_name>
</profiles>
```

사용자가 제약 조건을 위반하는 작업을 시도하면 예외가 발생하고
설정은 변경되지 않은 상태로 유지됩니다.

<div id="types-of-constraints">
  ## 제약 조건의 유형
</div>

ClickHouse는 몇 가지 제약 조건 유형을 지원합니다:

* `min`
* `max`
* `disallowed`
* `readonly` (`const` 별칭 포함)
* `changeable_in_readonly`

`min` 및 `max` 제약 조건은 숫자 설정의 상한과 하한을 지정하며,
함께 조합해 사용할 수 있습니다.

`disallowed` 제약 조건은 특정 설정에 대해 허용하지 않을 특정 값(들)을
지정하는 데 사용할 수 있습니다.

`readonly` 또는 `const` 제약 조건은 사용자가 해당 설정을 전혀 변경할 수
없도록 지정합니다.

`changeable_in_readonly` 제약 조건 유형은 `readonly` 설정이 `1`로 설정된 경우에도
사용자가 `min`/`max` 범위 내에서 설정을 변경할 수 있도록 하며,
그렇지 않으면 `readonly=1` 모드에서는 설정을 변경할 수 없습니다.

:::note
`changeable_in_readonly`는 `settings_constraints_replace_previous`
가 활성화된 경우에만 지원됩니다:

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

:::

<div id="multiple-constraint-profiles">
  ## 여러 제약 조건 프로필
</div>

사용자에게 여러 프로필이 활성화되어 있으면 제약 조건이 머지됩니다.
머지 과정은 `settings_constraints_replace_previous` 설정에 따라 달라집니다:

* **true** (권장): 동일한 설정에 대한 제약 조건은 머지 과정에서 대체되므로,
  마지막 제약 조건이 사용되고 그 이전의 모든 제약 조건은 무시됩니다.
  여기에는 새 제약 조건에서 설정되지 않은 필드도 포함됩니다.
* **false** (기본값): 동일한 설정에 대한 제약 조건은 다음과 같은 방식으로 머지됩니다.
  설정되지 않은 각 제약 조건 유형은 이전 프로필에서 가져오고,
  설정된 각 제약 조건 유형은 새 프로필의 값으로 대체됩니다.

<div id="read-only">
  ## 읽기 전용 모드
</div>

읽기 전용 모드는 `readonly` 설정으로 활성화됩니다. 이 설정은 `readonly` 제약 조건 유형과 혼동하지 마십시오:

* `readonly=0`: 읽기 전용 제한이 없습니다.
* `readonly=1`: 읽기 쿼리만 허용되며 `changeable_in_readonly`가 설정된 경우를 제외하고는 설정을 변경할 수 없습니다.
* `readonly=2`: 읽기 쿼리만 허용되지만 `readonly` 설정 자체를 제외한 설정은 변경할 수 있습니다.

<div id="example-read-only">
  ### 예시
</div>

`users.xml`에 다음 내용이 포함되어 있다고 가정합니다:

```xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

다음 쿼리는 모두 예외를 발생시킵니다:

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
`default` 프로필은 특별하게 처리됩니다. `default` 프로필에 정의된 모든 제약 조건은
기본 제약 조건이 되므로, 각 사용자에 대해 명시적으로 재정의하지 않는 한
모든 사용자에게 적용됩니다.
:::

<div id="constraints-on-merge-tree-settings">
  ## MergeTree 설정의 제약 조건
</div>

[MergeTree 설정](merge-tree-settings.md)에 제약 조건을 지정할 수 있습니다.
이 제약 조건은 MergeTree 엔진을 사용하는 테이블을 생성하거나
스토리지 설정을 변경할 때 적용됩니다.

`<constraints>` 섹션에서 참조할 때는
merge tree setting 이름 앞에 `merge_tree_` 접두사를 붙여야 합니다.

<div id="example-read-only">
  ### 예시
</div>

`storage_policy`를 명시적으로 지정하여 새 테이블을 생성하지 못하도록 할 수 있습니다

```xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```