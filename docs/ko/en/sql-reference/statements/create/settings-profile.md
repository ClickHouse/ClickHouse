---
description: 'SETTINGS PROFILE 참고 문서'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 43
slug: /sql-reference/statements/create/settings-profile
title: 'CREATE SETTINGS PROFILE'
doc_type: '참고'
---

사용자 또는 역할(Role)에 할당할 수 있는 [SETTINGS PROFILE](../../../guides/sre/user-management/index.md#settings-profiles-management)을 생성합니다.

구문:

```sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` 절을 사용하면 클러스터 전체에 SETTINGS PROFILE을 생성할 수 있습니다. 자세한 내용은 [분산 DDL](../../../sql-reference/distributed-ddl.md)을 참조하십시오.

<div id="example">
  ## 예시
</div>

사용자를 생성하세요:

```sql
CREATE USER robin IDENTIFIED BY 'password';
```

`max_memory_usage` 설정의 값과 제약 조건을 지정한 `max_memory_usage_profile` SETTINGS PROFILE을 생성하고, 이를 사용자 `robin`에 할당합니다:

```sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```