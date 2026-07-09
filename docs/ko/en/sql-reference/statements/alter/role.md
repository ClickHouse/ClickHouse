---
description: '역할 문서'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: '참고'
---

역할을 변경합니다.

구문:

```sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

`SET variable = value`는 `MODIFY SETTING variable = value`의 별칭입니다: 나머지 설정은 그대로 유지하면서 단일 설정만 수정하며, 전체 설정 목록을 대체하고 상속된 모든(상위) 프로필도 제거하는 `SETTINGS` 단독 절과는 다릅니다.