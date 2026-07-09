---
description: 'ロールのドキュメント'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: 'reference'
---

ロールを変更します。

構文:

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

`SET variable = value` は `MODIFY SETTING variable = value` の別名です。これは、他の設定を保持したまま単一の設定だけをその場で変更します。これに対し、単独の `SETTINGS` 句は設定リスト全体を置き換え、すべての継承元 (親) プロファイルも削除します。