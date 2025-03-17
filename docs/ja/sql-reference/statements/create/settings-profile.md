---
slug: /ja/sql-reference/statements/create/settings-profile
sidebar_position: 43
sidebar_label: SETTINGS PROFILE
title: "CREATE SETTINGS PROFILE"
---

ユーザーまたはロールに割り当てることができる[設定プロファイル](../../../guides/sre/user-management/index.md#settings-profiles-management)を作成します。

構文:

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER`句を使用すると、クラスター上で設定プロファイルを作成できます。[分散DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

## 例

ユーザーを作成します:
```sql
CREATE USER robin IDENTIFIED BY 'password';
```

`max_memory_usage_profile`という設定プロファイルを`max_memory_usage`設定の値と制約を含めて作成し、ユーザー`robin`に割り当てます:

``` sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```
