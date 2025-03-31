---
description: 'Documentation for Set Role'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'SET ROLE Statement'
---

Activates roles for the current user.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role}

Sets default roles to a user.

Default roles are automatically activated at user login. You can set as default only the previously granted roles. If the role isn't granted to a user, ClickHouse throws an exception.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

## Examples {#examples}

Set multiple default roles to a user:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Set all the granted roles as default to a user:

```sql
SET DEFAULT ROLE ALL TO user
```

Purge default roles from a user:

```sql
SET DEFAULT ROLE NONE TO user
```

Set all the granted roles as default except for specific roles `role1` and `role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```
