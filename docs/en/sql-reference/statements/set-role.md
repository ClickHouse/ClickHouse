---
toc_priority: 51
toc_title: SET ROLE
---

# SET ROLE Statement {#set-role-statement}

Activates roles for the current user.

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Sets default roles to a user.

Default roles are automatically activated at user login. You can set as default only the previously granted roles. If the role isn’t granted to a user, ClickHouse throws an exception.

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

## Examples {#set-default-role-examples}

Set multiple default roles to a user:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Set all the granted roles as default to a user:

``` sql
SET DEFAULT ROLE ALL TO user
```

Purge default roles from a user:

``` sql
SET DEFAULT ROLE NONE TO user
```

Set all the granted roles as default excepting some of them:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```
