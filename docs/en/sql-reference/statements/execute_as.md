---
description: 'Documentation for EXECUTE AS Statement'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'EXECUTE AS Statement'
doc_type: 'reference'
---

# EXECUTE AS Statement

Allows to execute queries on behalf of a different user.

## Syntax {#syntax}

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

The first form (without `subquery`) sets that all the following queries in the current session will be executed on behalf of the specified `target_user`.

The second form (with `subquery`) executes only the specified `subquery` on behalf of the specified `target_user`.

In order to work both forms require server setting [allow_impersonate_user](/operations/server-configuration-parameters/settings#allow_impersonate_user)
to be set to `1` and the `IMPERSONATE` privilege to be granted. For example, the following commands
```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```
allow user `user2` to execute commands `EXECUTE AS user1 ...` and also allow user `user3` to execute commands as any user.

While impersonating another user function [currentUser()](/sql-reference/functions/other-functions#currentUser) returns the name of that other user,
and function [authenticatedUser()](/sql-reference/functions/other-functions#authenticatedUser) returns the name of the user who has been actually authenticated.

## Examples {#examples}

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```
