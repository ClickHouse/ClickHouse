# Access Control

Syntax:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
    [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
    [DEFAULT ROLE {role[,...] | NONE}]
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [ACCOUNT {LOCK | UNLOCK}]
```

``` sql
ALTER USER [IF EXISTS] name
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
    [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
    [DEFAULT ROLE {role[,...] | NONE | ALL}]
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [UNSET {varname [,...] | ALL}]
    [ACCOUNT {LOCK | UNLOCK}]
```

``` sql
SET DEFAULT ROLE
    {role [,role...] | NONE | ALL}
    TO user [, user ] ...
```

``` sql
SET ROLE {role [,role...] | NONE | ALL | ALL EXCEPT role[,role...] | DEFAULT}
```

``` sql
SHOW CREATE USER name
```

``` sql
DROP USER [IF EXISTS] name [,name,...]
```

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name [,name,...]
```

``` sql
DROP ROLE [IF EXISTS] name [,name,...]
```

```sql
GRANT {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    TO user_or_role [, user_or_role ...]
    [WITH GRANT OPTION]
```

```sql
REVOKE [GRANT OPTION FOR]
    {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    FROM user_or_role [, user_or_role ...]
```

``` sql
SET partial_revokes = 0
```

``` sql
SET partial_revokes = 1
```

``` sql
GRANT
    role [, role ...]
    TO user_or_role [, user_or_role...]
    [WITH ADMIN OPTION]
```

``` sql
REVOKE [ADMIN OPTION FOR]
    role [, role ...]
    FROM user_or_role [, user_or_role...]
```

``` sql
SHOW GRANTS FOR user_or_role
```

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [UNSET {varname [,...] | ALL}]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
SHOW CREATE SETTINGS PROFILE name
```

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
ALTER QUOTA [IF EXIST] name
    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
SHOW CREATE QUOTA name
```

``` sql
DROP QUOTA [IF EXISTS] name [,name...]
```

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] name
    ON {database.table | table}
    USING condition 
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
ALTER [ROW] POLICY [IF EXISTS] name
    ON {database.table | table}
    USING condition 
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

``` sql
SHOW CREATE [ROW] POLICY name
```

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,name...]
```
