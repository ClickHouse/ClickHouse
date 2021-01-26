---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: REVOKE
---

# REVOKE {#revoke}

لغو امتیازات از کاربران و یا نقش.

## نحو {#revoke-syntax}

**لغو امتیازات از کاربران**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**نقش لغو از کاربران**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## توصیف {#revoke-description}

برای لغو برخی از امتیاز شما می توانید یک امتیاز از دامنه گسترده تر استفاده کنید و سپس شما قصد لغو. برای مثال اگر یک کاربر `SELECT (x,y)` امتیاز مدیر می تواند انجام دهد `REVOKE SELECT(x,y) ...` یا `REVOKE SELECT * ...` یا حتی `REVOKE ALL PRIVILEGES ...` پرسوجو برای لغو این امتیاز.

### لغو نسبی {#partial-revokes-dscr}

شما می توانید بخشی از یک امتیاز لغو. برای مثال اگر یک کاربر `SELECT *.*` امتیاز شما می توانید از این امتیاز لغو به خواندن داده ها از برخی از جدول و یا یک پایگاه داده.

## مثالها {#revoke-example}

اعطای `john` حساب کاربری با یک امتیاز را انتخاب کنید از تمام پایگاه داده به استثنای `accounts` یک:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

اعطای `mira` حساب کاربری با یک امتیاز را انتخاب کنید از تمام ستون ها از `accounts.staff` جدول به استثنای `wage` یک

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

{## [مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/settings/) ##}
