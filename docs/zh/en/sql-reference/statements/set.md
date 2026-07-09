---
description: 'SET 语句说明'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'SET 语句'
doc_type: 'reference'
---

```sql
SET param = value
```

将 `value` 赋给当前会话中的 `param` [设置](/zh/operations/settings/overview)。你不能通过这种方式更改[服务器设置](../../operations/server-configuration-parameters/settings.md)。

你也可以在一次查询中设置指定设置 profile 中的所有值。

```sql
SET profile = 'profile-name-from-the-settings-file'
```

对于设为 `true` 的布尔设置，可以省略值赋值，使用简写语法。仅指定设置名称时，会自动将其设为 `1` (true) 。

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

设置 session time zone。这是 `SET session_timezone = 'timezone'` 的别名，提供此语法是为了兼容 PostgreSQL 和其他 SQL 数据库。

许多 SQL 客户端、ORM 和 JDBC 驱动在建立连接时会自动执行 `SET TIME ZONE`。该语法使这类工具无需自定义变通方案即可与 ClickHouse 一起使用。

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

时区值必须是 [IANA Time Zone Database](https://www.iana.org/time-zones) 中的有效名称。无效的时区名称会导致报错。

有关 `session_timezone` 设置的更多信息，请参阅 [session&#95;timezone](/zh/operations/settings/settings#session_timezone)。

<div id="setting-query-parameters">
  ## 设置查询参数
</div>

`SET` 语句也可用于定义查询参数，方法是给参数名加上 `param_` 前缀。
查询参数允许您编写带有占位符的通用查询，并在执行时将这些占位符替换为实际值。

```sql
SET param_name = value
```

要在查询中使用查询参数，请使用 `{name: datatype}` 语法引用它：

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

当需要使用不同的值多次执行同一查询时，查询参数尤其有用。

有关查询参数的更多详细信息，包括如何与 `Identifier` 类型一起使用，请参见[定义和使用查询参数](../../sql-reference/syntax.md#defining-and-using-query-parameters)。

更多信息，请参见[Settings](../../operations/settings/settings.md)。