---
description: '查询级别设置'
sidebar_label: '查询级别会话设置'
slug: /operations/settings/query-level
title: '查询级别会话设置'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

可以通过多种方式在特定设置下运行语句。
设置采用分层配置，后续每一层都会覆盖前一层中该设置的值。

<div id="order-of-priority">
  ## 优先级顺序
</div>

定义设置的优先级顺序如下：

1. 直接将设置应用于用户，或在 SETTINGS PROFILE 中应用

   * SQL (推荐)
   * 将一个或多个 XML 或 YAML 文件添加到 `/etc/clickhouse-server/users.d`

2. 会话设置

   * 在 ClickHouse Cloud SQL 控制台或以交互模式运行的
     `clickhouse client` 中发送 `SET setting=value`。同样，你也可以在 HTTP 协议中使用 ClickHouse
     会话。为此，需要指定
     `session_id` HTTP 参数。

3. 查询设置

   * 以非交互模式启动 `clickhouse client` 时，设置启动
     参数 `--setting=value`。
   * 使用 HTTP API 时，传递 CGI 参数 (`URL?setting_1=value&setting_2=value...`) 。
   * 在 SELECT 查询的
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     子句中定义设置。该设置值仅应用于该查询，
     并会在查询执行后重置为默认值或先前的值。

<div id="converting-a-setting-to-its-default-value">
  ## 将设置恢复为默认值
</div>

如果你更改了某项设置，并希望将其恢复为默认值，请将该值设为 `DEFAULT`。语法如下：

```sql
SET setting_name = DEFAULT
```

例如，`async_insert` 的默认值是 `0`。假设你将其值改为 `1`：

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

返回结果如下：

```response
┌─value──┐
│ 1      │
└────────┘
```

以下命令会将其值重置为 0：

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

该设置现已恢复为默认值：

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## 自定义设置
</div>

除了常见的[设置](/zh/operations/settings/settings.md)外，用户还可以定义自定义设置。
自定义设置允许你传递**会话专用参数**，这些参数可在查询、策略或函数中引用。这在以下情况下非常有用：

* 根据用户身份或 organization 过滤数据
* 根据上下文应用不同的业务逻辑
* 在一个会话中跨查询保留有状态信息

自定义设置名称必须以前缀开头，而此前缀必须是你定义的预定义前缀列表中的一项。
此前缀列表可通过 [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes) 这一服务器设置指定，该设置定义在服务器配置文件中。

在下面的示例中，`SQL_` 被选作自定义前缀：

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
在 ClickHouse Cloud 中，无法指定自定义前缀。
所有自定义用户设置均以 `SQL_` 为前缀。
:::

要定义自定义设置，请使用 `SET` 命令：

```sql
SET SQL_a = 123;
```

要获取自定义设置的当前值，请使用 `getSetting()` 函数：

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## 示例
</div>

这些示例都会将 `async_insert` 设置的值设为 `1`，并展示如何在正在运行的系统中查看这些设置。

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### 使用 SQL 直接为用户应用设置
</div>

这会创建用户 `ingester`，并为其设置 `async_inset = 1`：

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### 查看SETTINGS PROFILE及其分配情况
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### 使用 SQL 创建SETTINGS PROFILE并将其分配给用户
</div>

这会创建 profile `log_ingest`，并将 `async_inset` 设置为 `1`：

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

这会创建用户 `ingester`，并将设置 profile `log_ingest` 分配给该用户：

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### 使用 XML 创建 SETTINGS PROFILE 和用户
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment-1">
  #### 查看 SETTINGS PROFILE 及其分配情况
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### 为会话指定设置
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### 在查询时指定设置
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## 另请参见
</div>

* 请参阅 [设置](/zh/operations/settings/settings.md) 页面，了解 ClickHouse 设置的相关说明。
* [全局 server 设置](/zh/operations/server-configuration-parameters/settings.md)