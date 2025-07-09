---
description: 'Settings at the query-level'
sidebar_label: 'Query-level Session Settings'
slug: /operations/settings/query-level
title: 'Query-level Session Settings'
---

## Overview {#overview}

There are multiple ways to run statements with specific settings.
Settings are configured in layers, and each subsequent layer redefines the previous values of a setting.

## Order of priority {#order-of-priority}

The order of priority for defining a setting is:

1. Applying a setting to a user directly, or within a settings profile

    - SQL (recommended)
    - adding one or more XML or YAML files to `/etc/clickhouse-server/users.d`

2. Session settings

    - Send `SET setting=value` from the ClickHouse Cloud SQL console or
    `clickhouse client` in interactive mode. Similarly, you can use ClickHouse
    sessions in the HTTP protocol. To do this, you need to specify the
    `session_id` HTTP parameter.

3. Query settings

    - When starting `clickhouse client` in non-interactive mode, set the startup
    parameter `--setting=value`.
    - When using the HTTP API, pass CGI parameters (`URL?setting_1=value&setting_2=value...`).
    - Define settings in the
    [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
    clause of the SELECT query. The setting value is applied only to that query
    and is reset to the default or previous value after the query is executed.


## Converting a Setting to its Default Value {#converting-a-setting-to-its-default-value}

If you change a setting and would like to revert it back to its default value, set the value to `DEFAULT`. The syntax looks like:

```sql
SET setting_name = DEFAULT
```

For example, the default value of `async_insert` is `0`. Suppose you change its value to `1`:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

The response is:

```response
┌─value──┐
│ 1      │
└────────┘
```

The following command sets its value back to 0:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

The setting is now back to its default:

```response
┌─value───┐
│ 0       │
└─────────┘
```

## Custom Settings {#custom_settings}

In addition to the common [settings](/operations/settings/settings.md), users can define custom settings.

A custom setting name must begin with one of predefined prefixes. The list of these prefixes must be declared in the [custom_settings_prefixes](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes) parameter in the server configuration file.

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

To define a custom setting use `SET` command:

```sql
SET custom_a = 123;
```

To get the current value of a custom setting use `getSetting()` function:

```sql
SELECT getSetting('custom_a');
```

## Examples {#examples}

These examples all set the value of the `async_insert` setting to `1`, and
show how to examine the settings in a running system.

### Using SQL to apply a setting to a user directly {#using-sql-to-apply-a-setting-to-a-user-directly}

This creates the user `ingester` with the setting `async_inset = 1`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

#### Examine the settings profile and assignment {#examine-the-settings-profile-and-assignment}

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
### Using SQL to create a settings profile and assign to a user {#using-sql-to-create-a-settings-profile-and-assign-to-a-user}

This creates the profile `log_ingest` with the setting `async_inset = 1`:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

This creates the user `ingester` and assigns the user the settings profile `log_ingest`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```


### Using XML to create a settings profile and user {#using-xml-to-create-a-settings-profile-and-user}

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

#### Examine the settings profile and assignment {#examine-the-settings-profile-and-assignment-1}

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

### Assign a setting to a session {#assign-a-setting-to-a-session}

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

### Assign a setting during a query {#assign-a-setting-during-a-query}

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

## See also {#see-also}

- View the [Settings](/operations/settings/settings.md) page for a description of the ClickHouse settings.
- [Global server settings](/operations/server-configuration-parameters/settings.md)
