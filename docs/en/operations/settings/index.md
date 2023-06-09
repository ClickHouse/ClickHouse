---
sidebar_label: Settings Overview
sidebar_position: 1
slug: /en/operations/settings/
pagination_next: en/operations/settings/settings
---

# Settings Overview

There are multiple ways to define ClickHouse settings. Settings are configured in layers, and each subsequent layer redefines the previous values of a setting.

The order of priority for defining a setting is:

1. Settings in the `users.xml` server configuration file

    - Set in the element `<profiles>`.

2. Session settings

    - Send `SET setting=value` from the ClickHouse console client in interactive mode.
    Similarly, you can use ClickHouse sessions in the HTTP protocol. To do this, you need to specify the `session_id` HTTP parameter.

3. Query settings

    -   When starting the ClickHouse console client in non-interactive mode, set the startup parameter `--setting=value`.
    -   When using the HTTP API, pass CGI parameters (`URL?setting_1=value&setting_2=value...`).
    -   Define settings in the [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query) clause of the SELECT query. The setting value is applied only to that query and is reset to the default or previous value after the query is executed.

View the [Settings](./settings.md) page for a description of the ClickHouse settings.

## Converting a Setting to its Default Value

If you change a setting and would like to revert it back to its default value, set the value to `DEFAULT`. The syntax looks like:

```sql
SET setting_name = DEFAULT
```

For example, the default value of `max_insert_block_size` is 1048449. Suppose you change its value to 100000:

```sql
SET max_insert_block_size=100000;

SELECT value FROM system.settings where name='max_insert_block_size';
```

The response is:

```response
┌─value──┐
│ 100000 │
└────────┘
```

The following command sets its value back to 1048449:

```sql
SET max_insert_block_size=DEFAULT;

SELECT value FROM system.settings where name='max_insert_block_size';
```

The setting is now back to its default:

```response
┌─value───┐
│ 1048449 │
└─────────┘
```


## Custom Settings {#custom_settings}

In addition to the common [settings](../../operations/settings/settings.md), users can define custom settings.

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

**See Also**

-   [Server Configuration Settings](../../operations/server-configuration-parameters/settings.md)
