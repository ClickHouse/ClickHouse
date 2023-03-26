---
sidebar_label: Settings
sidebar_position: 51
slug: /en/operations/settings/
---

# Settings Overview

There are multiple ways to make all the settings described in this section of documentation.

Settings are configured in layers, so each subsequent layer redefines the previous settings.

Ways to configure settings, in order of priority:

-   Settings in the `users.xml` server configuration file.

    Set in the element `<profiles>`.

-   Session settings.

    Send `SET setting=value` from the ClickHouse console client in interactive mode.
    Similarly, you can use ClickHouse sessions in the HTTP protocol. To do this, you need to specify the `session_id` HTTP parameter.

-   Query settings.

    -   When starting the ClickHouse console client in non-interactive mode, set the startup parameter `--setting=value`.
    -   When using the HTTP API, pass CGI parameters (`URL?setting_1=value&setting_2=value...`).
    -   Make settings in the [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select) clause of the SELECT query. The setting value is applied only to that query and is reset to default or previous value after the query is executed.

Settings that can only be made in the server config file are not covered in this section.

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

[Original article](https://clickhouse.com/docs/en/operations/settings/) <!--hide-->
