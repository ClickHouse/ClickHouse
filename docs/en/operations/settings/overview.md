---
description: 'Overview page for settings.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'Settings Overview'
---

# Settings Overview

:::note
XML-based Settings Profiles and [configuration files](/operations/configuration-files) are currently not supported for ClickHouse Cloud. To specify settings for your ClickHouse Cloud service, you must use [SQL-driven Settings Profiles](/operations/access-rights#settings-profiles-management).
:::

There are two main groups of ClickHouse settings:

- Global server settings
- Session settings

The main distinction between both is that global server settings apply globally for the ClickHouse server, while session settings apply to user sessions or even individual queries.

Read about [global server settings](/operations/server-configuration-parameters/settings.md) to learn more about configuring your ClickHouse server at the global server level.

Read about [session settings](/operations/settings/settings-query-level.md) to learn more about configuring your ClickHouse server at the session level.

## See non-default settings {#see-non-default-settings}

To view which settings have been changed from their default value:

```sql
SELECT name, value FROM system.settings WHERE changed
```

If you haven't changed any settings from their default value, then ClickHouse will return nothing.

To check the value of a particular setting, specify the `name` of the setting in your query:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

This command should return something like:

```response
┌─name────────┬─value─────┐
│ max_threads │ 'auto(8)' │
└─────────────┴───────────┘

1 row in set. Elapsed: 0.002 sec.
```
