---
description: 'Overview page for settings.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'Settings Overview'
doc_type: 'reference'
---

# Settings Overview

## Overview {#overview}

:::note
XML-based Settings Profiles and [configuration files](/operations/configuration-files) are currently not 
supported for ClickHouse Cloud. To specify settings for your ClickHouse Cloud 
service, you must use [SQL-driven Settings Profiles](/operations/access-rights#settings-profiles-management).
:::

There are following main groups of ClickHouse settings:

- Global server settings
- Session settings
- Query settings
- Background operations settings

Global settings apply by default unless overridden at further levels. Session settings can be specified via profiles, user configuration and SET commands. Query settings can be provided via SETTINGS clause and are applied to individual queries. Background operations settings are applied to Mutations, Merges and potentially other operations, executed asynchronously in the background.

## Viewing non-default settings {#see-non-default-settings}

To view which settings have been changed from their default value you can query the
`system.settings` table:

```sql
SELECT name, value FROM system.settings WHERE changed
```

If no settings have been changed from their default value, then ClickHouse will 
return nothing.

To check the value of a particular setting, you can specify the `name` of the 
setting in your query:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

Which will return something like this:

```response
┌─name────────┬─value─────┐
│ max_threads │ 'auto(8)' │
└─────────────┴───────────┘

1 row in set. Elapsed: 0.002 sec.
```

## Further reading {#further-reading}

- See [global server settings](/operations/server-configuration-parameters/settings.md) to learn more about configuring your 
  ClickHouse server at the global server level.
- See [session settings](/operations/settings/settings-query-level.md) to learn more about configuring your ClickHouse 
  server at the session level.
- See [context hierarchy](/development/architecture.md#context) to learn more about configuration processing by Clickhouse.
