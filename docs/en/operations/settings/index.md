---
title: "Settings Overview"
sidebar_position: 1
slug: /en/operations/settings/
---

# Settings Overview

:::note
XML-based Settings Profiles and [configuration files](https://clickhouse.com/docs/en/operations/configuration-files) are currently not supported for ClickHouse Cloud. To specify settings for your ClickHouse Cloud service, you must use [SQL-driven Settings Profiles](https://clickhouse.com/docs/en/operations/access-rights#settings-profiles-management).
:::

There are two main groups of ClickHouse settings:

- Global server settings
- Query-level settings

The main distinction between global server settings and query-level settings is that global server settings must be set in configuration files, while query-level settings can be set in configuration files or with SQL queries.

Read about [global server settings](/docs/en/operations/server-configuration-parameters/settings.md) to learn more about configuring your ClickHouse server at the global server level.

Read about [query-level settings](/docs/en/operations/settings/settings-query-level.md) to learn more about configuring your ClickHouse server at the query level.

## See non-default settings

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

## Settings Categories

Reference documentation is available per setting category at the pages below:

| Settings Category                                                                      | Description                                                                          |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [Global Server Settings](/docs/en/operations/server-configuration-parameters/settings) | Server settings that cannot be changed at the session or query level.                |
| [Format Settings](/docs/en/operations/settings/formats)                                | Settings which control input/output formats.                                         |
| [MergeTree Tables Settings](/docs/en/operations/settings/merge-tree-settings)          | Globally set MergeTree settings belonging to the `system.merge_tree_settings` table. |
| [Core Settings](/docs/en/operations/settings/settings)                                 | Settings available in the `system.settings` table.                                   |
| [User Settings](/docs/en/operations/settings/settings-users)                           | Settings found in the `user` section of the `users.xml` configuration file.          |

## Working with Settings

The following pages cover various aspect of working with settings in ClickHouse:

| Topic                                                                               | Description                                                                                                                                                                 |
|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Composable Protocols](/docs/en/operations/settings/composable-protocols)           | Composable protocols allows more flexible configuration of TCP access to the ClickHouse server. This configuration can co-exist with or replace conventional configuration. |
| [Constraints on Settings](/docs/en/operations/settings/constraints-on-settings)     | Setting constraints on settings which prohibit users from changing some of the settings with the `SET` query.                                                               |
| [Memory overcommit](/docs/en/operations/settings/memory-overcommit)                 | Experimental technique intended to allow to set more flexible memory limits for queries.                                                                                    |
| [Permissions for Queries](/docs/en/operations/settings/permissions-for-queries)     | Permission types for queries in ClickHouse.                                                                                                                                 |
| [Restrictions on Query Complexity](/docs/en/operations/settings/query-complexity)   | Restrictions on query complexity as part of the settings, used to provide safer execution from the user interface.                                                          |
| [Query-level Settings](/docs/en/operations/settings/query-level)                    | Explanation and examples of the multiple ways to set ClickHouse query-level settings.                                                                                       |
| [Settings Profiles](/docs/en/operations/settings/settings-profiles)                 | Collections of settings grouped under the same name and how they are declared.                                                                                              |
| [Named collections](/docs/en/operations/named-collections)                          | Named collections as a way to store collections of key-value pairs to be used to configure integrations with external sources.                                              |
| [Configuration Files](/docs/en/operations/configuration-files)                      | Configuring ClickHouse server with configuration files in XML or YAML syntax.                                                                                               |



