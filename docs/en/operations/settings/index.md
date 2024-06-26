---
sidebar_label: Settings Overview
sidebar_position: 1
slug: /en/operations/settings/
pagination_next: en/operations/settings/settings
---

# Settings Overview

:::note
XML-based Settings Profiles and [configuration files](https://clickhouse.com/docs/en/operations/configuration-files) are currently not supported for ClickHouse Cloud. To specify settings for your ClickHouse Cloud service, you must use [SQL-driven Settings Profiles](https://clickhouse.com/docs/en/operations/access-rights#settings-profiles-management).
:::

There are two main groups of ClickHouse settings:

- Global server settings
- Query-level settings

The main distinction between global server settings and query-level settings is that
global server settings must be set in configuration files while query-level settings
can be set in configuration files or with SQL queries.

Read about [global server settings](/docs/en/operations/server-configuration-parameters/settings.md) to learn more about configuring your ClickHouse server at the global server level.

Read about [query-level settings](/docs/en/operations/settings/settings-query-level.md) to learn more about configuring your ClickHouse server at the query-level.

