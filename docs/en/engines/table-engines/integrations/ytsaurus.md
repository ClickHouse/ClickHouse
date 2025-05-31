---
description: 'The engine allows to import data from the YTsaurus cluster.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'YTsaurus'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# YTsaurus

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

Experimental table
The engine allows to import data from the YTsaurus cluster.

## Creating a Table {#creating-a-table}

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the YTsaurus table engine
with [allow_experimental_ytsaurus_table_engine](/operations/settings/settings#allow_experimental_ytsaurus_table_engine) setting.
Input the command `set allow_experimental_ytsaurus_table_engine = 1`.
:::


**Engine Parameters**

- `http_proxy_url` — URL to the YTsaurus http proxy.
- `cypress_path` — Cypress path to the data source.
- `oauth_token` — OAuth token.


## Usage Example {#usage-example}

Shows a query creating the YTsaurus table:

```sql
SHOW CREATE TABLE yt_saurus;
```

```text
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')

```

Returns the data from the table:

```sql
SELECT * FROM yt_saurus;
```

```text
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

**See Also**

- [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md) table function
