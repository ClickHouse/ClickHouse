---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'YTsaurus dictionary source'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'Configure YTsaurus as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

:::info
This is an experimental feature that may change in backwards-incompatible ways in future releases.
Enable usage of the YTsaurus dictionary source
using setting [`allow_experimental_ytsaurus_dictionary_source`](/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(YTSAURUS(
    http_proxy_urls 'http://localhost:8000'
    cypress_path '//tmp/test'
    oauth_token 'password'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <ytsaurus>
        <http_proxy_urls>http://localhost:8000</http_proxy_urls>
        <cypress_path>//tmp/test</cypress_path>
        <oauth_token>password</oauth_token>
        <check_table_schema>1</check_table_schema>
    </ytsaurus>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `http_proxy_urls` | URL to the YTsaurus http proxy. |
| `cypress_path` | Cypress path to the table source. |
| `oauth_token` | OAuth token. |
| `lookup_throttler_max_requests_per_second` | Maximum number of YTsaurus lookup requests per second when fetching keys for selective dictionary loads (`CACHE`, `COMPLEX_KEY_CACHE`, etc.). `0` disables throttling. The default is `200000`. |
| `lookup_max_rows_per_query` | Maximum number of rows per single YTsaurus lookup request. Selective loads are split into chunks of this size. `0` means unlimited (a single chunk per load). The default is `1024`. |
