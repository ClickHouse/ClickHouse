---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'YTsaurus 딕셔너리 소스'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'ClickHouse에서 YTsaurus를 딕셔너리 소스로 구성합니다.'
doc_type: '참고'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
이 기능은 실험적 기능이며, 향후 릴리스에서 하위 호환되지 않는 방식으로 변경될 수 있습니다.
setting [`allow_experimental_ytsaurus_dictionary_source`](/ko/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source)을 사용하여
YTsaurus 딕셔너리 소스 사용을 활성화하십시오.
:::

설정 예시:

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

  <TabItem value="xml" label="설정 파일">
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

<br />

설정 필드:

| 설정                | 설명                         |
| ----------------- | -------------------------- |
| `http_proxy_urls` | YTsaurus HTTP 프록시의 URL입니다. |
| `cypress_path`    | 테이블 소스의 Cypress 경로입니다.     |
| `oauth_token`     | OAuth 토큰입니다.               |