---
description: '이 테이블 함수는 YTsaurus 클러스터에서 데이터를 읽을 수 있도록 합니다.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # ytsaurus 테이블 함수
</div>

<ExperimentalBadge />

이 테이블 함수를 사용하면 YTsaurus 클러스터의 데이터를 읽을 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
이 기능은 실험적 기능이며, 향후 릴리스에서 하위 호환되지 않는 방식으로 변경될 수 있습니다.
YTsaurus 테이블 함수 사용을 활성화하려면
[allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/ko/operations/settings/settings#allow_experimental_ytsaurus_table_engine) 설정을 사용하십시오.
`set allow_experimental_ytsaurus_table_function = 1` 명령을 입력하십시오.
:::

<div id="arguments">
  ## 인수
</div>

* `http_proxy_url` — YTsaurus http 프록시의 URL입니다.
* `cypress_path` — 데이터 소스의 Cypress 경로입니다.
* `oauth_token` — OAuth 토큰입니다.
* `format` — 데이터 소스의 [포맷](/ko/interfaces/formats)입니다.

**반환 값**

YTsaurus 클러스터의 지정된 Cypress 경로에서 데이터를 읽기 위한, 지정된 구조의 테이블입니다.

**관련 항목**

* [ytsaurus 엔진](/ko/engines/table-engines/integrations/ytsaurus.md)