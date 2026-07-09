---
description: 'ClickHouse를 사용한 하드웨어 성능 테스트 및 벤치마크 가이드'
sidebar_label: '하드웨어 성능 테스트'
sidebar_position: 54
slug: /operations/performance-test
title: 'ClickHouse를 사용해 하드웨어를 테스트하는 방법'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse 패키지를 설치하지 않고도 어떤 서버에서든 간단한 ClickHouse 성능 테스트를 실행할 수 있습니다.

<div id="automated-run">
  ## 자동 실행
</div>

스크립트 하나로 벤치마크를 실행할 수 있습니다.

1. 스크립트를 다운로드합니다.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. 스크립트를 실행하세요.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. 출력 결과를 복사하여 feedback@clickhouse.com으로 보내십시오

모든 결과는 다음 페이지에 게시됩니다: https://clickhouse.com/benchmark/hardware/