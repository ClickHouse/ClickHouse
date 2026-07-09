---
description: '使用 ClickHouse 测试和进行硬件性能基准测试的指南'
sidebar_label: '测试硬件'
sidebar_position: 54
slug: /operations/performance-test
title: '如何使用 ClickHouse 测试您的硬件'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

您可以在任何服务器上运行基本的 ClickHouse 性能测试，无需安装 ClickHouse 软件包。

<div id="automated-run">
  ## 自动化运行
</div>

你可以使用单个脚本运行该基准测试。

1. 下载该脚本。

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. 运行该脚本。

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. 复制输出结果并发送至 feedback@clickhouse.com

所有结果均发布于此：https://clickhouse.com/benchmark/hardware/