---
description: 'Guide to testing and benchmarking hardware performance with ClickHouse'
sidebar_label: 'Testing Hardware'
sidebar_position: 54
slug: /operations/performance-test
title: 'How to Test Your Hardware with ClickHouse'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

You can run a basic ClickHouse performance test on any server without installation of ClickHouse packages.


## Automated Run {#automated-run}

You can run the benchmark with a single script.

1. Download the script.
```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Run the script.
```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copy the output and send it to feedback@clickhouse.com

All the results are published here: https://clickhouse.com/benchmark/hardware/
