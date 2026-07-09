---
description: 'ClickHouse を使用してハードウェア性能のテストとベンチマークを行うためのガイド'
sidebar_label: 'ハードウェアのテスト'
sidebar_position: 54
slug: /operations/performance-test
title: 'ClickHouse を使ってハードウェアをテストする方法'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouseパッケージをインストールしなくても、任意のサーバーでClickHouseの基本的なパフォーマンステストを実行できます。

<div id="automated-run">
  ## 自動実行
</div>

1 つのスクリプトでベンチマークを実行できます。

1. スクリプトをダウンロードします。

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. スクリプトを実行します。

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. 出力結果をコピーして、feedback@clickhouse.com に送信してください

すべての結果は以下で公開されています: https://clickhouse.com/benchmark/hardware/