---
slug: /ja/operations/performance-test
sidebar_position: 54
sidebar_label: ハードウェアのテスト
title: "ClickHouseでハードウェアをテストする方法"
---

import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouseパッケージをインストールせずに、任意のサーバーで基本的なClickHouseのパフォーマンステストを実行できます。


## 自動実行

ベンチマークを単一のスクリプトで実行することができます。

1. スクリプトをダウンロードします。
```
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. スクリプトを実行します。
```
chmod a+x ./hardware.sh
./hardware.sh
```

3. 出力をコピーして、feedback@clickhouse.com に送信してください。

すべての結果はここに公開されています: https://clickhouse.com/benchmark/hardware/
