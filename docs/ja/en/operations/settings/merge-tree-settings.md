---
description: '`system.merge_tree_settings` に含まれる MergeTree 設定'
slug: /operations/settings/merge-tree-settings
title: 'MergeTree テーブルの設定'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

システムテーブル `system.merge_tree_settings` には、グローバルに設定された MergeTree 設定が表示されます。

MergeTree 設定は、サーバー設定ファイルの `merge_tree` セクションで設定するか、各 `MergeTree` テーブルごとに
`CREATE TABLE` ステートメントの `SETTINGS` 句で個別に指定できます。

設定 `max_suspicious_broken_parts` をカスタマイズする例:

サーバー設定ファイルで、すべての `MergeTree` テーブルに対するデフォルト値を設定します:

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

特定のテーブルに設定する場合：

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

特定のテーブルの設定は、`ALTER TABLE ... MODIFY SETTING` を使用して変更できます。

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## MergeTree 設定
</div>

{/* 以下の設定は、次のスクリプトによって自動生成されています。 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }