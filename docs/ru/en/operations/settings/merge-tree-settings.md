---
description: 'Настройки таблиц семейства MergeTree в `system.merge_tree_settings`'
slug: /operations/settings/merge-tree-settings
title: 'Настройки таблиц семейства MergeTree'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

Системная таблица `system.merge_tree_settings` показывает глобально заданные настройки MergeTree.

Настройки MergeTree можно задать в разделе `merge_tree` файла конфигурации сервера или указать отдельно для каждой таблицы `MergeTree` в
секции `SETTINGS` оператора `CREATE TABLE`.

Пример настройки параметра `max_suspicious_broken_parts`:

Настройте значение по умолчанию для всех таблиц `MergeTree` в файле конфигурации сервера:

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

Для конкретной таблицы:

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

Измените настройки для конкретной таблицы с помощью `ALTER TABLE ... MODIFY SETTING`:

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## Настройки MergeTree
</div>

{/* Приведённые ниже настройки автоматически генерируются скриптом: 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }