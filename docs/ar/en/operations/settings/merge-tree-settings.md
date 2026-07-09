---
description: 'إعدادات MergeTree في `system.merge_tree_settings`'
slug: /operations/settings/merge-tree-settings
title: 'إعدادات جداول MergeTree'
doc_type: 'مرجع'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

يعرض جدول النظام `system.merge_tree_settings` إعدادات MergeTree المضبوطة على مستوى النظام.

يمكن ضبط إعدادات MergeTree في قسم `merge_tree` من ملف تهيئة الخادم، أو تحديدها لكل جدول `MergeTree` على حدة في
عبارة `SETTINGS` ضمن جملة `CREATE TABLE`.

مثال على تخصيص الإعداد `max_suspicious_broken_parts`:

اضبط القيمة الافتراضية لجميع جداول `MergeTree` في ملف تهيئة الخادم:

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

للتعيين لجدول معيّن:

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

غيّر إعدادات جدول معيّن باستخدام `ALTER TABLE ... MODIFY SETTING`:

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## إعدادات MergeTree
</div>

{/* يتم توليد الإعدادات أدناه تلقائيًا بواسطة البرنامج النصي الموجود على 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }