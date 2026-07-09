---
description: 'Configuración de MergeTree disponible en `system.merge_tree_settings`'
slug: /operations/settings/merge-tree-settings
title: 'Configuración de las tablas MergeTree'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

La tabla del sistema `system.merge_tree_settings` muestra la configuración global de MergeTree.

La configuración de MergeTree puede establecerse en la sección `merge_tree` del archivo de configuración del servidor, o especificarse individualmente para cada tabla `MergeTree` en
la cláusula `SETTINGS` de la sentencia `CREATE TABLE`.

Ejemplo de cómo personalizar la configuración `max_suspicious_broken_parts`:

Configure el valor predeterminado para todas las tablas `MergeTree` en el archivo de configuración del servidor:

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

Para una tabla concreta:

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

Cambie la configuración de una tabla concreta mediante `ALTER TABLE ... MODIFY SETTING`:

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## Configuración de MergeTree
</div>

{/* Los ajustes que aparecen a continuación se generan automáticamente mediante el script en 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }