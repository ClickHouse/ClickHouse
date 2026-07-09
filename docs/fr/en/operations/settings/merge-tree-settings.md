---
description: 'Paramètres de MergeTree dans `system.merge_tree_settings`'
slug: /operations/settings/merge-tree-settings
title: 'Paramètres des tables MergeTree'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

La table système `system.merge_tree_settings` affiche les paramètres MergeTree définis globalement.

Les paramètres MergeTree peuvent être définis dans la section `merge_tree` du fichier de configuration du serveur, ou spécifiés individuellement pour chaque table `MergeTree` dans
la clause `SETTINGS` de l’instruction `CREATE TABLE`.

Exemple de personnalisation du paramètre `max_suspicious_broken_parts` :

Configurez la valeur par défaut pour toutes les tables `MergeTree` dans le fichier de configuration du serveur :

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

Pour une table particulière :

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

Modifiez les paramètres d’une table donnée à l’aide de `ALTER TABLE ... MODIFY SETTING` :

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## Paramètres MergeTree
</div>

{/* Les paramètres ci-dessous sont générés automatiquement par le script disponible à l’adresse 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }