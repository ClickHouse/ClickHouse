---
description: "Cette table contient des messages d’avertissement concernant le serveur ClickHouse."
keywords: [ 'table système', 'avertissements' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## Description
</div>

Cette table affiche des avertissements concernant le serveur ClickHouse.
Les avertissements de même type sont regroupés en un seul avertissement.
Par exemple, si le nombre N de bases de données attachées dépasse un seuil configurable T, une seule entrée contenant la valeur actuelle N est affichée au lieu de N entrées distinctes.
Si la valeur actuelle redescend sous le seuil, l’entrée est supprimée de la table.

La table peut être configurée avec les paramètres suivants :

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/fr/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/fr/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## Colonnes
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — Message d’avertissement.
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — La chaîne de format utilisée pour mettre en forme le message.

<div id="example">
  ## Exemple
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```