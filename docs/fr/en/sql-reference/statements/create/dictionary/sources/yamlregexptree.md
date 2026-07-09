---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'Source de dictionnaire YAMLRegExpTree'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'Configurer un fichier YAML comme source pour les dictionnaires en arbre d’expressions régulières.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

La source `YAMLRegExpTree` charge un arbre d’expressions régulières à partir d’un fichier YAML situé sur le système de fichiers local.
Elle est conçue exclusivement pour être utilisée avec le [layout de dictionnaire `regexp_tree`](../layouts/regexp-tree.md)
et fournit des correspondances hiérarchiques entre regex et attributs pour des opérations de lookup basées sur des motifs, comme l’analyse du user agent.

:::note
La source `YAMLRegExpTree` est disponible uniquement dans ClickHouse Open Source.
Avec ClickHouse Cloud, exportez le dictionnaire au format CSV et chargez-le plutôt via une [source de table ClickHouse](./clickhouse.md).
Consultez [Using regexp&#95;tree dictionaries in ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) pour plus de détails.
:::

<div id="configuration">
  ## Configuration
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

Champs de paramétrage :

| Paramètre | Description                                                                                                                                                                |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PATH`    | Le chemin absolu vers le fichier YAML contenant l’arbre d’expressions régulières. Lors de sa création via DDL, le fichier doit se trouver dans le répertoire `user_files`. |

<div id="yaml-file-structure">
  ## Structure du fichier YAML
</div>

Le fichier YAML contient une liste de nœuds d’un arbre d’expressions régulières. Chaque nœud peut avoir des attributs et des nœuds enfants, formant une hiérarchie :

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Chaque nœud a la structure suivante :

* **`regexp`** : l’expression régulière de ce nœud.
* **attributes** : attributs de dictionnaire définis par l’utilisateur (par ex. `name`, `version`). Les valeurs d’attribut peuvent contenir des **références arrière** à des groupes de capture dans l’expression régulière, écrites sous la forme `\1` ou `$1` (nombres de 1 à 9). Celles-ci sont remplacées par le groupe de capture correspondant à l’exécution de la requête.
* **child nodes** : une liste d’enfants, chacun avec ses propres attributs et, éventuellement, d’autres enfants. Le nom de la liste d’enfants est arbitraire (par ex. `versions` ci-dessus). La correspondance des chaînes s’effectue en parcours en profondeur : si une chaîne correspond à un nœud, ses enfants sont également vérifiés. Les attributs du nœud correspondant le plus profond prévalent sur ceux du parent portant le même nom.

<div id="related-pages">
  ## Pages associées
</div>

* [layout de dictionnaire regexp&#95;tree](../layouts/regexp-tree.md) — configuration du layout, exemples de requêtes et modes de correspondance
* [dictGet](/fr/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/fr/sql-reference/functions/ext-dict-functions#dictGetAll) — fonctions pour interroger les dictionnaires regexp tree