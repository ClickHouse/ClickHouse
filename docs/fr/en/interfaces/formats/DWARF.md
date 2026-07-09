---
alias: []
description: 'Documentation du format DWARF'
input_format: true
keywords: ['DWARF']
output_format: false
slug: /interfaces/formats/DWARF
title: 'DWARF'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Le format `DWARF` analyse les symboles de débogage DWARF à partir d’un fichier ELF (exécutable, bibliothèque ou fichier objet).
Il est similaire à `dwarfdump`, mais beaucoup plus rapide (des centaines de Mo/s) et prend en charge SQL.
Il produit une ligne pour chaque entrée d’information de débogage (DIE) dans la section `.debug_info`
et inclut des entrées « nulles » que l’encodage DWARF utilise pour terminer les listes d’enfants dans l’arborescence.

:::info
`.debug_info` se compose d’*unités*, qui correspondent aux unités de compilation :

* Chaque unité est une arborescence de *DIE*, avec un DIE `compile_unit` comme racine.
* Chaque DIE possède un *tag* et une liste d’*attributs*.
* Chaque attribut possède un *nom* et une *valeur* (ainsi qu’une *forme*, qui indique comment la valeur est encodée).

Les DIE représentent des éléments du code source, et leur *tag* indique de quel type d’élément il s’agit. Par exemple, il existe :

* des fonctions (tag = `subprogram`)
* des classes/structs/enums (`class_type`/`structure_type`/`enumeration_type`)
* des variables (`variable`)
* des arguments de fonction (`formal_parameter`).

La structure arborescente reflète le code source correspondant. Par exemple, un DIE `class_type` peut contenir des DIE `subprogram` représentant les méthodes de la classe.
:::

Le format `DWARF` produit les colonnes suivantes :

* `offset` - position du DIE dans la section `.debug_info`
* `size` - nombre d’octets du DIE encodé (attributs inclus)
* `tag` - type du DIE ; le préfixe conventionnel « DW&#95;TAG&#95; » est omis
* `unit_name` - nom de l’unité de compilation contenant ce DIE
* `unit_offset` - position de l’unité de compilation contenant ce DIE dans la section `.debug_info`
* `ancestor_tags` - tableau des tags des ancêtres du DIE actuel dans l’arborescence, dans l’ordre du plus interne au plus externe
* `ancestor_offsets` - décalages des ancêtres, parallèles à `ancestor_tags`
* quelques attributs courants dupliqués à partir du tableau d’attributs, par commodité :
  * `name`
  * `linkage_name` - nom pleinement qualifié manglé ; en général, seules les fonctions l’ont (mais pas toutes)
  * `decl_file` - nom du fichier de code source dans lequel cette entité a été déclarée
  * `decl_line` - numéro de ligne dans le code source où cette entité a été déclarée
* tableaux parallèles décrivant les attributs :
  * `attr_name` - nom de l’attribut ; le préfixe conventionnel « DW&#95;AT&#95; » est omis
  * `attr_form` - manière dont l’attribut est encodé et interprété ; le préfixe conventionnel DW&#95;FORM&#95; est omis
  * `attr_int` - valeur entière de l’attribut ; 0 si l’attribut n’a pas de valeur numérique
  * `attr_str` - valeur chaîne de l’attribut ; vide si l’attribut n’a pas de valeur de chaîne

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Le format `DWARF` peut être utilisé pour trouver les unités de compilation présentant le plus grand nombre de définitions de fonctions (y compris les instanciations de Template et les fonctions provenant des fichiers d’en-tête inclus) :

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```

```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

<div id="format-settings">
  ## Paramètres de format
</div>
