---
alias: []
description: 'Documentation sur le format Pretty'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `Pretty` affiche les données sous forme de tableaux en pseudo-graphisme Unicode,
en utilisant des séquences d’échappement ANSI pour afficher les couleurs dans le terminal.
Une grille complète de la table est dessinée, et chaque row occupe deux lignes dans le terminal.
Chaque block de résultats est affiché sous la forme d’une table distincte.
Cela est nécessaire pour que les blocks puissent être affichés sans buffering des résultats (le buffering serait nécessaire pour précalculer la largeur visible de toutes les values).

[NULL](/fr/sql-reference/syntax.md) est affiché sous la forme `ᴺᵁᴸᴸ`.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple (pour le format [`PrettyCompact`](./PrettyCompact.md)) :

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Les lignes ne sont échappées dans aucun des formats `Pretty`. L&#39;exemple suivant est présenté pour le format [`PrettyCompact`](./PrettyCompact.md) :

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Pour éviter d’afficher trop de données dans le terminal, seules les `10,000` premières lignes sont affichées.
Si le nombre de lignes est supérieur ou égal à `10,000`, le message &quot;Showed first 10 000&quot; est affiché.

:::note
Ce format est uniquement approprié pour afficher un résultat de la requête, mais pas pour l’analyse des données.
:::

Le format Pretty prend en charge l’affichage des valeurs totales (lors de l’utilisation de `WITH TOTALS`) et des valeurs extrêmes (lorsque &#39;extremes&#39; est défini sur 1).
Dans ces cas, les valeurs totales et les valeurs extrêmes sont affichées après les données principales, dans des tables distinctes.
Cela est illustré dans l’exemple suivant, qui utilise le format [`PrettyCompact`](./PrettyCompact.md) :

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />