---
alias: []
description: 'Documentation sur le format RowBinaryWithDefaults'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'référence'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Semblable au format [`RowBinary`](./RowBinary.md), mais avec un octet supplémentaire avant chaque colonne qui indique s&#39;il faut utiliser la valeur par défaut.

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

Exemples :

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* Pour la colonne `x`, il n’y a qu’un seul octet, `01`, qui indique que la valeur par défaut doit être utilisée, et aucune autre donnée n’est fournie après cet octet.
* Pour la colonne `y`, les données commencent par l’octet `00`, ce qui indique que la colonne contient une valeur effective qui doit être lue dans les données suivantes `01000000`.

<div id="format-settings">
  ## Paramètres de format
</div>

<RowBinaryFormatSettings />