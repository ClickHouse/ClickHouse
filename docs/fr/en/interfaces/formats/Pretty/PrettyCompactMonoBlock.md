---
alias: []
description: 'Documentation sur le format PrettyCompactMonoBlock'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`PrettyCompact`](./PrettyCompact.md) en ce que jusqu’à `10,000` lignes sont mises en mémoire tampon,
puis affichées sous la forme d’une seule table, et non par [blocs](/fr/development/architecture#block).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres du format
</div>

<PrettyFormatSettings />