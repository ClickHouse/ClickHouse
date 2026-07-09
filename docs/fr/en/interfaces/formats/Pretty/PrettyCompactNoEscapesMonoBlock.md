---
alias: []
description: 'Documentation du format PrettyCompactNoEscapesMonoBlock'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
doc_type: 'Référence'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) par le fait que jusqu’à `10,000` lignes sont mises en mémoire tampon,
puis affichées sous la forme d’un seul tableau, et non par [blocs](/fr/development/architecture#block).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />