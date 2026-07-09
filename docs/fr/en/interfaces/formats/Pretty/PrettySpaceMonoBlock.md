---
alias: []
description: 'Documentation sur le format PrettySpaceMonoBlock'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'référence'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

À la différence du format [`PrettySpace`](./PrettySpace.md), jusqu&#39;à `10,000` lignes sont d&#39;abord mises en mémoire tampon,
puis affichées sous la forme d&#39;une seule table, et non par [blocs](/fr/development/architecture#block).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />