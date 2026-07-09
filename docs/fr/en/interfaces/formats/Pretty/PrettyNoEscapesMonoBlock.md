---
alias: []
description: 'Documentation sur le format PrettyNoEscapesMonoBlock'
input_format: false
keywords: ['PrettyNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyNoEscapesMonoBlock
title: 'PrettyNoEscapesMonoBlock'
doc_type: 'référence'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`PrettyNoEscapes`](./PrettyNoEscapes.md) en ce que jusqu’à `10,000` lignes sont mises en mémoire tampon,
puis affichées sous la forme d’une seule table, et non par blocs.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />