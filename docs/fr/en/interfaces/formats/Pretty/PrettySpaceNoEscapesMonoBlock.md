---
alias: []
description: 'Documentation sur le format PrettySpaceNoEscapesMonoBlock'
input_format: false
keywords: ['PrettySpaceNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapesMonoBlock
title: 'PrettySpaceNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

À la différence du format [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md), jusqu’à `10,000` lignes sont mises en mémoire tampon,
puis affichées sous la forme d’un seul tableau, et non par [blocs](/fr/development/architecture#block).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />