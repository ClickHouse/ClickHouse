---
alias: []
description: 'Documentation sur le format PrettyMonoBlock'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`Pretty`](/fr/interfaces/formats/Pretty) en ce que jusqu&#39;à `10,000` lignes sont mises en mémoire tampon,
puis affichées sous la forme d&#39;une seule table, et non par [blocs](/fr/development/architecture#block).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />