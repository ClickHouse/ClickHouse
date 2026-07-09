---
alias: []
description: 'Documentation sur le format PrettySpaceNoEscapes'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`PrettySpace`](./PrettySpace.md) en ce que les [séquences d’échappement ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code) ne sont pas utilisées.
Cela est nécessaire pour afficher ce format dans un navigateur, ainsi que pour utiliser l’utilitaire en ligne de commande &#39;watch&#39;.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />