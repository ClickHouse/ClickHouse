---
alias: []
description: 'Documentation sur le format PrettyNoEscapes'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue de [Pretty](/fr/interfaces/formats/Pretty) par le fait que les [séquences d’échappement ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code) ne sont pas utilisées.
Cela est nécessaire pour afficher ce format dans un navigateur, ainsi que pour utiliser l’utilitaire en ligne de commande &#39;watch&#39;.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple :

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
L’[interface HTTP](/fr/interfaces/http) peut être utilisée pour afficher ce format dans un navigateur.
:::

<div id="format-settings">
  ## Paramètres de format
</div>

<PrettyFormatSettings />