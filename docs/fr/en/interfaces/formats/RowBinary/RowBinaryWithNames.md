---
description: 'Documentation sur le format RowBinaryWithNames'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Semblable au format [`RowBinary`](./RowBinary.md), mais avec un en-tête supplémentaire :

* nombre de colonnes (N) encodé en [`LEB128`](https://en.wikipedia.org/wiki/LEB128) ;
* N `String` indiquant les noms des colonnes.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<RowBinaryFormatSettings />

:::note

* Si le paramètre [`input_format_with_names_use_header`](/fr/operations/settings/settings-formats.md/#input_format_with_names_use_header) est défini sur `1`, les colonnes des données d’entrée seront associées aux colonnes de la table par leur nom, et les colonnes dont le nom est inconnu seront ignorées.
* Si le paramètre [`input_format_skip_unknown_fields`](/fr/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) est défini sur `1`.
  Sinon, la première ligne sera ignorée.
  :::