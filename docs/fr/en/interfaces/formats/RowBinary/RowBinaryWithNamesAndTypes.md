---
alias: []
description: 'Documentation sur le format RowBinaryWithNamesAndTypes'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'référence'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Semblable au format [RowBinary](./RowBinary.md), mais avec un en-tête supplémentaire :

* nombre de colonnes (N) encodé en [`LEB128`](https://en.wikipedia.org/wiki/LEB128) ;
* N `String` indiquant les noms des colonnes ;
* N `String` indiquant les types des colonnes.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

<RowBinaryFormatSettings />

:::note
Si le paramètre [`input_format_with_names_use_header`](/fr/operations/settings/settings-formats.md/#input_format_with_names_use_header) est défini sur 1,
les colonnes des données d’entrée seront associées aux colonnes de la table en fonction de leur nom, et les colonnes dont le nom est inconnu seront ignorées si le paramètre [input&#95;format&#95;skip&#95;unknown&#95;fields](/fr/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) est défini sur 1.
Sinon, la première ligne sera ignorée.
Si le paramètre [`input_format_with_types_use_header`](/fr/operations/settings/settings-formats.md/#input_format_with_types_use_header) est défini sur `1`,
les types des données d’entrée seront comparés à ceux des colonnes correspondantes de la table. Sinon, la deuxième ligne sera ignorée.
:::