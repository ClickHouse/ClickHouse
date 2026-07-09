---
alias: []
description: 'Documentation du format TemplateIgnoreSpaces'
input_format: true
keywords: ['TemplateIgnoreSpaces']
output_format: false
slug: /interfaces/formats/TemplateIgnoreSpaces
title: 'TemplateIgnoreSpaces'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Semblable à [`Template`], mais ignore les caractères d’espacement entre les délimiteurs et les valeurs dans le flux d’entrée.
Cependant, si les chaînes de format contiennent des caractères d’espacement, ces caractères devront être présents dans le flux d’entrée.
Il permet également de spécifier des espaces réservés vides (`${}` ou `${:None}`) pour scinder certains délimiteurs en plusieurs parties afin d’ignorer les espaces entre elles.
Ces espaces réservés servent uniquement à ignorer les caractères d’espacement.
Il est possible de lire du `JSON` avec ce format si les valeurs des colonnes sont dans le même ordre sur toutes les lignes.

:::note
Ce format convient uniquement en entrée.
:::

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

La requête suivante peut être utilisée pour insérer des données à partir de l&#39;exemple de sortie du format [JSON](/fr/interfaces/formats/JSON) :

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

<div id="format-settings">
  ## Paramètres de format
</div>
