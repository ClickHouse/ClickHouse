---
alias: []
description: 'Documentation du format Vertical'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Affiche chaque valeur sur une ligne distincte, avec le nom de la colonne correspondant. Ce format est pratique pour afficher une seule ligne ou quelques lignes lorsque chacune contient un grand nombre de colonnes.

Notez que [`NULL`](/fr/sql-reference/syntax.md) est affiché sous la forme `ᴺᵁᴸᴸ` afin de faciliter la distinction entre la valeur de chaîne `NULL` et l&#39;absence de valeur. Les colonnes JSON seront affichées de manière lisible, et `NULL` sera affiché sous la forme `null`, car il s&#39;agit d&#39;une valeur JSON valide et qu&#39;elle se distingue facilement de `"null"`.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple :

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Les lignes ne sont pas échappées dans le format Vertical :

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Ce format convient uniquement pour produire le résultat de la requête, mais pas pour l’analyse (récupération de données à insérer dans une table).

<div id="format-settings">
  ## Paramètres de format
</div>
