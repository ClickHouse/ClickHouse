---
alias: []
description: 'Documentation sur le format JSONAsString'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: 'référence'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Dans ce format, un objet JSON unique est interprété comme une seule valeur.
Si l’entrée contient plusieurs objets JSON (séparés par des virgules), ils sont interprétés comme des lignes distinctes.
Si les données d’entrée sont entourées de `[]`, elles sont interprétées comme un tableau d’objets JSON.

:::note
Ce format ne peut être analysé que pour une table avec un seul champ de type [String](/fr/sql-reference/data-types/string.md).
Les autres colonnes doivent être définies sur [`DEFAULT`](/fr/sql-reference/statements/create/table.md/#default) ou [`MATERIALIZED`](/fr/sql-reference/statements/create/view#materialized-view),
ou être omises.
:::

Une fois l’objet JSON entier sérialisé dans une String, vous pouvez utiliser les [fonctions JSON](/fr/sql-reference/functions/json-functions.md) pour le traiter.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="basic-example">
  ### Exemple simple
</div>

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

<div id="an-array-of-json-objects">
  ### Un tableau d’objets JSON
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

<div id="format-settings">
  ## Paramètres de format
</div>
