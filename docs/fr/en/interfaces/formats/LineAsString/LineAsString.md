---
alias: []
description: 'Documentation sur le format LineAsString'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `LineAsString` interprète chaque ligne des données d&#39;entrée comme une seule valeur de chaîne.
Ce format ne peut être analysé que pour une table comportant un seul champ de type [String](/fr/sql-reference/data-types/string.md).
Les colonnes restantes doivent être définies comme [`DEFAULT`](/fr/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/fr/sql-reference/statements/create/view#materialized-view) ou omises.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

<div id="format-settings">
  ## Paramètres du format
</div>
