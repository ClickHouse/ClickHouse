---
alias: []
description: 'Documentation sur le format SQLInsert'
input_format: false
keywords: ['SQLInsert']
output_format: true
slug: /interfaces/formats/SQLInsert
title: 'SQLInsert'
doc_type: 'reference'
---

| Entrée | sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      |       |

<div id="description">
  ## Description
</div>

Génère les données sous forme d’une séquence d’instructions `INSERT INTO table (columns...) VALUES (...), (...) ...;`.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple :

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

Pour lire les données générées par ce format, vous pouvez utiliser le format d’entrée [MySQLDump](../formats/MySQLDump.md).

<div id="format-settings">
  ## Paramètres du format
</div>

| Paramètre                                                                                                                                       | Description                                                  | Par défaut |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ---------- |
| [`output_format_sql_insert_max_batch_size`](../../operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)             | Le nombre maximal de lignes dans une instruction INSERT.     | `65505`    |
| [`output_format_sql_insert_table_name`](../../operations/settings/settings-formats.md/#output_format_sql_insert_table_name)                     | Le nom de la table dans la requête INSERT générée.           | `'table'`  |
| [`output_format_sql_insert_include_column_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) | Inclure les noms de colonnes dans la requête INSERT.         | `true`     |
| [`output_format_sql_insert_use_replace`](../../operations/settings/settings-formats.md/#output_format_sql_insert_use_replace)                   | Utiliser l&#39;instruction REPLACE au lieu d&#39;INSERT.     | `false`    |
| [`output_format_sql_insert_quote_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_quote_names)                   | Entourer les noms de colonnes avec les caractères « &#96; ». | `true`     |