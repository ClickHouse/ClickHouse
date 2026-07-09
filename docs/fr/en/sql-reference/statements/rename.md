---
description: 'Documentation de l’instruction RENAME'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'Instruction RENAME'
doc_type: 'reference'
---

Renomme des bases de données, des tables ou des dictionnaires. Plusieurs entités peuvent être renommées dans une même requête.
Notez que la requête `RENAME` portant sur plusieurs entités est une opération non atomique. Pour échanger atomiquement les noms des entités, utilisez l’instruction [EXCHANGE](./exchange.md).

**Syntaxe**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

Renomme des bases de données.

**Syntaxe**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

Renomme une ou plusieurs tables.

Le renommage de tables est une opération légère. Si vous indiquez une autre base de données après `TO`, la table sera déplacée vers cette base de données. Toutefois, les répertoires des bases de données doivent se trouver sur le même système de fichiers. Sinon, une erreur est renvoyée.
Si vous renommez plusieurs tables dans une seule requête, l&#39;opération n&#39;est pas atomique. Elle peut être partiellement exécutée, et des requêtes dans d&#39;autres sessions peuvent renvoyer l&#39;erreur `Table ... does not exist ...`.

**Syntaxe**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Exemple**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

Et vous pouvez utiliser une requête SQL plus simple :

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

Renomme un ou plusieurs dictionnaires. Cette requête peut être utilisée pour déplacer des dictionnaires d&#39;une base de données à une autre.

**Syntaxe**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**Voir aussi**

* [Dictionnaires](./create/dictionary/overview.md)