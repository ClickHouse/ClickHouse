---
description: 'Documentation des instructions ALTER DATABASE ... MODIFY COMMENT
qui permettent d’ajouter, de modifier ou de supprimer des commentaires de base de données.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'Instructions ALTER DATABASE ... MODIFY COMMENT'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Ajoute, modifie ou supprime un commentaire de base de données, qu’il ait déjà été défini
ou non. La modification du commentaire se reflète à la fois dans [`system.databases`](/fr/operations/system-tables/databases.md)
et dans la requête `SHOW CREATE DATABASE`.

<div id="syntax">
  ## Syntaxe
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Exemples
</div>

Pour créer une `DATABASE` avec un commentaire :

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

Pour modifier le commentaire :

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

Pour afficher le commentaire modifié :

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

Pour supprimer le commentaire de la base de données :

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

Pour vérifier que le commentaire a bien été supprimé :

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## Contenu associé
</div>

* clause [`COMMENT`](/fr/sql-reference/statements/create/table#comment-clause)
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)