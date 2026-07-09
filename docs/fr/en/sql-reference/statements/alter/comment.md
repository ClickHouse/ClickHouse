---
description: 'Documentation sur ALTER TABLE ... MODIFY COMMENT, qui permet
d’ajouter, de modifier ou de supprimer un commentaire de table'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Ajoute, modifie ou supprime un commentaire de table, qu’il ait été défini
auparavant ou non. La modification du commentaire est répercutée à la fois dans [`system.tables`](../../../operations/system-tables/tables.md)
et dans la requête `SHOW CREATE TABLE`.

<div id="syntax">
  ## Syntaxe
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Exemples
</div>

Pour créer une table avec un commentaire :

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Pour modifier le commentaire de la table :

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

Pour afficher le commentaire modifié :

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

Pour supprimer le commentaire de la table :

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

Pour vérifier que le commentaire a bien été supprimé :

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="caveats">
  ## Points à connaître
</div>

Pour les tables Replicated, le commentaire peut différer d’une réplique à l’autre.
La modification du commentaire ne s’applique qu’à une seule réplique.

Cette fonctionnalité est disponible à partir de la version 23.9. Elle ne fonctionne pas dans les versions antérieures de
ClickHouse.

<div id="related-content">
  ## Contenu connexe
</div>

* clause [`COMMENT`](/fr/sql-reference/statements/create/table#comment-clause)
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)