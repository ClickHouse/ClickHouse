---
description: 'Documentation sur la manipulation des contraintes'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Manipulation des contraintes'
doc_type: 'reference'
---

Les contraintes peuvent être ajoutées, modifiées ou supprimées à l’aide de la syntaxe suivante :

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

Comme pour la création d&#39;une table, une contrainte peut être déclarée soit en `CHECK` (appliqué lors de `INSERT`), soit en `ASSUME` (considéré comme fiable par l&#39;optimiseur sans être vérifié). Voir [contraintes](../../../sql-reference/statements/create/table.md#constraints) pour la différence entre les deux.

`MODIFY CONSTRAINT` remplace la déclaration d&#39;une contrainte existante, tout en conservant sa position dans la définition de la table. Il peut également modifier le type de contrainte (par exemple, de `CHECK` à `ASSUME`). Cela équivaut à supprimer la contrainte puis à l&#39;ajouter de nouveau avec la nouvelle déclaration. Si la contrainte n&#39;existe pas, la requête génère une erreur, sauf si `IF EXISTS` est spécifié.

Voir aussi [contraintes](../../../sql-reference/statements/create/table.md#constraints).

Les requêtes ajoutent, modifient ou suppriment des métadonnées de contraintes de la table ; elles sont donc traitées immédiatement.

:::tip
La vérification de la contrainte **ne sera pas exécutée** sur les données existantes si elle a été ajoutée ou modifiée.
:::

Toutes les modifications apportées aux tables répliquées sont diffusées vers ZooKeeper et seront également appliquées aux autres répliques.