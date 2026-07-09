---
description: 'Documentation de l’instruction USE'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'Instruction USE'
doc_type: 'reference'
---

```sql
USE [DATABASE] db
```

Permet de définir la base de données courante pour la session.

La base de données courante est utilisée pour rechercher des tables si la base de données n&#39;est pas explicitement indiquée dans la requête par un point placé avant le nom de la table.

Cette requête ne peut pas être exécutée avec le protocole HTTP, car il n&#39;y a pas de notion de session.