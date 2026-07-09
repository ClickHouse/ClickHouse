---
description: "Documentation de l’instruction EXISTS"
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: "Instruction EXISTS"
doc_type: 'reference'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Renvoie une seule colonne de type `UInt8`, qui contient la valeur unique `0` si la table ou la base de données n&#39;existe pas, ou `1` si la table existe dans la base de données spécifiée.