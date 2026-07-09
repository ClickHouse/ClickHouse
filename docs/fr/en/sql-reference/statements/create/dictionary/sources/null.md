---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'Source de dictionnaire Null'
sidebar_position: 14
sidebar_label: 'Null'
description: 'Configurer une source de dictionnaire Null (vide) dans ClickHouse pour les tests.'
doc_type: 'reference'
---

Une source spéciale permettant de créer des dictionnaires fictifs (vides).
Les dictionnaires fictifs peuvent être utiles pour les tests ou dans des configurations avec des nœuds de données et de requête séparés, ainsi que des tables distribuées.

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```