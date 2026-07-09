---
description: 'Documentation du type de données UUID dans ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

Un identifiant universel unique (UUID) est une valeur de 16 octets utilisée pour identifier des enregistrements. Pour plus d&#39;informations sur les UUID, consultez [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

Bien qu&#39;il existe différentes variantes d&#39;UUID, par exemple UUIDv4 et UUIDv7 (voir [ici](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse ne vérifie pas que les UUID insérés sont conformes à une variante particulière.
En interne, les UUID sont traités comme une séquence de 16 octets aléatoires, avec une [représentation 8-4-4-4-12](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) au niveau SQL.

Exemple de valeur UUID :

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

L’UUID par défaut est composé uniquement de zéros. Il est utilisé, par exemple, lorsqu’un nouvel enregistrement est inséré sans qu’aucune valeur soit spécifiée pour une colonne UUID :

```text
00000000-0000-0000-0000-000000000000
```

:::warning
Pour des raisons historiques, les UUIDs sont triés d’après leur seconde moitié.

Bien que cela convienne aux valeurs UUIDv4, cela peut dégrader les performances lorsque des colonnes UUIDv7 sont utilisées dans des définitions d’index primaire (leur utilisation dans des clés de tri ou de partition ne pose pas de problème).
Plus précisément, les valeurs UUIDv7 se composent d’un horodatage dans la première moitié et d’un compteur dans la seconde moitié.
Le tri des UUIDv7 dans les index primaires clairsemés (c.-à-d. les premières valeurs de chaque granule d’index) se fera donc sur le champ compteur.
Si les UUIDs étaient triés selon leur première moitié (l’horodatage), l’étape d’analyse de l’index primaire au début des requêtes devrait écarter tous les marks de toutes les parts sauf une.
Cependant, avec un tri selon la seconde moitié (le compteur), au moins un mark devrait être renvoyé pour chaque part, ce qui entraîne des accès disque inutiles.
:::

Exemple :

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

Pour contourner ce problème, l’UUID peut être converti en horodatage extrait de la seconde moitié :

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

Résultat (en supposant que les mêmes données ont été insérées) :

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## Génération d’UUID
</div>

ClickHouse fournit la fonction [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) pour générer des UUIDv4 aléatoires.

<div id="usage-example">
  ## Exemple d’utilisation
</div>

**Exemple 1**

Cet exemple illustre la création d’une table avec une colonne UUID ainsi que l’insertion d’une valeur dans cette table.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Exemple 2**

Dans cet exemple, aucune valeur n’est spécifiée pour la colonne UUID lors de l’insertion de l’enregistrement ; autrement dit, la valeur UUID par défaut est insérée :

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## Restrictions
</div>

Le type de données UUID prend uniquement en charge les fonctions également prises en charge par le type de données [String](../../sql-reference/data-types/string.md) (par exemple, [min](/fr/sql-reference/aggregate-functions/reference/min), [max](/fr/sql-reference/aggregate-functions/reference/max) et [count](/fr/sql-reference/aggregate-functions/reference/count)).

Le type de données UUID ne prend pas en charge les opérations arithmétiques (par exemple, [abs](/fr/sql-reference/functions/arithmetic-functions#abs)) ni les fonctions d&#39;agrégation, telles que [sum](/fr/sql-reference/aggregate-functions/reference/sum) et [avg](/fr/sql-reference/aggregate-functions/reference/avg)).