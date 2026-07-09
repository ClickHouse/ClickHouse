---
description: "Permet de lire et d’écrire des données exposées via un serveur Apache Arrow Flight."
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

Permet de lire et d’écrire des données exposées via un serveur [Apache Arrow Flight](/fr/interfaces/arrowflight).

**Syntaxe**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Arguments**

* `host:port` — Adresse du serveur Arrow Flight. Si le port est omis, le port par défaut `8815` est utilisé. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Nom du jeu de données ou du descripteur disponible sur le serveur Arrow Flight. [String](../../sql-reference/data-types/string.md).
* `username` — Nom d’utilisateur pour l’authentification HTTP de base. [String](../../sql-reference/data-types/string.md).
* `password` — Mot de passe pour l’authentification HTTP de base. [String](../../sql-reference/data-types/string.md).

Si `username` et `password` ne sont pas spécifiés, aucune authentification n’est utilisée (cela fonctionne uniquement si le serveur Arrow Flight autorise l’accès non authentifié).

La fonction prend également en charge les [named collections](/fr/operations/named-collections) — consultez le [moteur de table ArrowFlight](/fr/engines/table-engines/integrations/arrowflight#named-collections) pour obtenir la liste des paramètres pris en charge.

**Valeur renvoyée**

Un objet table représentant le jeu de données distant. Le schéma est déduit du serveur Arrow Flight.

**Paramètres**

* `arrow_flight_request_descriptor_type` — Contrôle la manière dont le nom du jeu de données est envoyé au serveur Flight. Valeurs : `path` (par défaut) ou `command`. Consultez le [moteur de table ArrowFlight](/fr/engines/table-engines/integrations/arrowflight#settings) pour plus de détails.

**Exemples**

Lecture à partir d’un serveur Arrow Flight distant :

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Insertion de données sur un serveur Arrow Flight distant :

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Utiliser une collection nommée :

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**Voir aussi**

* [moteur de table ArrowFlight](/fr/engines/table-engines/integrations/arrowflight)
* [Interface Arrow Flight](/fr/interfaces/arrowflight)
* [spécification Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)