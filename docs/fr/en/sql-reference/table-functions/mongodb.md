---
description: 'Permet d’exécuter des requêtes `SELECT` sur des données stockées sur un
  serveur MongoDB distant.'
sidebar_label: 'mongodb'
sidebar_position: 135
slug: /sql-reference/table-functions/mongodb
title: 'mongodb'
doc_type: 'reference'
---

Permet d’exécuter des requêtes `SELECT` sur des données stockées sur un serveur MongoDB distant.

<div id="syntax">
  ## Syntaxe
</div>

```sql
mongodb(host:port, database, collection, user, password, structure[, options[, oid_columns]]);
mongodb(uri, collection, structure[, oid_columns]);
mongodb(named_collection_name[, <arg>=<value>...]);
```

<div id="arguments">
  ## Arguments
</div>

| Argument      | Description                                                                                                          |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | Adresse du serveur MongoDB.                                                                                          |
| `database`    | Nom de la base de données distante.                                                                                  |
| `collection`  | Nom de la collection distante.                                                                                       |
| `user`        | Utilisateur MongoDB.                                                                                                 |
| `password`    | Mot de passe de l’utilisateur.                                                                                       |
| `structure`   | Schéma de la table ClickHouse renvoyée par cette fonction.                                                           |
| `options`     | Options de la chaîne de connexion MongoDB (paramètre facultatif).                                                    |
| `oid_columns` | Liste de colonnes séparées par des virgules devant être traitées comme `oid` dans la clause WHERE. `_id` par défaut. |

:::tip
Si vous utilisez l’offre cloud MongoDB Atlas, veuillez ajouter ces options :

```ini
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

Vous pouvez également vous connecter à l’aide d’un URI :

```sql
mongodb(uri, collection, structure[, oid_columns])
```

| Argument      | Description                                                                                               |
| ------------- | --------------------------------------------------------------------------------------------------------- |
| `uri`         | Chaîne de connexion.                                                                                      |
| `collection`  | Nom de la collection distante.                                                                            |
| `structure`   | Schéma de la table ClickHouse renvoyée par cette fonction.                                                |
| `oid_columns` | Liste de colonnes séparées par des virgules à traiter comme `oid` dans la clause WHERE. `_id` par défaut. |
| :::           |                                                                                                           |

Vous pouvez transmettre les arguments à l’aide d’une collection nommée :

```sql
mongodb(_named_collection_[, host][, port][, database][, collection][, user][, password][, structure][, options][, oid_columns])
-- or
mongodb(_named_collection_[, uri][, structure][, oid_columns])
```

<div id="returned_value">
  ## Valeur de retour
</div>

Un objet de type table avec les mêmes colonnes que la table MongoDB d’origine.

<div id="examples">
  ## Exemples
</div>

Supposons que nous ayons une collection appelée `my_collection`, définie dans une base de données MongoDB nommée `test`, et que nous y insérions quelques documents :

```sql
db.createUser({user:"test_user",pwd:"password",roles:[{role:"readWrite",db:"test"}]})

db.createCollection("my_collection")

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.9", command: "check-cpu-usage -w 75 -c 90" }
)

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.4", command: "system-check"}
)
```

Interrogeons la collection à l’aide de la fonction table `mongodb` :

```sql
SELECT * FROM mongodb(
    '127.0.0.1:27017',
    'test',
    'my_collection',
    'test_user',
    'password',
    'log_type String, host String, command String',
    'connectTimeoutMS=10000'
)
```

ou :

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

ou :

```sql
CREATE NAMED COLLECTION mongo_creds AS
       uri='mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
       collection='default_collection';

SELECT * FROM mongodb(
        mongo_creds,
        collection = 'my_collection',
        structure = 'log_type String, host String, command String'
)
```

<div id="related">
  ## Voir aussi
</div>

* [Le moteur de table `MongoDB`](/fr/engines/table-engines/integrations/mongodb.md)
* [Utiliser MongoDB comme source de dictionnaire](../statements/create/dictionary/sources/mongodb.md)