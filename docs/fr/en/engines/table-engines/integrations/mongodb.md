---
description: "Le moteur MongoDB est un moteur de table en lecture seule qui permet de lire des données à partir d’une collection [MongoDB](https://www.mongodb.com/) distante."
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'Moteur de table MongoDB'
doc_type: 'reference'
---

Le moteur MongoDB est un moteur de table en lecture seule qui permet de lire des données à partir d’une collection [MongoDB](https://www.mongodb.com/) distante.

Seuls les serveurs MongoDB v3.6+ sont pris en charge.
La [Seed list (`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list) n’est pas encore prise en charge.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
```

**Paramètres du moteur**

| Parameter     | Description                                                                                                                                                                                                                         |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | Adresse du serveur MongoDB.                                                                                                                                                                                                         |
| `database`    | Nom de la database distante.                                                                                                                                                                                                        |
| `collection`  | Nom de la collection distante.                                                                                                                                                                                                      |
| `user`        | Utilisateur MongoDB.                                                                                                                                                                                                                |
| `password`    | Mot de passe de l’utilisateur.                                                                                                                                                                                                      |
| `options`     | Facultatif. [Options](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options) de la connection string MongoDB, sous forme de chaîne au format URL. Par exemple : `'authSource=admin&ssl=true'` |
| `oid_columns` | Liste de colonnes séparées par des virgules qui doivent être traitées comme `oid` dans la clause WHERE. `_id` par défaut.                                                                                                           |

:::tip
Si vous utilisez l’offre cloud MongoDB Atlas, l’URL de connexion peut être récupérée via l’option &#39;Atlas SQL&#39;.
La Seed list (`mongodb**+srv**`) n’est pas encore prise en charge, mais elle sera ajoutée dans de prochaines versions.
:::

Vous pouvez également fournir un URI :

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**Paramètres du moteur**

| Paramètre     | Description                                                                                                  |
| ------------- | ------------------------------------------------------------------------------------------------------------ |
| `uri`         | URI de connexion au serveur MongoDB.                                                                         |
| `collection`  | Nom de la collection distante.                                                                               |
| `oid_columns` | Liste des colonnes, séparées par des virgules, à traiter comme `oid` dans la clause WHERE. `_id` par défaut. |

<div id="types-mappings">
  ## Correspondance des types
</div>

| MongoDB                 | ClickHouse                                                                           |
| ----------------------- | ------------------------------------------------------------------------------------ |
| bool, int32, int64      | *tout type numérique sauf Decimals*, Boolean, String                                 |
| double                  | Float64, String                                                                      |
| date                    | Date, Date32, DateTime, DateTime64, String                                           |
| string                  | String, *tout type numérique (sauf Decimals) s’il est correctement formaté*          |
| document                | String(en JSON)                                                                      |
| array                   | Array, String(en JSON)                                                               |
| oid                     | String                                                                               |
| binary                  | String si dans une colonne, chaîne encodée en base64 si dans un Array ou un document |
| uuid (binary subtype 4) | UUID                                                                                 |
| *tout autre*            | String                                                                               |

Si la clé est introuvable dans le document MongoDB (par exemple, si le nom de la colonne ne correspond pas), la valeur par défaut ou `NULL` (si la colonne est Nullable) sera insérée.

<div id="oid">
  ### OID
</div>

Si vous souhaitez qu’une `String` soit traitée comme un `oid` dans la clause WHERE, il suffit d’indiquer le nom de la colonne dans le dernier argument du moteur de table.
Cela peut être nécessaire lors d’une requête sur un enregistrement via la colonne `_id`, qui a par défaut le type `oid` dans MongoDB.
Si le champ `_id` de la table a un autre type, par exemple `uuid`, vous devez spécifier un `oid_columns` vide ; sinon, la valeur par défaut de ce paramètre, `_id`, sera utilisée.

```javascript
db.sample_oid.insertMany([
    {"another_oid_column": ObjectId()},
]);

db.sample_oid.find();
[
    {
        "_id": {"$oid": "67bf6cc44ebc466d33d42fb2"},
        "another_oid_column": {"$oid": "67bf6cc40000000000ea41b1"}
    }
]
```

Par défaut, seule la colonne `_id` est traitée comme une colonne `oid`.

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

Dans ce cas, la valeur renvoyée sera `0`, car ClickHouse ne sait pas que `another_oid_column` est de type `oid`. Corrigeons cela :

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid', '_id,another_oid_column');

-- or

CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('host', 'db', 'sample_oid', 'user', 'pass', '', '_id,another_oid_column');

SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; -- will output 1 now
```

<div id="supported-clauses">
  ## Clauses prises en charge
</div>

Seules les requêtes comportant des expressions simples sont prises en charge (par exemple, `WHERE field = <constant> ORDER BY field2 LIMIT <constant>`).
Ces expressions sont traduites en langage de requête MongoDB et exécutées côté serveur.
Vous pouvez désactiver toutes ces restrictions à l’aide de [mongodb&#95;throw&#95;on&#95;unsupported&#95;query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query).
Dans ce cas, ClickHouse essaie de convertir la requête dans la mesure du possible, mais cela peut entraîner un parcours complet de la table et un traitement côté ClickHouse.

:::note
Il est toujours préférable de définir explicitement le type d’un littéral, car Mongo exige des filtres strictement typés.
Par exemple, si vous souhaitez filtrer par `Date` :

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

Cela ne fonctionnera pas, car Mongo ne convertira pas une chaîne de caractères en `Date` ; vous devez donc la convertir manuellement :

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

Cela s’applique à `Date`, `Date32`, `DateTime`, `Bool`, `UUID`.

:::

<div id="usage-example">
  ## Exemple d’utilisation
</div>

En supposant que le jeu de données [sample&#95;mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) soit chargé dans MongoDB

Créez une Table dans ClickHouse permettant de lire des données depuis une collection MongoDB :

```sql title="Query"
CREATE TABLE sample_mflix_table
(
    _id String,
    title String,
    plot String,
    genres Array(String),
    directors Array(String),
    writers Array(String),
    released Date,
    imdb String,
    year String
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

```sql title="Query"
SELECT count() FROM sample_mflix_table
```

```text title="Response"
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```sql title="Query"
-- JSONExtractString cannot be pushed down to MongoDB
SET mongodb_throw_on_unsupported_query = 0;

-- Find all 'Back to the Future' sequels with rating > 7.5
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
title:     Back to the Future
plot:      A young man is accidentally sent 30 years into the past in a time-traveling DeLorean invented by his friend, Dr. Emmett Brown, and must make sure his high-school-age parents unite in order to save his own existence.
genres:    ['Adventure','Comedy','Sci-Fi']
directors: ['Robert Zemeckis']
released:  1985-07-03

Row 2:
──────
title:     Back to the Future Part II
plot:      After visiting 2015, Marty McFly must repeat his visit to 1955 to prevent disastrous changes to 1985... without interfering with his first trip.
genres:    ['Action','Adventure','Comedy']
directors: ['Robert Zemeckis']
released:  1989-11-22
```

```sql title="Query"
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) AS rating
FROM sample_mflix_table
WHERE arrayExists(x -> x LIKE 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text title="Response"
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

<div id="troubleshooting">
  ## Dépannage
</div>

Vous pouvez voir la requête MongoDB générée dans les logs de niveau DEBUG.

Les détails d’implémentation sont disponibles dans la documentation de [mongocxx](https://github.com/mongodb/mongo-cxx-driver) et de [mongoc](https://github.com/mongodb/mongo-c-driver).