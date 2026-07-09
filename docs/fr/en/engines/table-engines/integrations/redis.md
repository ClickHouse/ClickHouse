---
description: "Ce moteur permet d'intégrer ClickHouse à Redis."
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Moteur de table Redis'
doc_type: 'guide'
---

Ce moteur permet d&#39;intégrer ClickHouse à [Redis](https://redis.io/). Redis reposant sur un modèle clé-valeur, nous vous recommandons vivement de ne l&#39;interroger qu&#39;au cas par cas, par exemple avec `where k=xx` ou `where k in (xx, xx)`.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**Paramètres du moteur**

* `host:port` — adresse du serveur Redis ; vous pouvez omettre le port, et le port Redis par défaut 6379 sera utilisé.
* `db_index` — index de la base de données Redis compris entre 0 et 15 ; la valeur par défaut est 0.
* `password` — mot de passe de l’utilisateur ; la valeur par défaut est une chaîne vide.
* `pool_size` — taille maximale du pool de connexions Redis ; la valeur par défaut est 16.
* `primary_key_name` - n’importe quel nom de colonne de la liste des colonnes.

:::note Sérialisation
`PRIMARY KEY` ne prend en charge qu’une seule colonne. La clé primaire sera sérialisée au format binaire en tant que clé Redis.
Les colonnes autres que la clé primaire seront sérialisées au format binaire en tant que valeur Redis dans l’ordre correspondant.
:::

Les arguments peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `host` et `port` doivent être spécifiés séparément. Cette approche est recommandée pour un environnement de production. À l’heure actuelle, tous les paramètres transmis à Redis via des collections nommées sont obligatoires.

:::note Filtrage
Les requêtes utilisant `key equals` ou le filtrage `in` seront optimisées en recherches multiclés dans Redis. Si des requêtes sont effectuées sans clé de filtrage, un parcours complet de la table aura lieu, ce qui constitue une opération coûteuse.
:::

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

Créez une table dans ClickHouse à l’aide du moteur `Redis` avec des arguments simples :

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

Ou avec les [collections nommées](/fr/operations/named-collections.md) :

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

Insertion :

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

Mise à jour :

Notez que la clé primaire ne peut pas être modifiée.

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

Supprimer :

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate :

Vide la base de données Redis de manière asynchrone. `Truncate` prend aussi en charge le mode SYNC.

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

Jointure :

Jointure avec d’autres tables.

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## Limitations
</div>

Le moteur Redis prend également en charge les requêtes de balayage, telles que `where k > xx`, mais il présente certaines limites :

1. Dans de très rares cas, une requête de balayage peut produire des clés en double pendant le rehashing. Voir les détails dans [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269).
2. Pendant le balayage, des clés peuvent être créées et supprimées ; le jeu de données obtenu ne peut donc pas correspondre à un instant précis.