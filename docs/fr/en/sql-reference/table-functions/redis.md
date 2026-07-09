---
description: 'Cette fonction de table permet d’intégrer ClickHouse avec Redis.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

Cette fonction de table permet d’intégrer ClickHouse avec [Redis](https://redis.io/).

<div id="syntax">
  ## Syntaxe
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## Arguments
</div>

| Argument    | Description                                                                                                                                        |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port` | Adresse du serveur Redis ; vous pouvez omettre le port, et le port Redis par défaut, 6379, sera utilisé.                                           |
| `key`       | n’importe quel nom de colonne dans la liste des colonnes.                                                                                          |
| `structure` | Le schéma de la table ClickHouse renvoyée par cette fonction.                                                                                      |
| `db_index`  | index de la base Redis compris entre 0 et 15 ; la valeur par défaut est 0.                                                                         |
| `password`  | Mot de passe de l’utilisateur ; la valeur par défaut est une chaîne vide.                                                                          |
| `pool_size` | Taille maximale du pool de connexions Redis ; la valeur par défaut est 16.                                                                         |
| `primary`   | doit être spécifié ; une seule colonne dans la clé primaire est prise en charge. La clé primaire sera sérialisée en binaire en tant que clé Redis. |

* les colonnes autres que la clé primaire seront sérialisées en binaire en tant que valeur Redis, dans l’ordre correspondant.
* les requêtes avec filtrage sur la clé à l’aide de `=` ou de `IN` seront optimisées en recherche multi-clés dans Redis. Si les requêtes ne comportent pas de filtrage sur la clé, un parcours complet de la table sera effectué, ce qui constitue une opération lourde.

Les [collections nommées](/fr/operations/named-collections.md) ne sont pas prises en charge pour la fonction de table `redis` pour le moment.

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet de table dont la clé correspond à la clé Redis et dont les autres colonnes sont regroupées dans la valeur Redis.

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Lecture depuis Redis :

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Insertion dans Redis :

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## Voir aussi
</div>

* [Le moteur de table `Redis`](/fr/engines/table-engines/integrations/redis.md)
* [Utiliser Redis comme source de dictionnaire](/fr/sql-reference/statements/create/dictionary/sources/redis)