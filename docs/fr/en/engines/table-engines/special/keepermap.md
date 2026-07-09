---
description: 'Ce moteur vous permet d''utiliser un cluster Keeper/ZooKeeper comme
  stockage clé-valeur cohérent, avec des écritures linéarisables et des lectures à cohérence séquentielle.'
sidebar_label: 'KeeperMap'
sidebar_position: 150
slug: /engines/table-engines/special/keeper-map
title: 'Moteur de table KeeperMap'
doc_type: 'reference'
---

Ce moteur vous permet d’utiliser un cluster Keeper/ZooKeeper comme stockage clé-valeur cohérent, avec des écritures linéarisables et des lectures à cohérence séquentielle.

Pour activer le moteur de stockage KeeperMap, vous devez définir un chemin ZooKeeper où les tables seront stockées à l’aide de la config `<keeper_map_path_prefix>`.

Par exemple :

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

où path peut être tout autre chemin ZooKeeper valide.

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

Paramètres du moteur :

* `root_path` - chemin ZooKeeper où `table_name` sera stocké.
  Ce chemin ne doit pas contenir le préfixe défini dans la configuration `<keeper_map_path_prefix>`, car celui-ci sera automatiquement ajouté à `root_path`.
  De plus, le format `auxiliary_zookeeper_cluster_name:/some/path` est également pris en charge, où `auxiliary_zookeeper_cluster` désigne un cluster ZooKeeper défini dans la configuration `<auxiliary_zookeepers>`.
  Par défaut, le cluster ZooKeeper défini dans la configuration `<zookeeper>` est utilisé.
* `keys_limit` - nombre de clés autorisées dans la table.
  Cette limite est une limite souple, et il est possible que, dans certains cas particuliers, davantage de clés se retrouvent dans la table.
* `primary_key_name` – n&#39;importe quel nom de colonne dans la liste des colonnes.
* la clé primaire doit être spécifiée ; une seule colonne est prise en charge dans la clé primaire. La clé primaire sera sérialisée en binaire comme `node name` dans ZooKeeper.
* les colonnes autres que la clé primaire seront sérialisées en binaire dans l&#39;ordre correspondant et stockées comme valeur du nœud résultant défini par la clé sérialisée.
* les requêtes avec un filtrage sur la clé par `equals` ou `in` seront optimisées en recherches multiclés depuis `Keeper` ; sinon, toutes les valeurs seront récupérées.

Exemple :

```sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

avec

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

Chaque valeur, qui correspond à la sérialisation binaire de `(v1, v2, v3)`, sera stockée dans `/keeper_map_tables/keeper_map_table/data/serialized_key` dans `Keeper`.
De plus, le nombre de clés aura une limite souple fixée à 4.

Si plusieurs tables sont créées sur le même chemin ZooKeeper, les valeurs sont conservées tant qu&#39;au moins 1 table l&#39;utilise.
Par conséquent, il est possible d&#39;utiliser la clause `ON CLUSTER` lors de la création de la table afin de partager les données entre plusieurs instances de ClickHouse.
Bien sûr, il est aussi possible d&#39;exécuter manuellement `CREATE TABLE` avec le même chemin sur des instances de ClickHouse non liées pour obtenir le même effet de partage des données.

<div id="supported-operations">
  ## Opérations prises en charge
</div>

<div id="inserts">
  ### Insertions
</div>

Lorsque de nouvelles lignes sont insérées dans `KeeperMap`, si la clé n’existe pas, une nouvelle entrée associée à cette clé est créée.
Si la clé existe et que le paramètre `keeper_map_strict_mode` est défini sur `true`, une exception est levée ; sinon, la valeur associée à la clé est écrasée.

Exemple :

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Suppressions
</div>

Les lignes peuvent être supprimées à l’aide d’une requête `DELETE` ou de `TRUNCATE`.
Si la clé existe et que le paramètre `keeper_map_strict_mode` est défini sur `true`, la récupération et la suppression des données ne réussiront que si elles peuvent être exécutées de façon atomique.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

<div id="updates">
  ### Mises à jour
</div>

Les valeurs peuvent être mises à jour à l’aide de la requête `ALTER TABLE`. La clé primaire ne peut pas être modifiée.
Si le paramètre `keeper_map_strict_mode` est défini sur `true`, la récupération et la mise à jour des données ne réussissent que si elles sont exécutées de manière atomique.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Créer des applications d’analytics en temps réel avec ClickHouse et Hex](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)