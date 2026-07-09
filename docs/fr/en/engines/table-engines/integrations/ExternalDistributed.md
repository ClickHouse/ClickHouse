---
description: 'Le moteur `ExternalDistributed` permet d''effectuer des requêtes `SELECT`
  sur des données stockées sur des serveurs MySQL ou PostgreSQL distants. Il accepte des moteurs MySQL ou
  PostgreSQL comme argument, ce qui permet le sharding.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'Moteur de table `ExternalDistributed`'
doc_type: 'reference'
---

Le moteur `ExternalDistributed` permet d&#39;effectuer des requêtes `SELECT` sur des données stockées sur des serveurs MySQL ou PostgreSQL distants. Il accepte des moteurs [MySQL](../../../engines/table-engines/integrations/mysql.md) ou [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) comme argument, ce qui permet le sharding.

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

Voir une description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

La structure de la table peut être différente de celle de la table d’origine :

* Les noms de colonnes doivent être les mêmes que dans la table d’origine, mais vous pouvez n’utiliser que certaines de ces colonnes, dans n’importe quel ordre.
* Les types de colonnes peuvent différer de ceux de la table d’origine. ClickHouse essaie de [convertir](/fr/sql-reference/functions/type-conversion-functions#CAST) les valeurs vers les types de données ClickHouse.

**Paramètres du moteur**

* `engine` — Le moteur de table `MySQL` ou `PostgreSQL`.
* `host:port` — Adresse du serveur MySQL ou PostgreSQL.
* `database` — Nom de la base de données distante.
* `table` — Nom de la table distante.
* `user` — Nom d’utilisateur.
* `password` — Mot de passe de l’utilisateur.

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Prend en charge plusieurs répliques, qui doivent être séparées par `|`, et plusieurs shards, qui doivent être séparés par `,`. Par exemple :

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

Lorsque vous spécifiez des répliques, l’une des répliques disponibles est sélectionnée pour chaque shard lors de la lecture. Si la connexion échoue, la réplique suivante est sélectionnée, et ainsi de suite jusqu’à épuisement de toutes les répliques. Si la tentative de connexion échoue pour toutes les répliques, elle est répétée plusieurs fois de la même manière.

Vous pouvez spécifier n’importe quel nombre de shards et n’importe quel nombre de répliques pour chaque shard.

**Voir aussi**

* [Moteur de table MySQL](../../../engines/table-engines/integrations/mysql.md)
* [Moteur de table PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
* [Moteur de table Distributed](../../../engines/table-engines/special/distributed.md)