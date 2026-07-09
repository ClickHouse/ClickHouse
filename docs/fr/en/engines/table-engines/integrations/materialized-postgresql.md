---
description: 'Crée une table ClickHouse avec un export initial des données d''une
  table PostgreSQL et lance le processus de réplication.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'Moteur de table MaterializedPostgreSQL'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # Moteur de table MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Il est recommandé aux utilisateurs de ClickHouse Cloud d&#39;utiliser [ClickPipes](/fr/integrations/clickpipes) pour la réplication de PostgreSQL vers ClickHouse. Cette solution prend en charge nativement la capture des changements de données (CDC) haute performance pour PostgreSQL.
:::

Crée une table ClickHouse à partir d&#39;un export initial des données d&#39;une table PostgreSQL et démarre le processus de réplication, c&#39;est-à-dire exécute une tâche en arrière-plan pour appliquer les nouvelles modifications au fur et à mesure qu&#39;elles surviennent dans la table PostgreSQL de la base de données PostgreSQL distante.

:::note
Ce moteur de table est expérimental. Pour l&#39;utiliser, définissez `allow_experimental_materialized_postgresql_table` sur 1 dans vos fichiers de configuration ou à l&#39;aide de la commande `SET` :

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

Si plusieurs tables sont nécessaires, il est fortement recommandé d’utiliser le moteur de base de données [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) plutôt que le moteur de table, ainsi que le paramètre `materialized_postgresql_tables_list`, qui spécifie les tables à répliquer (il sera également possible d’ajouter le `schema` de la base de données). Cette solution sera nettement meilleure en termes de CPU, avec moins de connexions et moins de slots de réplication dans la base de données PostgreSQL distante.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Paramètres du moteur**

* `host:port` — Adresse du serveur PostgreSQL.
* `database` — Nom de la base de données distante.
* `table` — Nom de la table distante.
* `user` — Utilisateur PostgreSQL.
* `password` — Mot de passe de l’utilisateur.

<div id="requirements">
  ## Exigences
</div>

1. Le paramètre [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) doit être défini sur `logical`, et le paramètre `max_replication_slots` doit avoir une valeur d’au moins `2` dans le fichier de configuration de PostgreSQL.

2. Une table avec le moteur `MaterializedPostgreSQL` doit avoir une clé primaire, identique à l’index `replica identity` (par défaut : la clé primaire) d’une table PostgreSQL (voir les [détails sur l’index `replica identity`](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Seul le moteur de base de données [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)) est autorisé.

4. Le moteur de table `MaterializedPostgreSQL` fonctionne uniquement avec les versions de PostgreSQL &gt;= 11, car son implémentation nécessite la fonction PostgreSQL [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html).

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_version` — Compteur de transactions. Type : [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — Marqueur de suppression. Type : [Int8](../../../sql-reference/data-types/int-uint.md). Valeurs possibles :
  * `1` — La ligne n&#39;est pas supprimée,
  * `-1` — La ligne est supprimée.

Il n&#39;est pas nécessaire d&#39;ajouter ces colonnes lors de la création d&#39;une table. Elles sont toujours accessibles dans une requête `SELECT`.
La colonne `_version` correspond à la position `LSN` dans le `WAL` et peut donc être utilisée pour vérifier à quel point la réplication est à jour.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
La réplication des valeurs [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) n’est pas prise en charge. La valeur par défaut de ce type de données sera utilisée.
:::