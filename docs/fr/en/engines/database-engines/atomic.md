---
description: 'Le moteur `Atomic` prend en charge les requêtes non bloquantes `DROP TABLE` et `RENAME TABLE`,
  ainsi que les requêtes atomiques `EXCHANGE TABLES`. Le moteur de base de données `Atomic` est utilisé
  par défaut.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

Le moteur `Atomic` prend en charge les requêtes non bloquantes [`DROP TABLE`](#drop-detach-table) et [`RENAME TABLE`](#rename-table), ainsi que les requêtes atomiques [`EXCHANGE TABLES`](#exchange-tables). Le moteur de base de données `Atomic` est utilisé par défaut dans ClickHouse open-source.

:::note
Dans ClickHouse Cloud, le [moteur de base de données `Shared`](/fr/cloud/reference/shared-catalog#shared-database-engine) est utilisé par défaut et prend également en charge les opérations mentionnées ci-dessus.
:::

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## Spécificités et recommandations
</div>

<div id="table-uuid">
  ### UUID de la table
</div>

Chaque table de la base de données `Atomic` possède un [UUID](../../sql-reference/data-types/uuid.md) persistant et stocke ses données dans le répertoire suivant :

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Où `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` est l’UUID de la table.

Par défaut, l’UUID est généré automatiquement. Toutefois, il est possible de spécifier explicitement l’UUID lors de la création d’une table, bien que cela ne soit pas recommandé.

Par exemple :

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
Vous pouvez utiliser le paramètre [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) pour afficher l’UUID dans la requête `SHOW CREATE`.
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

Les requêtes [`RENAME`](../../sql-reference/statements/rename.md) ne modifient pas l’UUID et ne déplacent pas les données de la table. Elles s’exécutent immédiatement et n’attendent pas la fin des autres requêtes utilisant la table.

<div id="drop-detach-table">
  ### DROP/DETACH TABLE
</div>

Avec `DROP TABLE`, aucune donnée n&#39;est supprimée. Le moteur `Atomic` se contente de marquer la table comme supprimée en déplaçant ses métadonnées vers `/clickhouse_path/metadata_dropped/` et en avertissant le thread d&#39;arrière-plan. Le délai avant la suppression définitive des données de la table est spécifié par le paramètre [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec).
Vous pouvez activer le mode synchrone à l&#39;aide du modificateur `SYNC`. Pour cela, utilisez le paramètre [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously). Dans ce cas, `DROP` attend la fin des requêtes `SELECT`, `INSERT` et des autres requêtes en cours qui utilisent la table. La table sera supprimée dès qu&#39;elle ne sera plus utilisée.

<div id="exchange-tables">
  ### EXCHANGE TABLES/DICTIONARIES
</div>

La requête [`EXCHANGE`](../../sql-reference/statements/exchange.md) permute des tables ou des dictionnaires de manière atomique. Par exemple, au lieu de cette opération non atomique :

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

vous pouvez en utiliser une de type atomic :

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### ReplicatedMergeTree dans une base de données atomic
</div>

Pour les tables [`ReplicatedMergeTree`](/fr/engines/table-engines/mergetree-family/replication), il est recommandé de ne pas spécifier les paramètres du moteur pour le chemin dans ZooKeeper ni pour le nom de la replica. Dans ce cas, les paramètres de configuration [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) et [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name) seront utilisés. Si vous souhaitez spécifier explicitement les paramètres du moteur, il est recommandé d’utiliser la macro `{uuid}`. Cela garantit la génération automatique de chemins uniques pour chaque table dans ZooKeeper.

<div id="metadata-disk">
  ### Disque de métadonnées
</div>

Lorsque `disk` est spécifié dans `SETTINGS`, le disque est utilisé pour stocker les fichiers de métadonnées de la table.
Par exemple :

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

Si rien n’est précisé, le disque défini dans `database_disk.disk` est utilisé par défaut.

<div id="see-also">
  ## Voir aussi
</div>

* [system.databases](../../operations/system-tables/databases.md) table système