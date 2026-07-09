---
description: 'Permet d’attacher instantanément une table ou une base de données depuis des sauvegardes en lecture seule.'
sidebar_label: 'Backup'
sidebar_position: 60
slug: /engines/database-engines/backup
title: 'Backup'
doc_type: 'reference'
---

Le moteur de base de données Backup permet d’attacher instantanément une table ou une base de données depuis des [sauvegardes](/fr/operations/backup/overview) en lecture seule.

Le moteur de base de données Backup fonctionne avec les sauvegardes incrémentielles comme non incrémentielles.

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
CREATE DATABASE backup_database
ENGINE = Backup('database_name_inside_backup', Disk('disk_name', 'backup_name'))
```

La destination de sauvegarde peut être n’importe quelle [destination de sauvegarde](/fr/operations/backup/disk#configure-backup-destinations-for-disk) valide, telle que `Disk`, `S3` ou `File`. Elle est passée sous forme de fonction, par exemple `Disk('disk_name', 'backup_name')`.

**Paramètres du moteur**

* `database_name_inside_backup` — Nom de la base de données dans la sauvegarde.
* `backup_destination` — Destination de sauvegarde.

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Prenons un exemple avec une destination de sauvegarde `Disk`. Configurons d’abord le disque de sauvegarde dans `storage.xml` :

```xml
<storage_configuration>
    <disks>
        <backups>
            <type>local</type>
            <path>/home/ubuntu/ClickHouseWorkDir/backups/</path>
        </backups>
    </disks>
</storage_configuration>
<backups>
    <allowed_disk>backups</allowed_disk>
    <allowed_path>/home/ubuntu/ClickHouseWorkDir/backups/</allowed_path>
</backups>
```

Exemple d’utilisation. Créons une base de données de test, des tables, insérons quelques données, puis créons une sauvegarde :

```sql
CREATE DATABASE test_database;

CREATE TABLE test_database.test_table_1 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_1 VALUES (0, 'test_database.test_table_1');

CREATE TABLE test_database.test_table_2 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_2 VALUES (0, 'test_database.test_table_2');

CREATE TABLE test_database.test_table_3 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_3 VALUES (0, 'test_database.test_table_3');

BACKUP DATABASE test_database TO Disk('backups', 'test_database_backup');
```

Maintenant que nous avons la sauvegarde `test_database_backup`, créons la base de données Backup :

```sql
CREATE DATABASE test_database_backup ENGINE = Backup('test_database', Disk('backups', 'test_database_backup'));
```

Nous pouvons désormais interroger n’importe quelle table de la base de données :

```sql
SELECT id, value FROM test_database_backup.test_table_1;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_1 │
└────┴────────────────────────────┘

SELECT id, value FROM test_database_backup.test_table_2;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_2 │
└────┴────────────────────────────┘

SELECT id, value FROM test_database_backup.test_table_3;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_3 │
└────┴────────────────────────────┘
```

Il est également possible de travailler avec cette base de données Backup comme avec toute base de données ordinaire. Par exemple, en interrogeant les tables qu’elle contient :

```sql
SELECT database, name FROM system.tables WHERE database = 'test_database_backup';
```

```text
┌─database─────────────┬─name─────────┐
│ test_database_backup │ test_table_1 │
│ test_database_backup │ test_table_2 │
│ test_database_backup │ test_table_3 │
└──────────────────────┴──────────────┘
```