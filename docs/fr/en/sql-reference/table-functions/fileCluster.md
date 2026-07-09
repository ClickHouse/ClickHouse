---
description: 'Permet le traitement simultané des fichiers correspondant à un chemin spécifié sur plusieurs nœuds d''un cluster. L''initiateur établit des connexions avec les nœuds worker, développe les globs dans le chemin de fichier et délègue les tâches de lecture aux nœuds worker. Chaque nœud worker interroge l''initiateur pour obtenir le fichier suivant à traiter, jusqu''à ce que toutes les tâches soient terminées (c''est-à-dire que tous les fichiers aient été lus).'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

Permet le traitement simultané des fichiers correspondant à un chemin spécifié sur plusieurs nœuds d’un cluster. L’initiateur établit des connexions avec les nœuds worker, développe les globs dans le chemin de fichier et délègue les tâches de lecture aux nœuds worker. Chaque nœud worker interroge l’initiateur pour obtenir le fichier suivant à traiter, jusqu’à ce que toutes les tâches soient terminées (c’est-à-dire que tous les fichiers aient été lus).

:::note
Cette fonction ne fonctionne *correctement* que si l’ensemble des fichiers correspondant au chemin initialement spécifié est identique sur tous les nœuds et que leur contenu est cohérent d’un nœud à l’autre.
Si ces fichiers diffèrent d’un nœud à l’autre, la valeur de retour ne peut pas être déterminée à l’avance et dépend de l’ordre dans lequel les nœuds worker demandent des tâches à l’initiateur.
:::

<div id="syntax">
  ## Syntaxe
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## Arguments
</div>

| Argument             | Description                                                                                                                                                                                                                |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | Nom d&#39;un cluster utilisé pour construire un ensemble d&#39;adresses et de paramètres de connexion pour les serveurs distants et locaux.                                                                                |
| `path`               | Chemin relatif vers le fichier à partir de [user&#95;files&#95;path](/fr/operations/server-configuration-parameters/settings.md#user_files_path). Le chemin du fichier prend également en charge les [globs](#globs-in-path). |
| `format`             | [Format](/fr/sql-reference/formats) des fichiers. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                  |
| `structure`          | Structure de la table au format `'UserID UInt64, Name String'`. Détermine les noms et les types des colonnes. Type : [String](../../sql-reference/data-types/string.md).                                                   |
| `compression_method` | Méthode de compression. Les types de compression pris en charge sont `gz`, `br`, `xz`, `zst`, `lz4` et `bz2`.                                                                                                              |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table dans le format et la structure spécifiés, contenant les données des fichiers correspondant au chemin spécifié.

**Exemple**

Étant donné un cluster nommé `my_cluster` et la valeur suivante du paramètre `user_files_path` :

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

De plus, étant donné que des fichiers `test1.csv` et `test2.csv` se trouvent dans `user_files_path` de chaque nœud du cluster et que leur contenu est identique d’un nœud à l’autre :

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

Par exemple, ces fichiers peuvent être créés en exécutant ces deux requêtes sur chaque nœud du cluster :

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

Lisez maintenant les données contenues dans `test1.csv` et `test2.csv` à l’aide de la fonction de table `fileCluster` :

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## Globs dans le chemin
</div>

Tous les motifs pris en charge par la fonction de table [File](../../sql-reference/table-functions/file.md#globs-in-path) le sont également par FileCluster.

<div id="related">
  ## Voir aussi
</div>

* [Fonction de table File](../../sql-reference/table-functions/file.md)