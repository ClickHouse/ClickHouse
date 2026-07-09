---
description: 'Permet de traiter en parallèle des fichiers HDFS à partir de nombreux nœuds d’un cluster spécifié.'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'référence'
---

Permet de traiter en parallèle des fichiers HDFS à partir de nombreux nœuds d’un cluster spécifié. Sur le nœud initiateur, une connexion est établie avec tous les nœuds du cluster, les astérisques du chemin de fichier HDFS sont résolus, puis chaque fichier est réparti dynamiquement. Sur le nœud worker, celui-ci interroge l’initiateur pour connaître la tâche suivante à traiter, puis la traite. Ce processus se répète jusqu’à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## Arguments
</div>

| Argument       | Description                                                                                                                                                                                                                                                                                                                                                 |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nom d’un cluster utilisé pour construire un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux.                                                                                                                                                                                                                         |
| `URI`          | URI d’un fichier ou d’un ensemble de fichiers. Prend en charge les caractères génériques suivants en mode lecture seule : `*`, `**`, `?`, `{'abc','def'}` et `{N..M}`, où `N` et `M` sont des nombres, et `abc` et `def` des chaînes. Pour plus d’informations, voir [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `format`       | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                                                                                                                                                             |
| `structure`    | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                              |

<div id="returned_value">
  ## Valeur de retour
</div>

Une table ayant la structure spécifiée pour lire les données du fichier spécifié.

<div id="examples">
  ## Exemples
</div>

1. Supposons que nous ayons un cluster ClickHouse nommé `cluster_simple`, ainsi que plusieurs fichiers avec les URI suivantes sur HDFS :

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Exécutez une requête pour obtenir le nombre de lignes dans ces fichiers :

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Interrogez le nombre de lignes de tous les fichiers de ces deux répertoires :

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
Si votre liste de fichiers contient des plages numériques avec des zéros non significatifs, utilisez la syntaxe avec des accolades pour chaque chiffre séparément, ou utilisez `?`.
:::

<div id="related">
  ## Voir aussi
</div>

* [Moteur HDFS](../../engines/table-engines/integrations/hdfs.md)
* [Fonction de table HDFS](../../sql-reference/table-functions/hdfs.md)