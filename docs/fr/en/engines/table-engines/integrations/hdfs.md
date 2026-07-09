---
description: "Ce moteur assure l'intégration avec l'écosystème Apache Hadoop en
  permettant de gérer les données sur HDFS via ClickHouse. Il est similaire aux moteurs File
  et URL, mais offre des fonctionnalités spécifiques à Hadoop."
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'Moteur de table HDFS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # Moteur de table HDFS
</div>

<CloudNotSupportedBadge />

Ce moteur assure l’intégration avec l’écosystème [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) en permettant de gérer des données sur [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) via ClickHouse. Ce moteur est similaire aux moteurs [File](/fr/engines/table-engines/special/file) et [URL](/fr/engines/table-engines/special/url), mais offre des fonctionnalités spécifiques à Hadoop.

Cette fonctionnalité n’est pas prise en charge par les ingénieurs ClickHouse et sa qualité est connue pour être douteuse. En cas de problème, corrigez-le vous-même et soumettez une pull request.

<div id="usage">
  ## Utilisation
</div>

```sql
ENGINE = HDFS(URI, format)
```

**Paramètres du moteur**

* `URI` - URI complète du fichier dans HDFS. La partie chemin de `URI` peut contenir des globs. Dans ce cas, la table est en lecture seule.
* `format` - spécifie l’un des formats de fichier disponibles. Pour exécuter des requêtes
  `SELECT`, le format doit être pris en charge en entrée, et pour exécuter des requêtes
  `INSERT` – en sortie. Les formats disponibles sont répertoriés dans la
  section [Formats](/fr/sql-reference/formats#formats-overview).
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — Facultatif. Dans la plupart des cas, vous n’avez pas besoin de clé de partition et, si c’est nécessaire, elle n’a généralement pas besoin d’être plus fine qu’un partitionnement mensuel. Le partitionnement n’accélère pas les requêtes (contrairement à l’expression ORDER BY). N’utilisez jamais un partitionnement trop fin. Ne partitionnez pas vos données par identifiant ou nom de client (faites plutôt de l’identifiant ou du nom du client la première colonne de l’expression ORDER BY).

Pour un partitionnement par mois, utilisez l’expression `toYYYYMM(date_column)`, où `date_column` est une colonne contenant une date de type [Date](/fr/sql-reference/data-types/date.md). Les noms de partition sont ici au format `"YYYYMM"`.

**Exemple :**

**1.** Configurez la table `hdfs_engine_table` :

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Remplissage du fichier :

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Interrogez les données :

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## Détails d’implémentation
</div>

* Les lectures et les écritures peuvent être parallélisées.
* Non pris en charge :

  * les opérations `ALTER` et `SELECT...SAMPLE` ;
  * les index ;
  * la réplication [zero-copy](../../../operations/storing-data.md#zero-copy) est possible, mais n’est pas recommandée.

  :::note La réplication zero-copy n’est pas prête pour la production
  La réplication zero-copy est désactivée par défaut dans ClickHouse version 22.8 et ultérieure. Cette fonctionnalité n’est pas recommandée pour une utilisation en production.
  :::

**Globs dans le chemin**

Plusieurs composants du chemin peuvent contenir des globs. Pour être traité, un fichier doit exister et correspondre à l’intégralité du motif de chemin. La liste des fichiers est déterminée lors du `SELECT` (et non au moment du `CREATE`).

* `*` — Remplace n’importe quel nombre de caractères, sauf `/`, y compris la chaîne vide.
* `?` — Remplace n’importe quel caractère unique.
* `{some_string,another_string,yet_another_one}` — Remplace l’une des chaînes `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Remplace n’importe quel nombre dans l’intervalle de N à M, bornes incluses.

Les constructions avec `{}` sont similaires à la table function [remote](../../../sql-reference/table-functions/remote.md).

**Exemple**

1. Supposons que nous ayons plusieurs fichiers au format TSV avec les URI suivantes sur HDFS :

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Il existe plusieurs façons de créer une table à partir de ces six fichiers :

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Autre méthode :

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

La table comprend tous les fichiers des deux répertoires (tous les fichiers doivent respecter le format et le schéma décrits dans la requête) :

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
Si la liste des fichiers contient des plages de nombres avec des zéros non significatifs, utilisez la construction avec des accolades pour chaque chiffre séparément, ou utilisez `?`.
:::

**Exemple**

Créez une table avec des fichiers nommés `file000`, `file001`, ... , `file999` :

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## Configuration
</div>

Comme GraphiteMergeTree, le moteur HDFS prend en charge une configuration étendue via le fichier de configuration de ClickHouse. Vous pouvez utiliser deux clés de configuration : une clé globale (`hdfs`) et une clé au niveau utilisateur (`hdfs_*`). La configuration globale est appliquée en premier, puis la configuration au niveau utilisateur est appliquée (si elle existe).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### Options de configuration
</div>

<div id="supported-by-libhdfs3">
  #### Pris en charge par libhdfs3
</div>

| **paramètre**                                                           | **valeur par défaut**             |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

La [référence de configuration HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) peut apporter des explications sur certains paramètres.

<div id="clickhouse-extras">
  #### Extras de ClickHouse
</div>

| **paramètre**                     | **valeur par défaut** |
| --------------------------------- | --------------------- |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot;          |
| hadoop&#95;kerberos&#95;principal | &quot;&quot;          |
| libhdfs3&#95;conf                 | &quot;&quot;          |

<div id="limitations">
  ### Limitations
</div>

* `hadoop_security_kerberos_ticket_cache_path` et `libhdfs3_conf` ne peuvent être définis qu&#39;au niveau global, et non par utilisateur

<div id="kerberos-support">
  ## Prise en charge de Kerberos
</div>

Si le paramètre `hadoop_security_authentication` a pour valeur `kerberos`, ClickHouse s&#39;authentifie via Kerberos.
Les paramètres sont [ici](#clickhouse-extras), et `hadoop_security_kerberos_ticket_cache_path` peut être utile.
Notez qu&#39;en raison des limitations de libhdfs3, seule l&#39;approche traditionnelle est prise en charge ;
les communications avec le datanode ne sont pas sécurisées par SASL (`HADOOP_SECURE_DN_USER` est un indicateur fiable de cette
approche de sécurité). Utilisez `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` comme référence.

Si `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` ou `hadoop_security_kerberos_ticket_cache_path` sont spécifiés, l&#39;authentification Kerberos sera utilisée. `hadoop_kerberos_keytab` et `hadoop_kerberos_principal` sont obligatoires dans ce cas.

<div id="namenode-ha">
  ## Prise en charge de la haute disponibilité du NameNode HDFS
</div>

libhdfs3 prend en charge la haute disponibilité du NameNode HDFS.

* Copiez `hdfs-site.xml` d&#39;un nœud HDFS vers `/etc/clickhouse-server/`.
* Ajoutez l&#39;extrait suivant au fichier de configuration de ClickHouse :

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* Utilisez ensuite la valeur de la balise `dfs.nameservices` dans `hdfs-site.xml` comme adresse du namenode dans l’URI HDFS. Par exemple, remplacez `hdfs://appadmin@192.168.101.11:8020/abc/` par `hdfs://appadmin@my_nameservice/abc/`.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille est inconnue, la valeur est `NULL`.
* `_time` — Date de dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette date est inconnue, la valeur est `NULL`.

<div id="storage-settings">
  ## Paramètres de stockage
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/fr/operations/settings/settings.md#hdfs_truncate_on_insert) - permet de tronquer le fichier avant d’y insérer des données. Désactivé par défaut.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/fr/operations/settings/settings.md#hdfs_create_new_file_on_insert) - permet de créer un nouveau fichier à chaque insertion si le format comporte un suffixe. Désactivé par défaut.
* [hdfs&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#hdfs_skip_empty_files) - permet d’ignorer les fichiers vides lors de la lecture. Désactivé par défaut.

**Voir aussi**

* [Colonnes virtuelles](../../../engines/table-engines/index.md#table_engines-virtual_columns)