---
description: 'Documentation de l’instruction INSERT INTO'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'Instruction INSERT INTO'
doc_type: 'reference'
---

Insère des données dans une table.

**Syntaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Vous pouvez spécifier une liste de colonnes à insérer à l’aide de `(c1, c2, c3)`. Vous pouvez également utiliser une expression avec un [sélecteur](../../sql-reference/statements/select/index.md#asterisk) de colonnes tel que `*` et/ou des [modificateurs](../../sql-reference/statements/select/index.md#select-modifiers) comme [APPLY](/fr/sql-reference/statements/select/apply-modifier), [EXCEPT](/fr/sql-reference/statements/select/except-modifier), [REPLACE](/fr/sql-reference/statements/select/replace-modifier).

Par exemple, considérons la table :

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

Si vous souhaitez insérer des données dans toutes les colonnes, à l’exception de la colonne `b`, vous pouvez le faire à l’aide du mot-clé `EXCEPT`. En vous référant à la syntaxe ci-dessus, veillez à insérer autant de valeurs (`VALUES (v11, v13)`) que de colonnes spécifiées (`(c1, c3)`) :

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

Dans cet exemple, on voit que, dans la deuxième ligne insérée, les colonnes `a` et `c` sont remplies avec les valeurs transmises, et `b` avec la valeur par défaut. Il est également possible d’utiliser le mot-clé `DEFAULT` pour insérer des valeurs par défaut :

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

Si une liste de colonnes n’inclut pas toutes les colonnes existantes, les autres colonnes sont remplies avec :

* Les valeurs calculées à partir des expressions `DEFAULT` spécifiées dans la définition de la table.
* Des zéros et des chaînes vides, si aucune expression `DEFAULT` n’est définie.

Les données peuvent être fournies à l’instruction INSERT dans n’importe quel [format](/fr/sql-reference/formats) pris en charge par ClickHouse. Le format doit être spécifié explicitement dans la requête :

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

Par exemple, le format de requête suivant est identique à la version de base de `INSERT ... VALUES` :

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse supprime tous les espaces et un saut de ligne (s&#39;il y en a un) avant les données. Lorsque vous rédigez une requête, nous recommandons de placer les données sur une nouvelle ligne après les opérateurs de la requête, ce qui est important si les données commencent par des espaces.

Exemple :

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Vous pouvez insérer des données séparément de la requête en utilisant le [client en ligne de commande](/fr/operations/utilities/clickhouse-local) ou l’[interface HTTP](/fr/interfaces/http).

:::note
Si vous souhaitez spécifier `SETTINGS` pour une requête `INSERT`, vous devez le faire *avant* la clause `FORMAT`, puisque tout ce qui suit `FORMAT format_name` est traité comme des données. Par exemple :

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## Contraintes
</div>

Si une table possède des [contraintes](../../sql-reference/statements/create/table.md#constraints), leurs expressions sont vérifiées pour chaque ligne de données insérée. Si l’une de ces contraintes n’est pas respectée, le serveur renverra une exception indiquant le nom et l’expression de la contrainte, et la requête sera interrompue.

<div id="data-type-validation">
  ## Validation des types de données
</div>

ClickHouse valide les types de données autorisés (contrôlés par des paramètres comme `enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types`, etc.) uniquement lors de la création d’une table (`CREATE TABLE`) et de la modification du schéma (`ALTER TABLE`), et non lors d’un `INSERT`.

Cela signifie que si une table contenant un type de données non autorisé existe déjà, il reste possible d’y insérer des données même lorsque le paramètre correspondant est désactivé sur le serveur. C’est intentionnel : une fois la table créée, les insertions ne doivent pas être bloquées par des paramètres qui contrôlent la création des types.

Par exemple :

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
Par conséquent, un client exécutant une version plus récente (où un paramètre est activé par défaut) peut insérer des données contenant des types de données non autorisés dans un serveur exécutant une version plus ancienne (où ce paramètre est désactivé), à condition que la table cible possède déjà les types de colonnes correspondants. La validation est appliquée au niveau DDL, et non au niveau DML.
:::

<div id="inserting-the-results-of-select">
  ## Insertion des résultats d’un SELECT
</div>

**Syntaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

Les colonnes sont associées en fonction de leur position dans la clause `SELECT`. Cependant, leurs noms dans l’expression `SELECT` et dans la table de destination de `INSERT` peuvent différer. Si nécessaire, un transtypage est effectué.

Aucun des formats de données, à l’exception du format Values, ne permet d’attribuer des valeurs à des expressions telles que `now()`, `1 + 2`, etc. Le format Values autorise une utilisation limitée des expressions, mais cela n’est pas recommandé, car dans ce cas, un code inefficace est utilisé pour les exécuter.

Les autres requêtes de modification des data parts ne sont pas prises en charge : `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Cependant, vous pouvez supprimer les anciennes données à l’aide de `ALTER TABLE ... DROP PARTITION`.

La clause `FORMAT` doit être spécifiée à la fin de la requête si la clause `SELECT` contient la fonction de table [input()](../../sql-reference/table-functions/input.md).

Pour insérer une valeur par défaut au lieu de `NULL` dans une colonne avec un type de données non Nullable, activez le paramètre [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default).

`INSERT` prend également en charge les CTE (common table expression). Par exemple, les deux instructions suivantes sont équivalentes :

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## Insertion de données depuis un fichier
</div>

**Syntaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Utilisez la syntaxe ci-dessus pour insérer des données à partir d’un ou de plusieurs fichiers stockés côté **client**. `file_name` et `type` sont des chaînes littérales. Le [format](../../interfaces/formats.md) du fichier d’entrée doit être indiqué dans la clause `FORMAT`.

Les fichiers compressés sont pris en charge. Le type de compression est détecté à partir de l’extension du nom de fichier. Il peut aussi être spécifié explicitement dans une clause `COMPRESSION`. Les types pris en charge sont : `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

Cette fonctionnalité est disponible dans le [client en ligne de commande](../../interfaces/client.md) et [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Exemples**

<div id="single-file-with-from-infile">
  ### Un seul fichier avec FROM INFILE
</div>

Exécutez les requêtes suivantes à l’aide du [client en ligne de commande](../../interfaces/client.md):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### Plusieurs fichiers avec FROM INFILE à l’aide de motifs glob
</div>

Cet exemple est très similaire au précédent, mais les insertions s’effectuent depuis plusieurs fichiers à l’aide de `FROM INFILE 'input_*.csv'`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
En plus de sélectionner plusieurs fichiers avec `*`, vous pouvez utiliser des plages (`{1,2}` ou `{1..9}`) et d’autres [substitutions glob](/fr/sql-reference/table-functions/file.md/#globs-in-path). Les trois exemples suivants fonctionneraient avec l’exemple ci-dessus :

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## Insertion avec une fonction de table
</div>

Des données peuvent être insérées dans des tables référencées par des [fonctions de table](../../sql-reference/table-functions/index.md).

**Syntaxe**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Exemple**

La fonction de table [remote](/fr/sql-reference/table-functions/remote) est utilisée dans les requêtes suivantes :

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## Insertion dans ClickHouse Cloud
</div>

Par défaut, les services sur ClickHouse Cloud disposent de plusieurs répliques pour assurer une haute disponibilité. Lorsque vous vous connectez à un service, une connexion est établie avec l&#39;une de ces répliques.

Une fois qu&#39;un `INSERT` a abouti, les données sont écrites dans le stockage sous-jacent. Cependant, il peut s&#39;écouler un certain temps avant que les répliques ne reçoivent ces mises à jour. Par conséquent, si vous utilisez une autre connexion qui exécute une requête `SELECT` sur l&#39;une de ces autres répliques, il se peut que les données mises à jour n&#39;y soient pas encore visibles.

Il est possible d&#39;utiliser `select_sequential_consistency` pour forcer la réplique à recevoir les dernières mises à jour. Voici un exemple de requête `SELECT` utilisant ce paramètre :

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Notez que l&#39;utilisation de `select_sequential_consistency` augmentera la charge de ClickHouse Keeper (utilisé en interne par ClickHouse Cloud) et peut entraîner des performances plus lentes selon la charge du service. Nous déconseillons d&#39;activer ce paramètre sauf si nécessaire. L&#39;approche recommandée consiste à effectuer les lectures/écritures dans la même session ou à utiliser un driver client qui utilise le protocole natif (et prend donc en charge les connexions persistantes).

<div id="inserting-into-a-replicated-setup">
  ## Insertion dans une configuration répliquée
</div>

Dans une configuration répliquée, les données ne seront visibles sur les autres répliques qu’une fois répliquées. La réplication des données (leur téléchargement sur les autres répliques) commence immédiatement après un `INSERT`. Cela diffère de ClickHouse Cloud, où les données sont immédiatement écrites dans un stockage partagé et où les répliques s’abonnent aux modifications de métadonnées.

Notez que, dans les configurations répliquées, les `INSERTs` peuvent parfois prendre un certain temps (de l’ordre d’une seconde), car ils nécessitent un commit dans ClickHouse Keeper pour le consensus distribué. L’utilisation de S3 pour le stockage ajoute également de la latence.

<div id="performance-considerations">
  ## Considérations relatives aux performances
</div>

`INSERT` trie les données d’entrée selon la clé primaire et les répartit en partitions selon la clé de partition. Si vous insérez des données dans plusieurs partitions à la fois, cela peut réduire considérablement les performances de la requête `INSERT`. Pour éviter cela :

* Ajoutez les données par lots suffisamment volumineux, par exemple 100 000 lignes à la fois.
* Regroupez les données par clé de partition avant de les téléverser dans ClickHouse.

Les performances ne diminueront pas si :

* Les données sont ajoutées en temps réel.
* Vous téléversez des données généralement triées par temps.

<div id="asynchronous-inserts">
  ### Insertions asynchrones
</div>

Il est possible d&#39;insérer des données de manière asynchrone au moyen d&#39;insertions de petite taille mais fréquentes. Les données issues de ces insertions sont regroupées en lots, puis insérées en toute sécurité dans une table. Pour utiliser les insertions asynchrones, activez le paramètre [`async_insert`](/fr/operations/settings/settings#async_insert).

L&#39;utilisation de `async_insert` ou du [moteur de table `Buffer`](/fr/engines/table-engines/special/buffer) entraîne une mise en mémoire tampon supplémentaire.

<div id="large-or-long-running-inserts">
  ### Insertions volumineuses ou de longue durée
</div>

Lorsque vous insérez de grands volumes de données, ClickHouse optimise les performances d&#39;écriture grâce à un processus appelé « squashing ». Les petits blocs de données insérées en mémoire sont fusionnés et regroupés en blocs plus volumineux avant d&#39;être écrits sur le disque. Le squashing réduit la surcharge associée à chaque opération d&#39;écriture. Dans ce processus, les données insérées deviennent disponibles à la requête une fois que ClickHouse a terminé l&#39;écriture de chaque groupe de [`max_insert_block_size`](/fr/operations/settings/settings#max_insert_block_size) lignes.

**Voir aussi**

* [async&#95;insert](/fr/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/fr/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/fr/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/fr/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/fr/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/fr/operations/settings/settings#async_insert_max_data_size)