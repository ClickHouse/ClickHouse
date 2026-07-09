---
description: 'Documentation de CHECK TABLE'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'Instruction CHECK TABLE'
doc_type: 'reference'
---

La requête `CHECK TABLE` dans ClickHouse sert à effectuer une vérification d’une table donnée ou de ses partitions. Elle garantit l’intégrité des données en vérifiant les sommes de contrôle et d’autres structures internes des données.

Elle compare notamment les tailles réelles des fichiers aux valeurs attendues stockées sur le serveur. Si les tailles des fichiers ne correspondent pas aux valeurs stockées, cela signifie que les données sont corrompues. Cela peut être causé, par exemple, par un plantage du système pendant l’exécution de la requête.

:::warning
La requête `CHECK TABLE` peut lire toutes les données de la table et mobiliser certaines ressources, ce qui la rend gourmande en ressources.
Évaluez l’impact potentiel sur les performances et l’utilisation des ressources avant d’exécuter cette requête.
Cette requête n’améliorera pas les performances du système et vous ne devez pas l’exécuter si vous n’êtes pas certain de ce que vous faites.
:::

<div id="syntax">
  ## Syntaxe
</div>

La syntaxe de la requête est la suivante :

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name` : Spécifie le nom de la table à vérifier.
* `partition_expression` : (Facultatif) Si vous souhaitez vérifier une partition spécifique de la table, vous pouvez utiliser cette expression pour la spécifier.
* `part_name` : (Facultatif) Si vous souhaitez vérifier une partie spécifique de la table, vous pouvez ajouter un littéral de chaîne pour indiquer le nom de la partie.
* `FORMAT format` : (Facultatif) Permet de spécifier le format de sortie du résultat.
* `SETTINGS` : (Facultatif) Permet d’ajouter des paramètres supplémentaires.
  * (Facultatif) : [check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result) : Ce paramètre détermine si la sortie est détaillée (`0`) ou résumée (`1`).
  * D’autres paramètres peuvent également être appliqués. Si vous n’avez pas besoin d’un ordre déterministe pour les résultats, vous pouvez définir max&#95;threads sur une valeur supérieure à un afin d’accélérer la requête.

La réponse de la requête dépend de la valeur du paramètre `check_query_single_value_result`.
Dans le cas de `check_query_single_value_result = 1`, seule la colonne `result`, contenant une unique ligne, est renvoyée. La valeur de cette ligne est `1` si la vérification d’intégrité a réussi et `0` si les données sont corrompues.

Avec `check_query_single_value_result = 0`, la requête renvoie les colonnes suivantes :

* `part_path` : Indique le chemin vers la partie de données ou le nom du fichier.
  * `is_passed` : Renvoie 1 si la vérification de cette partie a réussi, 0 sinon.
  * `message` : Tout message supplémentaire lié à la vérification, par exemple des erreurs ou des messages de succès.

La requête `CHECK TABLE` prend en charge les moteurs de table suivants :

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [famille MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

Son exécution sur des tables utilisant d’autres moteurs de table provoque une exception `NOT_IMPLEMENTED`.

Les moteurs de la famille `*Log` n’assurent pas de récupération automatique des données en cas de défaillance. Utilisez la requête `CHECK TABLE` pour détecter rapidement toute perte de données.

<div id="examples">
  ## Exemples
</div>

Par défaut, la requête `CHECK TABLE` affiche l’état général de la vérification de la table :

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

Si vous souhaitez afficher le statut de vérification de chaque partie de données, vous pouvez utiliser le paramètre `check_query_single_value_result`.

De plus, pour vérifier une partition spécifique de la table, vous pouvez utiliser le mot-clé `PARTITION`.

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

De la même manière, vous pouvez vérifier une partie spécifique de la table à l’aide du mot-clé `PART`.

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

Notez que si la partie n’existe pas, la requête renvoie une erreur :

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### Obtenir un résultat &#39;Corrupted&#39;
</div>

:::warning
Avertissement : la procédure décrite ici, y compris la manipulation manuelle ou la suppression de fichiers directement dans le répertoire de données, est réservée aux environnements expérimentaux ou de développement. N’essayez **pas** cette procédure sur un serveur de production, car cela peut entraîner une perte de données ou d’autres conséquences imprévues.
:::

Supprimez le fichier de somme de contrôle existant :

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

Si le fichier checksums.txt est absent, il peut être restauré. Il sera recalculé et réécrit lors de l’exécution de la commande `CHECK TABLE` pour la partition concernée, et l’état sera toujours indiqué comme &#39;is&#95;passed = 1&#39;.

Vous pouvez vérifier toutes les tables `(Replicated)MergeTree` existantes en une seule fois à l’aide de la requête `CHECK ALL TABLES`.

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## Si les données sont corrompues
</div>

Si la table est corrompue, vous pouvez copier les données non corrompues dans une autre table. Pour ce faire :

1. Créez une nouvelle table avec la même structure que la table endommagée. Pour ce faire, exécutez la requête `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2. Définissez la valeur de `max_threads` sur 1 afin d’exécuter la requête suivante sur un seul thread. Pour ce faire, exécutez la requête `SET max_threads = 1`.
3. Exécutez la requête `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Cette requête copie les données non corrompues de la table endommagée dans une autre table. Seules les données situées avant la partie corrompue seront copiées.
4. Redémarrez le `clickhouse-client` pour réinitialiser la valeur de `max_threads`.