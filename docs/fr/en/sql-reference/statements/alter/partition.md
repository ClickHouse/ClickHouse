---
description: 'Documentation sur PARTITION'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'Manipulation des partitions et des parts'
doc_type: 'reference'
---

Les opérations suivantes sur les [partitions](/fr/engines/table-engines/mergetree-family/custom-partitioning-key.md) sont disponibles :

* [DETACH PARTITION|PART](#detach-partitionpart) — Déplace une partition ou une part vers le répertoire `detached` et la retire des métadonnées actives.
* [DROP PARTITION|PART](#drop-partitionpart) — Supprime une partition ou une part.
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - Supprime une part ou toutes les parts d’une partition depuis `detached`.
* [FORGET PARTITION](#forget-partition) — Supprime les métadonnées d’une partition de ZooKeeper si elle est vide.
* [ATTACH PARTITION|PART](#attach-partitionpart) — Ajoute à la table une partition ou une part depuis le répertoire `detached`.
* [ATTACH PARTITION FROM](#attach-partition-from) — Copie la partition de données d’une table vers une autre et l’y ajoute.
* [REPLACE PARTITION](#replace-partition) — Copie la partition de données d’une table vers une autre et la remplace.
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — Déplace la partition de données d’une table vers une autre.
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — Réinitialise la valeur d’une colonne spécifiée dans une partition.
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — Réinitialise l’index secondaire spécifié dans une partition.
* [FREEZE PARTITION](#freeze-partition) — Crée une sauvegarde d’une partition.
* [UNFREEZE PARTITION](#unfreeze-partition) — Supprime une sauvegarde d’une partition.
* [FETCH PARTITION|PART](#fetch-partitionpart) — Télécharge une part ou une partition depuis un autre serveur.
* [MOVE PARTITION|PART](#move-partitionpart) — Déplace une partition ou une part de données vers un autre disque ou volume.
* [UPDATE IN PARTITION](#update-in-partition) — Met à jour les données à l’intérieur de la partition selon une condition.
* [DELETE IN PARTITION](#delete-in-partition) — Supprime les données à l’intérieur de la partition selon une condition.
* [REWRITE PARTS](#rewrite-parts) — Réécrit entièrement les parts de la table (ou d’une partition spécifique).

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Déplace toutes les données de la partition spécifiée vers le répertoire `detached`. Le serveur ignore la partition de données détachée, comme si elle n’existait pas. Il ne prendra pas ces données en compte tant que vous n’aurez pas exécuté la requête [ATTACH](#attach-partitionpart).

Exemple :

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression) pour savoir comment définir l’expression de partition.

Une fois la requête exécutée, vous pouvez faire ce que vous voulez des données du répertoire `detached` : les supprimer du système de fichiers ou simplement les laisser.

Cette requête est répliquée : elle déplace les données vers le répertoire `detached` sur toutes les répliques. Notez que vous ne pouvez exécuter cette requête que sur une réplique leader. Pour savoir si une réplique est leader, exécutez la requête `SELECT` sur la table [system.replicas](/fr/operations/system-tables/replicas). Sinon, il est plus simple d’exécuter une requête `DETACH` sur toutes les répliques : toutes les répliques lèvent une exception, à l’exception des répliques leader (car plusieurs leaders sont autorisés).

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Supprime la partition spécifiée de la table. Cette requête marque la partition comme inactive et supprime définitivement les données au bout d’environ 10 minutes.

Pour en savoir plus sur la définition de l’expression de partition, consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression).

La requête est répliquée : elle supprime les données sur toutes les répliques.

Exemple :

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

Supprime la part spécifiée ou toutes les parts de la partition spécifiée de `detached`.
Pour en savoir plus sur la manière de définir l’expression de partition, consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression).

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

Supprime toutes les métadonnées relatives à une partition vide dans ZooKeeper. La requête échoue si la partition n’est pas vide ou si elle est inconnue. Veillez à n’exécuter cette opération que pour des partitions qui ne seront plus jamais utilisées.

Pour en savoir plus sur la définition de l’expression de partition, consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression).

Exemple :

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Ajoute des données à la table à partir du répertoire `detached`. Il est possible d’ajouter des données pour une partition entière ou pour une part individuelle. Exemples :

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Pour en savoir plus sur la définition de l’expression de partition, consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression).

Cette requête est répliquée. La réplique initiatrice vérifie s’il y a des données dans le répertoire `detached`.
Si des données sont présentes, la requête en vérifie l’intégrité. Si tout est correct, elle ajoute les données à la table.

Si la réplique non initiatrice, qui reçoit la commande ATTACH, trouve la part avec les sommes de contrôle correctes dans son propre répertoire `detached`, elle attache les données sans les récupérer depuis d’autres répliques.
S’il n’existe aucune part avec les sommes de contrôle correctes, les données sont téléchargées depuis n’importe quelle réplique qui possède cette part.

Vous pouvez placer des données dans le répertoire `detached` d’une réplique et utiliser la requête `ALTER ... ATTACH` pour les ajouter à la table sur toutes les répliques.

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

Cette requête copie la partition de données de `table1` vers `table2`.

Notez que :

* Les données ne seront supprimées ni de `table1` ni de `table2`.
* `table1` peut être une table temporaire.

Pour que la requête s’exécute correctement, les conditions suivantes doivent être remplies :

* Les deux tables doivent avoir la même structure.
* Les deux tables doivent avoir la même clé de partition, la même clé ORDER BY et la même clé primaire.
* Les deux tables doivent avoir la même politique de stockage.
* La table de destination doit inclure tous les indices et toutes les projections de la table source. Si le paramètre `enforce_index_structure_match_on_partition_manipulation` est activé dans la table de destination, les indices et les projections doivent être identiques. Sinon, la table de destination peut contenir un surensemble des indices et des projections de la table source.

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

Cette requête copie la partition de données de `table1` vers `table2` et remplace la partition existante dans `table2`. L’opération est atomique.

Notez que :

* Les données ne seront pas supprimées de `table1`.
* `table1` peut être une table temporaire.

Pour que la requête s’exécute correctement, les conditions suivantes doivent être remplies :

* Les deux tables doivent avoir la même structure.
* Les deux tables doivent avoir la même clé de partition, la même clé ORDER BY et la même clé primaire.
* Les deux tables doivent avoir la même politique de stockage.
* La table de destination doit inclure tous les indices et projections de la table source. Si le paramètre `enforce_index_structure_match_on_partition_manipulation` est activé dans la table de destination, les indices et projections doivent être identiques. Sinon, la table de destination peut avoir un surensemble des indices et projections de la table source.

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

Cette requête déplace la partition de données de `table_source` vers `table_dest` en supprimant les données de `table_source`.

Pour que la requête s’exécute correctement, les conditions suivantes doivent être remplies :

* Les deux tables doivent avoir la même structure.
* Les deux tables doivent avoir la même clé de partition, la même clé ORDER BY et la même clé primaire.
* Les deux tables doivent avoir la même politique de stockage.
* Les deux tables doivent appartenir à la même famille de moteurs (répliqués ou non répliqués).
* La table de destination doit inclure tous les indices et toutes les projections de la table source. Si le paramètre `enforce_index_structure_match_on_partition_manipulation` est activé dans la table de destination, les indices et les projections doivent être identiques. Sinon, la table de destination peut contenir un surensemble des indices et projections de la table source.

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Réinitialise toutes les valeurs de la colonne spécifiée dans une partition. Si la clause `DEFAULT` a été définie lors de la création de la table, cette requête attribue à la colonne la valeur par défaut spécifiée.

Exemple :

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

Cette requête crée une sauvegarde locale d’une partition spécifiée. Si la clause `PARTITION` est omise, la requête crée la sauvegarde de toutes les partitions en une seule fois.

:::note
L’intégralité du processus de sauvegarde est effectuée sans arrêter le serveur.
:::

Notez que, pour les tables à l’ancien format, vous pouvez spécifier le préfixe du nom de la partition (par exemple, `2019`) : la requête crée alors la sauvegarde de toutes les partitions correspondantes. Pour savoir comment définir l’expression de partition, consultez la section [Comment définir l’expression de partition](#how-to-set-partition-expression).

Au moment de l’exécution, pour créer un instantané des données, la requête crée des liens physiques vers les données de la table. Les liens physiques sont placés dans le répertoire `/var/lib/clickhouse/shadow/N/...`, où :

* `/var/lib/clickhouse/` est le répertoire de travail de ClickHouse spécifié dans la config.
* `N` est le numéro incrémental de la sauvegarde.
* si le paramètre `WITH NAME` est spécifié, la valeur du paramètre `'backup_name'` est utilisée à la place du numéro incrémental.

:::note
Si vous utilisez [un ensemble de disques pour le stockage des données d’une table](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes), le répertoire `shadow/N` apparaît sur chaque disque et stocke les parts de données correspondant à l’expression `PARTITION`.
:::

La même structure de répertoires est créée à l’intérieur de la sauvegarde que dans `/var/lib/clickhouse/`. La requête exécute `chmod` sur tous les fichiers afin d’en interdire l’écriture.

Après avoir créé la sauvegarde, vous pouvez copier les données depuis `/var/lib/clickhouse/shadow/` vers le serveur distant, puis les supprimer du serveur local. Notez que la requête `ALTER t FREEZE PARTITION` n’est pas répliquée. Elle crée une sauvegarde locale uniquement sur le serveur local.

La requête crée la sauvegarde presque instantanément (mais elle attend d’abord que les requêtes en cours sur la table correspondante soient terminées).

`ALTER TABLE t FREEZE PARTITION` copie uniquement les données, pas les métadonnées de la table. Pour sauvegarder les métadonnées de la table, copiez le fichier `/var/lib/clickhouse/metadata/database/table.sql`

Pour restaurer des données à partir d’une sauvegarde, procédez comme suit :

1. Créez la table si elle n’existe pas. Pour afficher la requête, utilisez le fichier .sql (remplacez `ATTACH` par `CREATE`).
2. Copiez les données du répertoire `data/database/table/` à l’intérieur de la sauvegarde vers le répertoire `/var/lib/clickhouse/data/database/table/detached/`.
3. Exécutez les requêtes `ALTER TABLE t ATTACH PARTITION` pour ajouter les données à la table.

La restauration à partir d’une sauvegarde ne nécessite pas l’arrêt du serveur.

La requête traite les parts en parallèle, le nombre de threads étant régulé par le paramètre `max_threads`.

Pour plus d’informations sur les sauvegardes et la restauration des données, consultez la section [&quot;Sauvegarde et restauration dans ClickHouse&quot;](/fr/operations/backup/overview).

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Supprime du disque les partitions `frozen` portant le nom indiqué. Si la clause `PARTITION` est omise, la requête supprime d’un seul coup la sauvegarde de toutes les partitions.

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

La requête fonctionne comme `CLEAR COLUMN`, mais elle réinitialise un index au lieu des données d’une colonne.

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Télécharge une partition depuis un autre serveur. Cette requête fonctionne uniquement pour les tables répliquées.

La requête effectue les opérations suivantes :

1. Télécharge la partition|part depuis le shard spécifié. Dans &#39;path-in-zookeeper&#39;, vous devez spécifier un chemin vers le shard dans ZooKeeper.
2. Ensuite, la requête place les données téléchargées dans le répertoire `detached` de la table `table_name`. Utilisez la requête [ATTACH PARTITION|PART](#attach-partitionpart) pour ajouter les données à la table.

Par exemple :

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Notez que :

* La requête `ALTER ... FETCH PARTITION|PART` n&#39;est pas répliquée. Elle place la part ou la partition dans le répertoire `detached`, uniquement sur le serveur local.
* La requête `ALTER TABLE ... ATTACH` est répliquée. Elle ajoute les données à toutes les répliques. Les données sont ajoutées à l&#39;une des répliques depuis le répertoire `detached`, et aux autres, depuis des répliques voisines.

Avant le téléchargement, le système vérifie que la partition existe et que la structure de la table correspond. La réplique la plus appropriée est automatiquement sélectionnée parmi les répliques saines.

Bien que la requête s&#39;appelle `ALTER TABLE`, elle ne modifie pas la structure de la table et ne change pas immédiatement les données disponibles dans celle-ci.

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

Déplace des partitions ou des parts de données vers un autre volume ou disque pour les tables utilisant le moteur `MergeTree`. Voir [Utiliser plusieurs périphériques de bloc pour le stockage des données](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

La requête `ALTER TABLE t MOVE` :

* N’est pas répliquée, car différentes répliques peuvent avoir des politiques de stockage différentes.
* Renvoie une erreur si le disque ou le volume spécifié n’est pas configuré. La requête renvoie également une erreur si les conditions de déplacement des données spécifiées dans la stratégie de stockage ne peuvent pas être appliquées.
* Peut renvoyer une erreur lorsque les données à déplacer ont déjà été déplacées par un processus en arrière-plan, une requête `ALTER TABLE t MOVE` concurrente ou à la suite d’une fusion de données en arrière-plan. L’utilisateur ne doit effectuer aucune action supplémentaire dans ce cas.

Exemple :

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

Modifie les données de la partition spécifiée qui correspondent à l’expression de filtrage indiquée. Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

Syntaxe :

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Exemple
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### Voir aussi
</div>

* [UPDATE](/fr/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

Supprime les données de la partition spécifiée qui correspondent à l’expression de filtrage indiquée. Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

Syntaxe :

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Exemple
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

Cela réécrira les parts à partir de zéro en appliquant tous les nouveaux paramètres. C’est logique, car les paramètres au niveau de la table, comme `use_const_adaptive_granularity`, ne sont par défaut appliqués qu’aux parts nouvellement écrites.

<div id="example">
  ### Exemple
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### Voir aussi
</div>

* [DELETE](/fr/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## Comment définir l’expression de partition
</div>

Vous pouvez spécifier l’expression de partition dans les requêtes `ALTER ... PARTITION` de différentes manières :

* À partir d’une valeur de la colonne `partition` de la table `system.parts`. Par exemple, `ALTER TABLE visits DETACH PARTITION 201901`.
* En utilisant le mot-clé `ALL`. Il peut être utilisé uniquement avec DROP/DETACH/ATTACH/ATTACH FROM. Par exemple, `ALTER TABLE visits ATTACH PARTITION ALL`.
* Sous la forme d’un tuple d’expressions ou de constantes correspondant (par les types) au tuple des clés de partitionnement de la table. Dans le cas d’une clé de partitionnement à un seul élément, l’expression doit être encapsulée dans la fonction `tuple (...)`. Par exemple, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
* En utilisant l’identifiant de partition. L’identifiant de partition est un identifiant sous forme de chaîne de la partition (lisible par l’humain, si possible), utilisé comme nom des partitions dans le système de fichiers et dans ZooKeeper. L’identifiant de partition doit être spécifié dans la clause `PARTITION ID`, entre guillemets simples. Par exemple, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
* Dans les requêtes [ALTER ATTACH PART](#attach-partitionpart) et [DROP DETACHED PART](#drop-detached-partitionpart), pour spécifier le nom d’une part, utilisez une chaîne littérale contenant une valeur de la colonne `name` de la table [system.detached&#95;parts](/fr/operations/system-tables/detached_parts). Par exemple, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

L’utilisation des guillemets lors de la spécification de la partition dépend du type de l’expression de partition. Par exemple, pour le type `String`, vous devez indiquer son nom entre guillemets (`'`). Pour les types `Date` et `Int*`, aucun guillemet n’est nécessaire.

Toutes les règles ci-dessus s’appliquent également à la requête [OPTIMIZE](/fr/sql-reference/statements/optimize.md). Si vous devez spécifier l’unique partition lors de l’optimisation d’une table non partitionnée, définissez l’expression `PARTITION tuple()`. Par exemple :

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` spécifie la partition à laquelle s’appliquent les expressions [UPDATE](/fr/sql-reference/statements/alter/update) ou [DELETE](/fr/sql-reference/statements/alter/delete) dans le cadre de la requête `ALTER TABLE`. De nouvelles parts sont créées uniquement à partir de la partition spécifiée. Ainsi, `IN PARTITION` permet de réduire la charge lorsque la table est divisée en un grand nombre de partitions et que vous devez mettre à jour les données uniquement point par point.

Des exemples de requêtes `ALTER ... PARTITION` sont présentés dans les tests [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) et [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).