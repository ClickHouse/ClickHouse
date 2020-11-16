---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

Le `ALTER` la requête est prise en charge uniquement pour `*MergeTree` des tables, ainsi que `Merge`et`Distributed`. La requête a plusieurs variantes.

### Manipulations De Colonne {#column-manipulations}

Modification de la structure de la table.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

Dans la requête, spécifiez une liste d'une ou plusieurs actions séparées par des virgules.
Chaque action est une opération sur une colonne.

Les actions suivantes sont prises en charge:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column's type, default expression and TTL.

Ces actions sont décrites en détail ci-dessous.

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

Ajoute une nouvelle colonne à la table spécifiée `name`, `type`, [`codec`](create.md#codecs) et `default_expr` (voir la section [Expressions par défaut](create.md#create-default-values)).

Si l' `IF NOT EXISTS` la clause est incluse, la requête ne retournera pas d'erreur si la colonne existe déjà. Si vous spécifiez `AFTER name_after` (le nom d'une autre colonne), la colonne est ajoutée après celle spécifiée dans la liste des colonnes de la table. Sinon, la colonne est ajoutée à la fin de la table. Notez qu'il n'existe aucun moyen d'ajouter une colonne au début d'un tableau. Pour une chaîne d'actions, `name_after` peut être le nom d'une colonne est ajoutée dans l'une des actions précédentes.

L'ajout d'une colonne modifie simplement la structure de la table, sans effectuer d'actions avec des données. Les données n'apparaissent pas sur le disque après la `ALTER`. Si les données sont manquantes pour une colonne lors de la lecture de la table, elles sont remplies avec des valeurs par défaut (en exécutant l'expression par défaut s'il y en a une, ou en utilisant des zéros ou des chaînes vides). La colonne apparaît sur le disque après la fusion des parties de données (voir [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)).

Cette approche nous permet de compléter le `ALTER` requête instantanément, sans augmenter le volume de données anciennes.

Exemple:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Supprime la colonne avec le nom `name`. Si l' `IF EXISTS` la clause est spécifiée, la requête ne retournera pas d'erreur si la colonne n'existe pas.

Supprime les données du système de fichiers. Comme cela supprime des fichiers entiers, la requête est terminée presque instantanément.

Exemple:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Réinitialise toutes les données dans une colonne pour une partition spécifiée. En savoir plus sur la définition du nom de la partition dans la section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

Si l' `IF EXISTS` la clause est spécifiée, la requête ne retournera pas d'erreur si la colonne n'existe pas.

Exemple:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

Ajoute un commentaire à la colonne. Si l' `IF EXISTS` la clause est spécifiée, la requête ne retournera pas d'erreur si la colonne n'existe pas.

Chaque colonne peut avoir un commentaire. Si un commentaire existe déjà pour la colonne, un nouveau commentaire remplace le précédent commentaire.

Les commentaires sont stockés dans le `comment_expression` colonne renvoyée par le [DESCRIBE TABLE](misc.md#misc-describe-table) requête.

Exemple:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

Cette requête modifie le `name` les propriétés de la colonne:

-   Type

-   Expression par défaut

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

Si l' `IF EXISTS` la clause est spécifiée, la requête ne retournera pas d'erreur si la colonne n'existe pas.

Lors de la modification du type, les valeurs sont converties comme si [toType](../../sql-reference/functions/type-conversion-functions.md) les fonctions ont été appliquées. Si seule l'expression par défaut est modifiée, la requête ne fait rien de complexe et est terminée presque instantanément.

Exemple:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

Il y a plusieurs étapes de traitement:

-   Préparation de (nouveaux) fichiers temporaires avec des données modifiées.
-   Renommer les anciens fichiers.
-   Renommer les (nouveaux) fichiers temporaires en anciens noms.
-   Suppression des anciens fichiers.

Seule la première étape prend du temps. Si il y a un échec à ce stade, les données ne sont pas modifiées.
En cas d'échec au cours d'une des étapes successives, les données peuvent être restaurées manuellement. L'exception est si les anciens fichiers ont été supprimés du système de fichiers mais que les données des nouveaux fichiers n'ont pas été écrites sur le disque et ont été perdues.

Le `ALTER` la requête de modification des colonnes est répliquée. Les instructions sont enregistrées dans ZooKeeper, puis chaque réplique les applique. Tout `ALTER` les requêtes sont exécutées dans le même ordre. La requête attend que les actions appropriées soient terminées sur les autres répliques. Cependant, une requête pour modifier des colonnes dans une table répliquée peut être interrompue, et toutes les actions seront effectuées de manière asynchrone.

#### Modifier les limites de la requête {#alter-query-limitations}

Le `ALTER` query vous permet de créer et de supprimer des éléments distincts (colonnes) dans des structures de données imbriquées, mais pas des structures de données imbriquées entières. Pour ajouter une structure de données imbriquée, vous pouvez ajouter des colonnes avec un nom comme `name.nested_name` et le type `Array(T)`. Une structure de données imbriquée est équivalente à plusieurs colonnes de tableau avec un nom qui a le même préfixe avant le point.

Il n'y a pas de support pour supprimer des colonnes dans la clé primaire ou la clé d'échantillonnage (colonnes qui sont utilisées dans le `ENGINE` expression). La modification du type des colonnes incluses dans la clé primaire n'est possible que si cette modification n'entraîne pas la modification des données (par exemple, vous êtes autorisé à ajouter des valeurs à une énumération ou à modifier un type de `DateTime` de `UInt32`).

Si l' `ALTER` la requête n'est pas suffisante pour apporter les modifications de table dont vous avez besoin, vous pouvez créer une nouvelle table, y copier les données en utilisant le [INSERT SELECT](insert-into.md#insert_query_insert-select) requête, puis changer les tables en utilisant le [RENAME](misc.md#misc_operations-rename) requête et supprimer l'ancienne table. Vous pouvez utiliser l' [clickhouse-copieur](../../operations/utilities/clickhouse-copier.md) comme une alternative à la `INSERT SELECT` requête.

Le `ALTER` query bloque toutes les lectures et écritures pour la table. En d'autres termes, si une longue `SELECT` est en cours d'exécution au moment de la `ALTER` requête, la `ALTER` la requête va attendre qu'elle se termine. Dans le même temps, toutes les nouvelles requêtes à la même table attendre que ce `ALTER` est en cours d'exécution.

Pour les tables qui ne stockent pas les données elles-mêmes (telles que `Merge` et `Distributed`), `ALTER` change simplement la structure de la table, et ne change pas la structure des tables subordonnées. Par exemple, lors de L'exécution de ALTER pour un `Distributed` table, vous devrez également exécuter `ALTER` pour les tables sur tous les serveurs distants.

### Manipulations avec des Expressions clés {#manipulations-with-key-expressions}

La commande suivante est prise en charge:

``` sql
MODIFY ORDER BY new_expression
```

Cela ne fonctionne que pour les tables du [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) de la famille (y compris les
[répliqué](../../engines/table-engines/mergetree-family/replication.md) table). La commande change l'
[clé de tri](../../engines/table-engines/mergetree-family/mergetree.md) de la table
de `new_expression` (une expression ou un tuple d'expressions). Clé primaire reste le même.

La commande est légère en ce sens qu'elle ne modifie que les métadonnées. Pour conserver la propriété cette partie de données
les lignes sont ordonnées par l'expression de clé de tri vous ne pouvez pas ajouter d'expressions contenant des colonnes existantes
à la clé de tri (seules les colonnes ajoutées par `ADD COLUMN` commande dans le même `ALTER` requête).

### Manipulations avec des Indices de saut de données {#manipulations-with-data-skipping-indices}

Cela ne fonctionne que pour les tables du [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) de la famille (y compris les
[répliqué](../../engines/table-engines/mergetree-family/replication.md) table). Les opérations suivantes
sont disponibles:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Ajoute la description de l'index aux métadonnées des tables.

-   `ALTER TABLE [db].name DROP INDEX name` - Supprime la description de l'index des métadonnées des tables et supprime les fichiers d'index du disque.

Ces commandes sont légères dans le sens où elles ne modifient que les métadonnées ou suppriment des fichiers.
En outre, ils sont répliqués (synchronisation des métadonnées des indices via ZooKeeper).

### Manipulations avec contraintes {#manipulations-with-constraints}

En voir plus sur [contraintes](create.md#constraints)

Les contraintes peuvent être ajoutées ou supprimées à l'aide de la syntaxe suivante:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Les requêtes ajouteront ou supprimeront des métadonnées sur les contraintes de la table afin qu'elles soient traitées immédiatement.

Contrainte de vérifier *ne sera pas exécuté* sur les données existantes si elle a été ajoutée.

Toutes les modifications sur les tables répliquées sont diffusées sur ZooKeeper et seront donc appliquées sur d'autres répliques.

### Manipulations avec des Partitions et des pièces {#alter_manipulations-with-partitions}

Les opérations suivantes avec [partition](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) sont disponibles:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` répertoire et de l'oublier.
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` répertoire à la table.
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) - Copie la partition de données d'une table à l'autre et la remplace.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition)(#alter_move_to_table-partition) - déplace la partition de données d'une table à l'autre.
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) - Rétablit la valeur d'une colonne spécifiée dans une partition.
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) - Réinitialise l'index secondaire spécifié dans une partition.
-   [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) – Move partition/data part to another disk or volume.

<!-- -->

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Déplace toutes les données de la partition spécifiée vers `detached` répertoire. Le serveur oublie la partition de données détachée comme si elle n'existait pas. Le serveur ne connaîtra pas ces données tant que vous n'aurez pas [ATTACH](#alter_attach-partition) requête.

Exemple:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

Lisez à propos de la définition de l'expression de partition dans une section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

Une fois la requête exécutée, vous pouvez faire ce que vous voulez avec les données du `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` répertoire sur toutes les répliques. Notez que vous ne pouvez exécuter cette requête que sur un réplica leader. Pour savoir si une réplique est un leader, effectuez le `SELECT` requête à l' [système.réplique](../../operations/system-tables.md#system_tables-replicas) table. Alternativement, il est plus facile de faire une `DETACH` requête sur toutes les répliques - toutes les répliques lancent une exception, à l'exception de la réplique leader.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Supprime la partition spécifiée de la table. Cette requête marque la partition comme inactive et supprime complètement les données, environ en 10 minutes.

Lisez à propos de la définition de l'expression de partition dans une section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Supprime la partie spécifiée ou toutes les parties de la partition spécifiée de `detached`.
En savoir plus sur la définition de l'expression de partition dans une section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Ajoute des données à la table à partir du `detached` répertoire. Il est possible d'ajouter des données dans une partition entière ou pour une partie distincte. Exemple:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

En savoir plus sur la définition de l'expression de partition dans une section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

Cette requête est répliquée. L'initiateur de réplica vérifie s'il y a des données dans le `detached` répertoire. Si des données existent, la requête vérifie son intégrité. Si tout est correct, la requête ajoute les données à la table. Tous les autres réplicas téléchargent les données de l'initiateur de réplica.

Ainsi, vous pouvez mettre des données à la `detached` répertoire sur une réplique, et utilisez le `ALTER ... ATTACH` requête pour l'ajouter à la table sur tous les réplicas.

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

Cette requête copie la partition de données du `table1` de `table2` ajoute des données de gratuit dans la `table2`. Notez que les données ne seront pas supprimées de `table1`.

Pour que la requête s'exécute correctement, les conditions suivantes doivent être remplies:

-   Les deux tables doivent avoir la même structure.
-   Les deux tables doivent avoir la même clé de partition.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

Cette requête copie la partition de données du `table1` de `table2` et remplace la partition existante dans le `table2`. Notez que les données ne seront pas supprimées de `table1`.

Pour que la requête s'exécute correctement, les conditions suivantes doivent être remplies:

-   Les deux tables doivent avoir la même structure.
-   Les deux tables doivent avoir la même clé de partition.

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

Cette requête déplace la partition de données du `table_source` de `table_dest` avec la suppression des données de `table_source`.

Pour que la requête s'exécute correctement, les conditions suivantes doivent être remplies:

-   Les deux tables doivent avoir la même structure.
-   Les deux tables doivent avoir la même clé de partition.
-   Les deux tables doivent appartenir à la même famille de moteurs. (répliqué ou non répliqué)
-   Les deux tables doivent avoir la même stratégie de stockage.

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Réinitialise toutes les valeurs de la colonne spécifiée dans une partition. Si l' `DEFAULT` la clause a été déterminée lors de la création d'une table, cette requête définit la valeur de la colonne à une valeur par défaut spécifiée.

Exemple:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

Cette requête crée une sauvegarde locale d'une partition spécifiée. Si l' `PARTITION` la clause est omise, la requête crée la sauvegarde de toutes les partitions à la fois.

!!! note "Note"
    L'ensemble du processus de sauvegarde est effectuée sans arrêter le serveur.

Notez que pour les tables de style ancien, vous pouvez spécifier le préfixe du nom de la partition (par exemple, ‘2019’)- ensuite, la requête crée la sauvegarde pour toutes les partitions correspondantes. Lisez à propos de la définition de l'expression de partition dans une section [Comment spécifier l'expression de partition](#alter-how-to-specify-part-expr).

Au moment de l'exécution, pour un instantané de données, la requête crée des liens rigides vers des données de table. Les liens sont placés dans le répertoire `/var/lib/clickhouse/shadow/N/...`, où:

-   `/var/lib/clickhouse/` est le répertoire de travail clickhouse spécifié dans la configuration.
-   `N` est le numéro incrémental de la sauvegarde.

!!! note "Note"
    Si vous utilisez [un ensemble de disques pour le stockage des données dans une table](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes), le `shadow/N` le répertoire apparaît sur chaque disque, stockant les parties de données correspondant `PARTITION` expression.

La même structure de répertoires est créée à l'intérieur de la sauvegarde qu'à l'intérieur `/var/lib/clickhouse/`. La requête effectue ‘chmod’ pour tous les fichiers, interdisant d'écrire en eux.

Après avoir créé la sauvegarde, vous pouvez copier les données depuis `/var/lib/clickhouse/shadow/` sur le serveur distant, puis supprimez-le du serveur local. Notez que l' `ALTER t FREEZE PARTITION` la requête n'est pas répliqué. Il crée une sauvegarde locale uniquement sur le serveur local.

La requête crée une sauvegarde presque instantanément (mais elle attend d'abord que les requêtes en cours à la table correspondante se terminent).

`ALTER TABLE t FREEZE PARTITION` copie uniquement les données, pas les métadonnées de la table. Faire une sauvegarde des métadonnées de la table, copiez le fichier `/var/lib/clickhouse/metadata/database/table.sql`

Pour restaurer des données à partir d'une sauvegarde, procédez comme suit:

1.  Créer la table si elle n'existe pas. Pour afficher la requête, utilisez la .fichier sql (remplacer `ATTACH` avec `CREATE`).
2.  Copier les données de la `data/database/table/` répertoire à l'intérieur de la sauvegarde `/var/lib/clickhouse/data/database/table/detached/` répertoire.
3.  Exécuter `ALTER TABLE t ATTACH PARTITION` les requêtes pour ajouter les données à une table.

La restauration à partir d'une sauvegarde ne nécessite pas l'arrêt du serveur.

Pour plus d'informations sur les sauvegardes et la restauration [La Sauvegarde Des Données](../../operations/backup.md) section.

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

La requête fonctionne de manière similaire à `CLEAR COLUMN` mais il remet un index au lieu d'une colonne de données.

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Télécharge une partition depuis un autre serveur. Cette requête ne fonctionne que pour les tables répliquées.

La requête effectue les opérations suivantes:

1.  Télécharge la partition à partir du fragment spécifié. Dans ‘path-in-zookeeper’ vous devez spécifier un chemin vers le fragment dans ZooKeeper.
2.  Ensuite, la requête met les données téléchargées dans le `detached` répertoire de la `table_name` table. L'utilisation de la [ATTACH PARTITION\|PART](#alter_attach-partition) requête pour ajouter les données à la table.

Exemple:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Notez que:

-   Le `ALTER ... FETCH PARTITION` la requête n'est pas répliqué. Il place la partition à la `detached` répertoire sur le serveur local.
-   Le `ALTER TABLE ... ATTACH` la requête est répliquée. Il ajoute les données à toutes les répliques. Les données sont ajoutées à l'une des répliques `detached` répertoire, et aux autres-des répliques voisines.

Avant le téléchargement, le système vérifie si la partition existe et si la structure de la table correspond. La réplique la plus appropriée est sélectionnée automatiquement parmi les répliques saines.

Bien que la requête soit appelée `ALTER TABLE`, il ne modifie pas la structure de la table et ne modifie pas immédiatement les données disponibles dans la table.

#### MOVE PARTITION\|PART {#alter_move-partition}

Déplace des partitions ou des parties de données vers un autre volume ou disque pour `MergeTree`-tables de moteur. Voir [Utilisation de plusieurs périphériques de bloc pour le stockage de données](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

Le `ALTER TABLE t MOVE` requête:

-   Non répliqué, car différentes répliques peuvent avoir des stratégies de stockage différentes.
-   Renvoie une erreur si le disque ou le volume n'est pas configuré. Query renvoie également une erreur si les conditions de déplacement des données, spécifiées dans la stratégie de stockage, ne peuvent pas être appliquées.
-   Peut renvoyer une erreur dans le cas, lorsque les données à déplacer sont déjà déplacées par un processus en arrière-plan, simultané `ALTER TABLE t MOVE` requête ou à la suite de la fusion de données d'arrière-plan. Un utilisateur ne doit effectuer aucune action supplémentaire dans ce cas.

Exemple:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### Comment définir L'Expression de la Partition {#alter-how-to-specify-part-expr}

Vous pouvez spécifier l'expression de partition dans `ALTER ... PARTITION` requêtes de différentes manières:

-   Comme une valeur de l' `partition` la colonne de la `system.parts` table. Exemple, `ALTER TABLE visits DETACH PARTITION 201901`.
-   Comme expression de la colonne de la table. Les constantes et les expressions constantes sont prises en charge. Exemple, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   À l'aide de l'ID de partition. Partition ID est un identifiant de chaîne de la partition (lisible par l'homme, si possible) qui est utilisé comme noms de partitions dans le système de fichiers et dans ZooKeeper. L'ID de partition doit être spécifié dans `PARTITION ID` clause, entre guillemets simples. Exemple, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   Dans le [ALTER ATTACH PART](#alter_attach-partition) et [DROP DETACHED PART](#alter_drop-detached) requête, pour spécifier le nom d'une partie, utilisez le littéral de chaîne avec une valeur de `name` la colonne de la [système.detached_parts](../../operations/system-tables.md#system_tables-detached_parts) table. Exemple, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

L'utilisation de guillemets lors de la spécification de la partition dépend du type d'expression de partition. Par exemple, pour la `String` type, vous devez spécifier son nom entre guillemets (`'`). Pour l' `Date` et `Int*` types aucune citation n'est nécessaire.

Pour les tables de style ancien, vous pouvez spécifier la partition sous forme de nombre `201901` ou une chaîne de caractères `'201901'`. La syntaxe des tables new-style est plus stricte avec les types (similaire à l'analyseur pour le format D'entrée des valeurs).

Toutes les règles ci-dessus sont aussi valables pour la [OPTIMIZE](misc.md#misc_operations-optimize) requête. Si vous devez spécifier la seule partition lors de l'optimisation d'une table non partitionnée, définissez l'expression `PARTITION tuple()`. Exemple:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

Les exemples de `ALTER ... PARTITION` les requêtes sont démontrées dans les tests [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) et [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Manipulations avec Table TTL {#manipulations-with-table-ttl}

Vous pouvez modifier [tableau TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) avec une demande du formulaire suivant:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### Synchronicité des requêtes ALTER {#synchronicity-of-alter-queries}

Pour les tables non réplicables, tous `ALTER` les requêtes sont exécutées simultanément. Pour les tables réplicables, la requête ajoute simplement des instructions pour les actions appropriées à `ZooKeeper` et les actions elles-mêmes sont effectuées dès que possible. Cependant, la requête peut attendre que ces actions soient terminées sur tous les réplicas.

Pour `ALTER ... ATTACH|DETACH|DROP` les requêtes, vous pouvez utiliser le `replication_alter_partitions_sync` configuration pour configurer l'attente.
Valeurs possibles: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### Mutation {#alter-mutations}

Les Mutations sont une variante ALTER query qui permet de modifier ou de supprimer des lignes dans une table. Contrairement à la norme `UPDATE` et `DELETE` les requêtes qui sont destinées aux changements de données de point, les mutations sont destinées aux opérations lourdes qui modifient beaucoup de lignes dans une table. Pris en charge pour le `MergeTree` famille de moteurs de table, y compris les moteurs avec support de réplication.

Les tables existantes sont prêtes pour les mutations telles quelles (aucune conversion nécessaire), mais après l'application de la première mutation à une table, son format de métadonnées devient incompatible avec les versions précédentes du serveur et il devient impossible de revenir à une version précédente.

Commandes actuellement disponibles:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

Le `filter_expr` doit être de type `UInt8`. La requête supprime les lignes de la table pour lesquelles cette expression prend une valeur différente de zéro.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

Le `filter_expr` doit être de type `UInt8`. Cette requête met à jour les valeurs des colonnes spécifiées en les valeurs des expressions correspondantes dans les lignes pour lesquelles `filter_expr` prend une valeur non nulle. Les valeurs sont converties en type de colonne à l'aide `CAST` opérateur. La mise à jour des colonnes utilisées dans le calcul de la clé primaire ou de la clé de partition n'est pas prise en charge.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

La requête reconstruit l'index secondaire `name` dans la partition `partition_name`.

Une requête peut contenir plusieurs commandes séparées par des virgules.

Pour les tables \* MergeTree, les mutations s'exécutent en réécrivant des parties de données entières. Il n'y a pas d'atomicité-les pièces sont substituées aux pièces mutées dès qu'elles sont prêtes et un `SELECT` la requête qui a commencé à s'exécuter pendant une mutation verra les données des parties qui ont déjà été mutées ainsi que les données des parties qui n'ont pas encore été mutées.

Les Mutations sont totalement ordonnées par leur ordre de création et sont appliquées à chaque partie dans cet ordre. Les Mutations sont également partiellement ordonnées avec des insertions - les données insérées dans la table avant la soumission de la mutation seront mutées et les données insérées après ne seront pas mutées. Notez que les mutations ne bloquent en aucune façon les INSERTs.

Une requête de mutation retourne immédiatement après l'ajout de l'entrée de mutation (dans le cas de tables répliquées à ZooKeeper, pour les tables non compliquées - au système de fichiers). La mutation elle-même s'exécute de manière asynchrone en utilisant les paramètres du profil système. Pour suivre l'avancement des mutations vous pouvez utiliser la [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) table. Une mutation qui a été soumise avec succès continuera à s'exécuter même si les serveurs ClickHouse sont redémarrés. Il n'y a aucun moyen de faire reculer la mutation une fois qu'elle est soumise, mais si la mutation est bloquée pour une raison quelconque, elle peut être annulée avec le [`KILL MUTATION`](misc.md#kill-mutation) requête.

Les entrées pour les mutations finies ne sont pas supprimées immédiatement (le nombre d'entrées conservées est déterminé par `finished_mutations_to_keep` le moteur de stockage de paramètre). Les anciennes entrées de mutation sont supprimées.

## ALTER USER {#alter-user-statement}

Changements clickhouse comptes d'utilisateurs.

### Syntaxe {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Description {#alter-user-dscr}

Utiliser `ALTER USER` vous devez avoir le [ALTER USER](grant.md#grant-access-management) privilège.

### Exemple {#alter-user-examples}

Définir les rôles accordés par défaut:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

Si les rôles ne sont pas précédemment accordés à un utilisateur, ClickHouse lève une exception.

Définissez tous les rôles accordés à défaut:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Si un rôle seront accordés à un utilisateur dans l'avenir, il deviendra automatiquement par défaut.

Définissez tous les rôles accordés sur default excepting `role1` et `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

## ALTER ROLE {#alter-role-statement}

Les changements de rôles.

### Syntaxe {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## ALTER ROW POLICY {#alter-row-policy-statement}

Modifie la stratégie de ligne.

### Syntaxe {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER QUOTA {#alter-quota-statement}

Les changements de quotas.

### Syntaxe {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Les changements de quotas.

### Syntaxe {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[Article Original](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
