---
description: 'Documentation sur Column'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: 'Manipulation des colonnes'
doc_type: 'reference'
---

Ensemble de requêtes permettant de modifier la structure d’une table.

Syntaxe :

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

Dans la requête, indiquez une liste d&#39;une ou plusieurs actions séparées par des virgules.
Chaque action correspond à une opération sur une colonne.

Les actions suivantes sont prises en charge :

* [ADD COLUMN](#add-column) — Ajoute une nouvelle colonne à la table.
* [DROP COLUMN](#drop-column) — Supprime la colonne.
* [RENAME COLUMN](#rename-column) — Renomme une colonne existante.
* [CLEAR COLUMN](#clear-column) — Réinitialise les valeurs de la colonne.
* [COMMENT COLUMN](#comment-column) — Ajoute un commentaire à la colonne.
* [MODIFY COLUMN](#modify-column) — Modifie le type de la colonne, l&#39;expression par défaut, le TTL et les paramètres de la colonne.
* [MODIFY COLUMN REMOVE](#modify-column-remove) — Supprime l&#39;une des propriétés de la colonne.
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - Modifie les paramètres de la colonne.
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - Réinitialise les paramètres de la colonne.
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - Ajoute de nouvelles valeurs à Enum.
* [MATERIALIZE COLUMN](#materialize-column) — Matérialise la colonne dans les parts de la table où elle est absente.
  Ces actions sont décrites en détail ci-dessous.

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Ajoute une nouvelle colonne à la table avec les `name`, `type`, [`codec`](../create/table.md/#column_compression_codec) et `default_expr` spécifiés (voir la section [Expressions par défaut](/fr/sql-reference/statements/create/table#default_values)).

Si la clause `IF NOT EXISTS` est incluse, la requête ne renverra pas d’erreur si la colonne existe déjà. Si vous spécifiez `AFTER name_after` (le nom d’une autre colonne), la colonne est ajoutée après celle-ci dans la liste des colonnes de la table. Si vous souhaitez ajouter une colonne au début de la table, utilisez la clause `FIRST`. Sinon, la colonne est ajoutée à la fin de la table. Pour une chaîne d’actions, `name_after` peut être le nom d’une colonne ajoutée dans l’une des actions précédentes.

L’ajout d’une colonne modifie uniquement la structure de la table, sans effectuer d’opération sur les données. Les données n’apparaissent pas sur le disque après `ALTER`. Si des données sont absentes pour une colonne lors de la lecture de la table, elles sont remplacées par des valeurs par défaut (en évaluant l’expression par défaut s’il y en a une, ou en utilisant des zéros ou des chaînes vides). La colonne apparaît sur le disque après la fusion des parts de la table (voir [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree.md)).

Cette approche permet d’exécuter la requête `ALTER` instantanément, sans augmenter le volume des anciennes données.

Exemple :

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

Supprime la colonne nommée `name`. Si la clause `IF EXISTS` est spécifiée, la requête ne renverra pas d’erreur si la colonne n’existe pas.

Supprime les données du système de fichiers. Comme cela supprime des fichiers entiers, la requête s’exécute presque instantanément.

:::tip
Vous ne pouvez pas supprimer une colonne si elle est référencée par une [vue matérialisée](/fr/sql-reference/statements/create/view). Sinon, cela renvoie une erreur.
:::

Exemple :

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Renomme la colonne `name` en `new_name`. Si la clause `IF EXISTS` est spécifiée, la requête ne renverra pas d’erreur si la colonne n’existe pas. Comme ce renommage n’affecte pas les données elles-mêmes, la requête s’exécute presque instantanément.

**NOTE** : Les colonnes spécifiées dans l’expression de clé de la table (avec `ORDER BY` ou `PRIMARY KEY`) ne peuvent pas être renommées. Toute tentative de modifier ces colonnes entraînera `SQL Error [524]`.

Exemple :

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Réinitialise toutes les données d’une colonne pour la partition spécifiée. Pour en savoir plus sur la manière de définir le nom de la partition, consultez la section [Comment définir l’expression de partition](../alter/partition.md/#how-to-set-partition-expression).

Si la clause `IF EXISTS` est spécifiée, la requête ne renverra pas d’erreur si la colonne n’existe pas.

Exemple :

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Ajoute un commentaire à la colonne. Si la clause `IF EXISTS` est spécifiée, la requête ne renvoie pas d’erreur si la colonne n’existe pas.

Chaque colonne peut avoir un commentaire. Si un commentaire existe déjà pour la colonne, le nouveau commentaire remplace le précédent.

Les commentaires sont stockés dans la colonne `comment_expression` renvoyée par la requête [DESCRIBE TABLE](/fr/sql-reference/statements/describe-table.md).

Exemple :

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

Cette requête modifie les propriétés de la colonne `name` :

* Type

* Expression par défaut

* Codec de compression

* TTL

* Paramètres de colonne

* Valeurs d’énumération pour les types Enum/Enum8/Enum16

Pour des exemples de modification des codecs de compression des colonnes, voir [Codecs de compression des colonnes](../create/table.md/#column_compression_codec).

Pour des exemples de modification du TTL des colonnes, voir [TTL de colonne](/fr/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl).

Pour des exemples de modification des paramètres de colonne, voir [Paramètres de colonne](/fr/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings).

Si la clause `IF EXISTS` est spécifiée, la requête ne renverra pas d’erreur si la colonne n’existe pas.

Lors d’un changement de type, les valeurs sont converties comme si les fonctions [toType](/fr/sql-reference/functions/type-conversion-functions.md) leur étaient appliquées. Si seule l’expression par défaut est modifiée, la requête n’effectue aucune opération complexe et s’exécute presque instantanément.

Exemple :

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

La modification du type de colonne est la seule opération complexe : elle modifie le contenu des fichiers de données. Pour les grandes tables, cela peut prendre beaucoup de temps.

La requête peut également modifier l’ordre des colonnes à l’aide de la clause `FIRST | AFTER` ; voir la description de [ADD COLUMN](#add-column), mais dans ce cas, le type de colonne est obligatoire.

Exemple :

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

La requête `ALTER` est atomique. Pour les tables MergeTree, elle s’exécute également sans verrouillage.

La requête `ALTER` de modification des colonnes est répliquée. Les instructions sont enregistrées dans ZooKeeper, puis chaque réplique les applique. Toutes les requêtes `ALTER` sont exécutées dans le même ordre. La requête attend que les opérations correspondantes soient terminées sur les autres répliques. Cependant, une requête de modification de colonnes dans une table répliquée peut être interrompue, et toutes les opérations seront effectuées de manière asynchrone.

:::note
Soyez prudent lorsque vous modifiez une colonne Nullable en Non-Nullable. Assurez-vous qu’elle ne contient aucune valeur NULL, sinon cela entraînera des problèmes à la lecture. Dans ce cas, la solution de contournement consiste à arrêter la mutation et à remettre la colonne au type Nullable.
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

Supprime l’une des propriétés d’une colonne : `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS`.

Syntaxe :

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**Exemple**

Supprimer le TTL :

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**Voir aussi**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

Modifie un paramètre de colonne.

Syntaxe :

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**Exemple**

Modifiez la valeur de `max_compress_block_size` de la colonne à `1MB` :

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

Réinitialise un paramètre de colonne et supprime également sa déclaration dans l’expression de colonne de la requête CREATE de la table.

Syntaxe :

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**Exemple**

Réinitialiser le paramètre de colonne `max_compress_block_size` à sa valeur par défaut :

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

Ajoute de nouvelles valeurs à une colonne de type `Enum`, `Enum8`, `Enum16`, `Nullable(Enum)`, `Nullable(Enum8)` ou `Nullable(Enum16)`

Syntaxe :

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**Exemple**

Ajoutez deux valeurs à la colonne `enum_column_name` :

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

Matérialise une colonne avec une expression de valeur `DEFAULT` ou `MATERIALIZED`. Lors de l’ajout d’une colonne matérialisée à l’aide de `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`, les lignes existantes sans valeurs matérialisées ne sont pas automatiquement renseignées. L’instruction `MATERIALIZE COLUMN` peut être utilisée pour réécrire les données existantes d’une colonne après l’ajout ou la mise à jour d’une expression `DEFAULT` ou `MATERIALIZED` (ce qui met à jour uniquement les métadonnées, sans modifier les données existantes). Notez que matérialiser une colonne dans la clé de tri n’est pas une opération valide, car cela pourrait rompre l’ordre de tri.
Implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

Pour les colonnes avec une expression de valeur `MATERIALIZED` nouvelle ou mise à jour, toutes les lignes existantes sont réécrites.

Pour les colonnes avec une expression de valeur `DEFAULT` nouvelle ou mise à jour, le comportement dépend de la version de ClickHouse :

* Dans ClickHouse &lt; v24.2, toutes les lignes existantes sont réécrites.
* ClickHouse &gt;= v24.2 fait la distinction entre une valeur de ligne dans une colonne avec une expression de valeur `DEFAULT` explicitement spécifiée lors de l’insertion, et une valeur calculée à partir de l’expression de valeur `DEFAULT`. Si la valeur a été explicitement spécifiée, ClickHouse la conserve telle quelle. Si la valeur a été calculée, ClickHouse la remplace par la nouvelle expression de valeur `MATERIALIZED` ou sa version mise à jour.

Syntaxe :

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* Si vous spécifiez une PARTITION, une colonne sera matérialisée uniquement pour la PARTITION spécifiée.

**Exemple**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**Voir aussi**

* [MATERIALIZED](/fr/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## Limitations
</div>

La requête `ALTER` vous permet de créer et de supprimer des éléments distincts (colonnes) dans des structures de données imbriquées, mais pas des structures de données imbriquées entières. Pour ajouter une structure de données imbriquée, vous pouvez ajouter des colonnes portant un nom comme `name.nested_name` et le type `Array(T)`. Une structure de données imbriquée équivaut à plusieurs colonnes de type tableau dont le nom partage le même préfixe avant le point.

Le renommage des colonnes contenant des points dans leur nom est partiellement pris en charge. Les points sont réservés à l&#39;accès aux sous-colonnes [Nested](/fr/sql-reference/data-types/nested-data-structures/nested), donc le préfixe (nom parent) doit rester identique. Seul le suffixe (nom de la sous-colonne) peut être modifié. Par exemple, `a.b` peut être renommé en `a.c`, mais renommer `a.b` en `b.d` n&#39;est pas autorisé, car cela modifie le préfixe parent de Nested.

La suppression de colonnes dans la clé primaire ou la clé d&#39;échantillonnage (colonnes utilisées dans l&#39;expression `ENGINE`) n&#39;est pas prise en charge. La modification du type des colonnes incluses dans la clé primaire n&#39;est possible que si ce changement n&#39;entraîne pas de modification des données (par exemple, vous pouvez ajouter des valeurs à un Enum ou faire passer un type de `DateTime` à `UInt32`).

Si la requête `ALTER` ne suffit pas pour effectuer les modifications de table dont vous avez besoin, vous pouvez créer une nouvelle table, y copier les données à l&#39;aide de la requête [INSERT SELECT](/fr/sql-reference/statements/insert-into.md/#inserting-the-results-of-select), puis permuter les tables à l&#39;aide de la requête [RENAME](/fr/sql-reference/statements/rename.md/#rename-table) et supprimer l&#39;ancienne table.

La requête `ALTER` bloque toutes les lectures et écritures sur la table. Autrement dit, si un `SELECT` long est en cours au moment où la requête `ALTER` est lancée, la requête `ALTER` attendra qu&#39;il se termine. En parallèle, toutes les nouvelles requêtes sur la même table resteront en attente pendant l&#39;exécution de cet `ALTER`.

Pour les tables qui ne stockent pas elles-mêmes de données (comme [Merge](/fr/sql-reference/statements/alter/index.md) et [Distributed](/fr/sql-reference/statements/alter/index.md)), `ALTER` modifie uniquement la structure de la table et ne modifie pas celle des tables sous-jacentes. Par exemple, lorsque vous exécutez ALTER sur une table `Distributed`, vous devez également exécuter `ALTER` sur les tables de tous les serveurs distants.