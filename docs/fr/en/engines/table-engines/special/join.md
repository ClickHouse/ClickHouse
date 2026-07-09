---
description: 'Structure de données préparée facultative à utiliser dans les opérations JOIN.'
sidebar_label: 'Join'
sidebar_position: 70
slug: /engines/table-engines/special/join
title: 'Moteur de table Join'
doc_type: 'reference'
---

Structure de données préparée facultative à utiliser dans les opérations [JOIN](/fr/sql-reference/statements/select/join).

:::note
Dans ClickHouse Cloud, si votre service a été créé avec une version antérieure à 25.4, vous devrez régler la compatibilité sur 25.4 au minimum à l’aide de `SET compatibility=25.4`.
:::

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

Consultez la description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

<div id="engine-parameters">
  ## Paramètres du moteur
</div>

<div id="join_strictness">
  ### `join_strictness`
</div>

`join_strictness` – [mode de correspondance de JOIN](/fr/sql-reference/statements/select/join#supported-types-of-join).

<div id="join_type">
  ### `join_type`
</div>

`join_type` – [type de JOIN](/fr/sql-reference/statements/select/join#supported-types-of-join).

<div id="key-columns">
  ### Colonnes clés
</div>

`k1[, k2, ...]` – Colonnes clés de la clause `USING` sur lesquelles s’appuie l’opération `JOIN`.

Saisissez les paramètres `join_strictness` et `join_type` sans guillemets, par exemple `Join(ANY, LEFT, col1)`. Ils doivent correspondre à l’opération `JOIN` pour laquelle la table sera utilisée. Si les paramètres ne correspondent pas, ClickHouse ne génère pas d’exception et peut renvoyer des données incorrectes.

<div id="specifics-and-recommendations">
  ## Spécificités et recommandations
</div>

<div id="data-storage">
  ### Stockage des données
</div>

Les données de la table `Join` se trouvent toujours dans la RAM. Lors de l’insertion de lignes dans une table, ClickHouse écrit des blocs de données dans le répertoire du disque afin de pouvoir les restaurer lorsque le serveur redémarre.

Si le serveur redémarre incorrectement, le bloc de données sur le disque peut être perdu ou endommagé. Dans ce cas, vous devrez peut-être supprimer manuellement le fichier contenant les données endommagées.

<div id="selecting-and-inserting-data">
  ### Sélection et insertion de données
</div>

Vous pouvez utiliser des requêtes `INSERT` pour ajouter des données à des tables utilisant le moteur `Join`. Si la table a été créée avec le `mode de correspondance` `ANY`, les données associées à des clés dupliquées sont ignorées. Avec le `mode de correspondance` `ALL`, toutes les lignes sont ajoutées.

Les principaux cas d’usage des tables utilisant le moteur `Join` sont les suivants :

* Placer la table à droite dans une clause `JOIN`.
* Appeler la fonction [joinGet](/fr/sql-reference/functions/other-functions.md/#joinGet), qui permet d’extraire des données de la table de la même façon que depuis un dictionnaire.

<div id="deleting-data">
  ### Suppression de données
</div>

Les requêtes `ALTER DELETE` pour les tables utilisant le moteur `Join` sont implémentées sous forme de [mutations](/fr/sql-reference/statements/alter/index.md#mutations). La mutation `DELETE` lit les données filtrées et écrase les données en mémoire et sur disque.

<div id="join-limitations-and-settings">
  ### Limitations et paramètres
</div>

Lors de la création d&#39;une table, les paramètres suivants sont appliqués :

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

[join&#95;use&#95;nulls](/fr/operations/settings/settings.md/#join_use_nulls)

<div id="max_rows_in_join">
  #### `max_rows_in_join`
</div>

[max&#95;rows&#95;in&#95;join](/fr/operations/settings/settings#max_rows_in_join)

<div id="max_bytes_in_join">
  #### `max_bytes_in_join`
</div>

[max&#95;bytes&#95;in&#95;join](/fr/operations/settings/settings#max_bytes_in_join)

<div id="join_overflow_mode">
  #### `join_overflow_mode`
</div>

[join&#95;overflow&#95;mode](/fr/operations/settings/settings#join_overflow_mode)

<div id="join_any_take_last_row">
  #### `join_any_take_last_row`
</div>

[join&#95;any&#95;take&#95;last&#95;row](/fr/operations/settings/settings.md/#join_any_take_last_row)

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

<div id="persistent">
  #### Persistent
</div>

Désactive la persistance pour les moteurs de table Join et [Set](/fr/engines/table-engines/special/set.md).

Réduit la surcharge d’E/S. Convient aux scénarios privilégiant les performances et ne nécessitant pas de persistance.

Valeurs possibles :

* 1 — Activé.
* 0 — Désactivé.

Valeur par défaut : `1`.

Les tables du moteur `Join` ne peuvent pas être utilisées dans des opérations `GLOBAL JOIN`.

Le moteur `Join` permet de spécifier le paramètre [join&#95;use&#95;nulls](/fr/operations/settings/settings.md/#join_use_nulls) dans l’instruction `CREATE TABLE`. La requête [SELECT](/fr/sql-reference/statements/select/index.md) doit avoir la même valeur de `join_use_nulls`.

<div id="example">
  ## Exemples d’utilisation
</div>

Création de la table de gauche :

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

Création de la table `Join` de droite :

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

Jointure de tables :

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

Autre possibilité : vous pouvez récupérer des données à partir de la table `Join`, en spécifiant la valeur de la clé de jointure :

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

Suppression d’une ligne de la table `Join` :

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```