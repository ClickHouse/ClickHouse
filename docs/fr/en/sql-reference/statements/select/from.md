---
description: 'Documentation de la clause FROM'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'Clause FROM'
doc_type: 'reference'
---

La clause `FROM` spécifie la source à partir de laquelle lire les données :

* [Table](../../../engines/table-engines/index.md)
* [Sous-requête](../../../sql-reference/statements/select/index.md)
* [Fonction de table](/fr/sql-reference/table-functions)

Les clauses [JOIN](../../../sql-reference/statements/select/join.md) et [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) peuvent également être utilisées pour étendre les fonctionnalités de la clause `FROM`.

Une sous-requête est une autre requête `SELECT` qui peut être spécifiée entre parenthèses dans la clause `FROM`.

Une clause `VALUES` standard SQL peut également être utilisée comme expression de table :

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

Voir [la fonction de table Values](/fr/sql-reference/table-functions/values#sql-standard-values-clause) pour plus de détails.

Le `FROM` peut contenir plusieurs sources de données, séparées par des virgules, ce qui revient à effectuer un [CROSS JOIN](../../../sql-reference/statements/select/join.md) entre elles.

`FROM` peut également apparaître avant une clause `SELECT`. Il s&#39;agit d&#39;une extension propre à ClickHouse du SQL standard, qui rend les instructions `SELECT` plus lisibles. Exemple :

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## Modificateur FINAL
</div>

Lorsque `FINAL` est spécifié, ClickHouse fusionne complètement les données avant de renvoyer le résultat. Cela exécute également toutes les transformations de données effectuées lors des fusions pour le table engine concerné.

Il s&#39;applique lors de la sélection de données depuis des tables utilisant les table engines suivants :

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

Les queries `SELECT` avec `FINAL` sont exécutées en parallèle. Le paramètre [max&#95;final&#95;threads](/fr/operations/settings/settings#max_final_threads) limite le nombre de threads utilisés.

<div id="drawbacks">
  ### Inconvénients
</div>

Les requêtes qui utilisent `FINAL` s’exécutent légèrement plus lentement que des requêtes similaires qui n’utilisent pas `FINAL`, car :

* Les données sont fusionnées pendant l’exécution de la requête.
* Les requêtes avec `FINAL` peuvent lire les colonnes de clé primaire en plus des colonnes spécifiées dans la requête.

`FINAL` nécessite davantage de ressources de calcul et de mémoire, car le traitement qui se produirait normalement au moment de la fusion doit alors être effectué en mémoire lors de la requête. Cependant, l’utilisation de `FINAL` est parfois nécessaire pour produire des résultats exacts (car les données peuvent ne pas encore être entièrement fusionnées). Cela reste moins coûteux que d’exécuter `OPTIMIZE` pour forcer une fusion.

Comme alternative à l’utilisation de `FINAL`, il est parfois possible d’utiliser d’autres requêtes qui partent du principe que les processus d’arrière-plan du moteur `MergeTree` n’ont pas encore eu lieu, et de gérer cela en appliquant une agrégation (par exemple pour éliminer les doublons). Si vous devez utiliser `FINAL` dans vos requêtes pour obtenir les résultats souhaités, vous pouvez le faire, mais gardez à l’esprit le traitement supplémentaire nécessaire.

`FINAL` peut être appliqué automatiquement à toutes les tables d’une requête à l’aide du paramètre [FINAL](../../../operations/settings/settings.md#final), via une session ou un profil utilisateur.

<div id="example-usage">
  ### Exemple d’utilisation
</div>

Utilisation du mot-clé `FINAL`

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Utilisation de `FINAL` comme paramètre de requête

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Utilisation de `FINAL` comme paramètre de session

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### Alias et FINAL
</div>

Lorsqu’une table a un alias, `FINAL` se place après celui-ci. Cela se voit particulièrement dans les requêtes [`JOIN`](/fr/sql-reference/statements/select/join), où les tables sont généralement dotées d’un alias :

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` est un modificateur appliqué à la référence de table ; il doit donc suivre l’expression complète `table [AS alias]`. Le placer avant l’alias (`FROM table1 FINAL AS t1`) provoque une erreur de syntaxe.

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Si la clause `FROM` est omise, les données seront lues à partir de la table `system.one`.
La table `system.one` contient exactement une ligne (cette table remplit le même rôle que la table DUAL présente dans d’autres SGBD).

Pour exécuter une requête, toutes les colonnes mentionnées dans la requête sont extraites de la table appropriée. Les colonnes qui ne sont pas nécessaires à la requête externe sont ignorées dans les sous-requêtes.
Si une requête ne mentionne aucune colonne (par exemple, `SELECT count() FROM t`), une colonne est quand même extraite de la table (de préférence la plus petite), afin de calculer le nombre de lignes.