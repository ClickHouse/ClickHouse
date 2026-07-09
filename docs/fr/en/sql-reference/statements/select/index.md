---
description: 'Documentation de la requête SELECT'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'Requête SELECT'
doc_type: 'reference'
---

Les requêtes `SELECT` permettent de récupérer des données. Par défaut, les données demandées sont renvoyées au client ; associées à [INSERT INTO](../../../sql-reference/statements/insert-into.md), elles peuvent être redirigées vers une autre table.

<div id="syntax">
  ## Syntaxe
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

Toutes les clauses sont facultatives, à l’exception de la liste obligatoire d’expressions située immédiatement après `SELECT`, qui est décrite plus en détail [ci-dessous](#select-clause).

Les particularités de chaque clause facultative sont décrites dans des sections distinctes, présentées dans l’ordre où elles sont exécutées :

* [clause WITH](../../../sql-reference/statements/select/with.md)
* [clause SELECT](#select-clause)
* [clause DISTINCT](../../../sql-reference/statements/select/distinct.md)
* [clause FROM](../../../sql-reference/statements/select/from.md)
* [clause SAMPLE](../../../sql-reference/statements/select/sample.md)
* [clause JOIN](../../../sql-reference/statements/select/join.md)
* [clause PREWHERE](../../../sql-reference/statements/select/prewhere.md)
* [clause WHERE](../../../sql-reference/statements/select/where.md)
* [clause WINDOW](../../../sql-reference/window-functions/index.md)
* [clause GROUP BY](/fr/sql-reference/statements/select/group-by)
* [clause LIMIT BY](../../../sql-reference/statements/select/limit-by.md)
* [clause HAVING](../../../sql-reference/statements/select/having.md)
* [clause QUALIFY](../../../sql-reference/statements/select/qualify.md)
* [clause LIMIT](../../../sql-reference/statements/select/limit.md)
* [clause OFFSET](../../../sql-reference/statements/select/offset.md)
* [clause UNION](../../../sql-reference/statements/select/union.md)
* [clause INTERSECT](../../../sql-reference/statements/select/intersect.md)
* [clause EXCEPT](../../../sql-reference/statements/select/except.md)
* [clause INTO OUTFILE](../../../sql-reference/statements/select/into-outfile.md)
* [clause FORMAT](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## Clause SELECT
</div>

Les [Expressions](/fr/sql-reference/syntax#expressions) spécifiées dans la clause `SELECT` sont calculées une fois toutes les opérations des clauses décrites ci-dessus terminées. Ces expressions s’évaluent comme si elles s’appliquaient à des lignes distinctes du résultat. Si des expressions de la clause `SELECT` contiennent des fonctions d’agrégation, ClickHouse traite alors les fonctions d’agrégation ainsi que les expressions utilisées comme arguments lors de l’agrégation [GROUP BY](/fr/sql-reference/statements/select/group-by).

Si vous souhaitez inclure toutes les colonnes dans le résultat, utilisez le symbole astérisque (`*`). Par exemple, `SELECT * FROM ...`.

<div id="dynamic-column-selection">
  ### Sélection dynamique de colonnes
</div>

La sélection dynamique de colonnes (également appelée expression COLUMNS) vous permet de faire correspondre certaines colonnes d’un résultat à une expression régulière [re2](https://en.wikipedia.org/wiki/RE2_\(software\)).

```sql
COLUMNS('regexp')
```

Par exemple, prenons la table :

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

La requête suivante sélectionne les données de toutes les colonnes dont le nom contient le symbole `a`.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Les colonnes sélectionnées ne sont pas renvoyées dans l’ordre alphabétique.

Vous pouvez utiliser plusieurs expressions `COLUMNS` dans une requête et leur appliquer des fonctions.

Par exemple :

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Chaque colonne renvoyée par l’expression `COLUMNS` est transmise à la fonction comme un argument distinct. Vous pouvez également transmettre d’autres arguments à la fonction si elle les prend en charge. Soyez prudent lorsque vous utilisez des fonctions. Si une fonction ne prend pas en charge le nombre d’arguments que vous lui avez transmis, ClickHouse lève une exception.

Par exemple :

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

Dans cet exemple, `COLUMNS('a')` renvoie deux colonnes : `aa` et `ab`. `COLUMNS('c')` renvoie la colonne `bc`. L’opérateur `+` ne peut pas s’appliquer à 3 arguments. ClickHouse lève donc une exception avec le message approprié.

Les colonnes qui correspondent à l’expression `COLUMNS` peuvent avoir des types de données différents. Si `COLUMNS` ne correspond à aucune colonne et qu’il s’agit de la seule expression dans `SELECT`, ClickHouse lève une exception.

<div id="select-columns-with-like-or-ilike">
  #### Sélectionner des colonnes avec `LIKE` ou `ILIKE`
</div>

Vous pouvez également sélectionner des colonnes en faisant correspondre leur nom à un modèle après `*`, à l’aide d’un `LIKE` sensible à la casse ou d’un `ILIKE` insensible à la casse :

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Les motifs `LIKE` et `ILIKE` suivent la sémantique de `LIKE`, et non celle des expressions régulières. Le caractère `%` correspond à toute séquence de caractères, le caractère `_` à un seul caractère, et `\` sert à échapper `%`, `_` et `\`. La seule différence entre les deux est que `LIKE` fait la correspondance avec les noms de colonnes en respectant la casse, tandis que `ILIKE` est insensible à la casse. Par exemple :

```sql
SELECT * ILIKE 'a_' FROM col_names
```

La requête sélectionne les colonnes dont le nom comporte deux caractères et commence par `a`, comme `aa` et `ab`.

`* LIKE` et `* ILIKE` prennent également en charge les astérisques qualifiés et les transformateurs de colonnes :

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### Astérisque
</div>

Vous pouvez mettre un astérisque dans n’importe quelle partie d’une requête à la place d’une expression. Lors de l’analyse de la requête, l’astérisque est remplacé par la liste de toutes les colonnes de la table (à l’exclusion des colonnes `MATERIALIZED` et `ALIAS`). Il n’existe que quelques cas où l’utilisation d’un astérisque est justifiée :

* Lors de la création d’un dump de table.
* Pour les tables qui ne contiennent que quelques colonnes, comme les tables système.
* Pour obtenir des informations sur les colonnes présentes dans une table. Dans ce cas, définissez `LIMIT 1`. Mais il est préférable d’utiliser la requête `DESC TABLE`.
* Lorsqu’un filtrage important s’applique à un petit nombre de colonnes avec `PREWHERE`.
* Dans les sous-requêtes (puisque les colonnes qui ne sont pas nécessaires à la requête externe sont exclues des sous-requêtes).

Dans tous les autres cas, nous ne recommandons pas d’utiliser l’astérisque, car il ne vous apporte que les inconvénients d’un SGBD orienté colonnes, sans ses avantages. En d’autres termes, l’utilisation de l’astérisque n’est pas recommandée.

<div id="extreme-values">
  ### Valeurs extrêmes
</div>

En plus des résultats, vous pouvez également obtenir les valeurs minimales et maximales des colonnes de résultat. Pour cela, définissez le paramètre **extremes** sur 1. Les valeurs minimales et maximales sont calculées pour les types numériques, les dates et les dates avec heure. Pour les autres colonnes, les valeurs par défaut sont affichées.

Deux lignes supplémentaires sont calculées : les minimums et les maximums, respectivement. Ces deux lignes supplémentaires sont affichées dans les [formats](../../../interfaces/formats.md) `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` et `Pretty*`, séparément des autres lignes. Elles ne sont pas affichées dans les autres formats.

Dans les formats `JSON*` et `XML`, les valeurs extrêmes sont affichées dans un champ distinct nommé &#39;extremes&#39;. Dans les formats `TabSeparated*`, `CSV*` et `Vertical`, la ligne apparaît après le résultat principal, et après &#39;totals&#39; s&#39;il est présent. Elle est précédée d&#39;une ligne vide (après les autres données). Dans les formats `Pretty*`, la ligne est affichée sous la forme d&#39;une table distincte après le résultat principal, et après `totals` s&#39;il est présent. Dans le format `Template`, les valeurs extrêmes sont affichées selon le modèle spécifié.

Les valeurs extrêmes sont calculées sur les lignes avant `LIMIT`, mais après `LIMIT BY`. Cependant, lors de l&#39;utilisation de `LIMIT offset, size`, les lignes avant `offset` sont incluses dans `extremes`. Dans les requêtes en flux, le résultat peut également inclure un petit nombre de lignes ayant passé `LIMIT`.

<div id="notes">
  ### Remarques
</div>

Vous pouvez utiliser des synonymes (alias `AS`) dans n’importe quelle partie d’une requête.

Les clauses `GROUP BY`, `ORDER BY` et `LIMIT BY` acceptent les arguments positionnels. Pour les activer, activez le paramètre [enable&#95;positional&#95;arguments](/fr/operations/settings/settings#enable_positional_arguments). Ainsi, par exemple, `ORDER BY 1,2` triera les lignes de la table d’abord selon la première colonne, puis selon la deuxième.

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Si la requête omet les clauses `DISTINCT`, `GROUP BY` et `ORDER BY`, ainsi que les sous-requêtes `IN` et `JOIN`, elle sera entièrement traitée en flux, en utilisant une quantité de RAM de O(1). Sinon, la requête peut consommer beaucoup de RAM si les restrictions appropriées ne sont pas spécifiées :

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

Pour plus d’informations, voir la section « Settings ». Il est possible d’utiliser le tri externe (en enregistrant des tables temporaires sur un disque) et l’agrégation externe.

<div id="select-modifiers">
  ## Modificateurs SELECT
</div>

Vous pouvez utiliser les modificateurs suivants dans les requêtes `SELECT`.

| Modificateur                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`APPLY`](./apply_modifier.md)     | Permet d&#39;invoquer une fonction pour chaque ligne renvoyée par une expression de table externe dans une requête.                                                                                                                                                                                                                                                                                                                      |
| [`EXCEPT`](./except_modifier.md)   | Spécifie le nom d&#39;une ou de plusieurs colonnes à exclure du résultat. Tous les noms de colonnes correspondants sont omis de la sortie.                                                                                                                                                                                                                                                                                               |
| [`REPLACE`](./replace_modifier.md) | Spécifie un ou plusieurs [alias d&#39;expression](/fr/sql-reference/syntax#expression-aliases). Chaque alias doit correspondre à un nom de colonne de l&#39;instruction `SELECT *`. Dans la liste des colonnes de sortie, la colonne correspondant à l&#39;alias est remplacée par l&#39;expression de ce `REPLACE`. Ce modificateur ne modifie ni le nom ni l&#39;ordre des colonnes. En revanche, il peut modifier la valeur et son type. |

<div id="modifier-combinations">
  ### Combinaisons de modificateurs
</div>

Vous pouvez utiliser chaque modificateur séparément ou les combiner.

**Exemples :**

Utiliser le même modificateur plusieurs fois.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Utilisation de plusieurs modificateurs dans une même requête.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SETTINGS dans la requête SELECT
</div>

Vous pouvez spécifier les paramètres nécessaires directement dans la requête `SELECT`. La valeur du paramètre s’applique uniquement à cette requête, puis elle est réinitialisée à sa valeur par défaut ou à la valeur précédente une fois la requête exécutée.

Pour découvrir d’autres façons de définir des paramètres, consultez [cette page](/fr/operations/settings/overview).

Pour les paramètres booléens définis sur `true`, vous pouvez utiliser une syntaxe abrégée en omettant l’affectation de valeur. Lorsque seul le nom du paramètre est indiqué, il est automatiquement défini sur `1` (`true`).

**Exemple**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```