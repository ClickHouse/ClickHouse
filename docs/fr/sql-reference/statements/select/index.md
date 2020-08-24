---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: SELECT
---

# Sélectionnez la syntaxe des requêtes {#select-queries-syntax}

`SELECT` effectue la récupération des données.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] 
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

Toutes les clauses sont facultatives, à l'exception de la liste d'expressions requise immédiatement après `SELECT` qui est abordée plus en détail [dessous](#select-clause).

Spécificités de chaque clause facultative, sont couverts dans des sections distinctes, qui sont énumérés dans le même ordre qu'elles sont exécutées:

-   [AVEC la clause](with.md)
-   [La clause DISTINCT](distinct.md)
-   [De la clause](from.md)
-   [Exemple de clause](sample.md)
-   [Clause de JOINTURE](join.md)
-   [Clause PREWHERE](prewhere.md)
-   [Clause where](where.md)
-   [Groupe par clause](group-by.md)
-   [Limite par clause](limit-by.md)
-   [Clause HAVING](having.md)
-   [Clause SELECT](#select-clause)
-   [Clause LIMIT](limit.md)
-   [Clause UNION ALL](union-all.md)

## Clause SELECT {#select-clause}

[Expression](../../syntax.md#syntax-expressions) spécifié dans le `SELECT` clause sont calculés après toutes les opérations dans les clauses décrites ci-dessus sont terminés. Ces expressions fonctionnent comme si elles s'appliquaient à des lignes séparées dans le résultat. Si les expressions dans le `SELECT` la clause contient des fonctions d'agrégation, puis clickhouse traite les fonctions d'agrégation et les expressions utilisées [GROUP BY](group-by.md) agrégation.

Si vous souhaitez inclure toutes les colonnes dans le résultat, utilisez l'astérisque (`*`) symbole. Exemple, `SELECT * FROM ...`.

Pour correspondre à certaines colonnes dans le résultat avec un [re2](https://en.wikipedia.org/wiki/RE2_(software)) expression régulière, vous pouvez utiliser le `COLUMNS` expression.

``` sql
COLUMNS('regexp')
```

Par exemple, considérez le tableau:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

La requête suivante sélectionne les données de toutes les colonnes contenant les `a` symbole dans leur nom.

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Les colonnes sélectionnées sont retournés pas dans l'ordre alphabétique.

Vous pouvez utiliser plusieurs `COLUMNS` expressions dans une requête et leur appliquer des fonctions.

Exemple:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Chaque colonne renvoyée par le `COLUMNS` expression est passée à la fonction en tant qu'argument séparé. Vous pouvez également passer d'autres arguments à la fonction si elle les supporte. Soyez prudent lorsque vous utilisez des fonctions. Si une fonction ne prend pas en charge le nombre d'arguments que vous lui avez transmis, ClickHouse lève une exception.

Exemple:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

Dans cet exemple, `COLUMNS('a')` retourne deux colonnes: `aa` et `ab`. `COLUMNS('c')` renvoie la `bc` colonne. Le `+` l'opérateur ne peut pas s'appliquer à 3 arguments, donc ClickHouse lève une exception avec le message pertinent.

Colonnes qui correspondent à la `COLUMNS` l'expression peut avoir différents types de données. Si `COLUMNS` ne correspond à aucune colonne et est la seule expression dans `SELECT`, ClickHouse lance une exception.

### Astérisque {#asterisk}

Vous pouvez mettre un astérisque dans quelque partie de la requête au lieu d'une expression. Lorsque la requête est analysée, l'astérisque est étendu à une liste de toutes les colonnes `MATERIALIZED` et `ALIAS` colonne). Il n'y a que quelques cas où l'utilisation d'un astérisque est justifiée:

-   Lors de la création d'un vidage de table.
-   Pour les tables contenant seulement quelques colonnes, comme les tables système.
-   Pour obtenir des informations sur ce que sont les colonnes dans une table. Dans ce cas, la valeur `LIMIT 1`. Mais il est préférable d'utiliser la `DESC TABLE` requête.
-   Quand il y a une forte filtration sur un petit nombre de colonnes en utilisant `PREWHERE`.
-   Dans les sous-requêtes (puisque les colonnes qui ne sont pas nécessaires pour la requête externe sont exclues des sous-requêtes).

Dans tous les autres cas, nous ne recommandons pas d'utiliser l'astérisque, car il ne vous donne que les inconvénients d'un SGBD colonnaire au lieu des avantages. En d'autres termes, l'utilisation de l'astérisque n'est pas recommandée.

### Les Valeurs Extrêmes {#extreme-values}

En plus des résultats, vous pouvez également obtenir des valeurs minimales et maximales pour les colonnes de résultats. Pour ce faire, définissez la **extrême** réglage sur 1. Les Minimums et les maximums sont calculés pour les types numériques, les dates et les dates avec des heures. Pour les autres colonnes, les valeurs par défaut sont sorties.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, et `Pretty*` [format](../../../interfaces/formats.md), séparés des autres lignes. Ils ne sont pas Produits pour d'autres formats.

Dans `JSON*` formats, les valeurs extrêmes sont sorties dans un ‘extremes’ champ. Dans `TabSeparated*` formats, la ligne vient après le résultat principal, et après ‘totals’ si elle est présente. Elle est précédée par une ligne vide (après les autres données). Dans `Pretty*` formats, la ligne est sortie comme une table séparée après le résultat principal, et après `totals` si elle est présente.

Les valeurs extrêmes sont calculées pour les lignes avant `LIMIT` mais après `LIMIT BY`. Cependant, lors de l'utilisation de `LIMIT offset, size`, les lignes avant de les `offset` sont inclus dans `extremes`. Dans les requêtes de flux, le résultat peut également inclure un petit nombre de lignes qui ont traversé `LIMIT`.

### Note {#notes}

Vous pouvez utiliser des synonymes (`AS` alias) dans n'importe quelle partie d'une requête.

Le `GROUP BY` et `ORDER BY` les clauses ne supportent pas les arguments positionnels. Cela contredit MySQL, mais est conforme à SQL standard. Exemple, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

## Détails De Mise En Œuvre {#implementation-details}

Si la requête omet le `DISTINCT`, `GROUP BY` et `ORDER BY` les clauses et les `IN` et `JOIN` sous-requêtes, la requête sera complètement traitée en flux, en utilisant O (1) quantité de RAM. Sinon, la requête peut consommer beaucoup de RAM si les restrictions appropriées ne sont pas spécifiées:

-   `max_memory_usage`
-   `max_rows_to_group_by`
-   `max_rows_to_sort`
-   `max_rows_in_distinct`
-   `max_bytes_in_distinct`
-   `max_rows_in_set`
-   `max_bytes_in_set`
-   `max_rows_in_join`
-   `max_bytes_in_join`
-   `max_bytes_before_external_sort`
-   `max_bytes_before_external_group_by`

Pour plus d'informations, consultez la section “Settings”. Il est possible d'utiliser le tri externe (sauvegarde des tables temporaires sur un disque) et l'agrégation externe.

{## [Article Original](https://clickhouse.tech/docs/en/sql-reference/statements/select/) ##}
