---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 33
toc_title: SELECT
---

# Sélectionnez La Syntaxe Des requêtes {#select-queries-syntax}

`SELECT` effectue la récupération des données.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

Toutes les clauses sont facultatives, à l’exception de la liste d’expressions requise immédiatement après SELECT.
Les clauses ci-dessous sont décrites dans presque le même ordre que dans l’exécution de la requête convoyeur.

Si la requête omet le `DISTINCT`, `GROUP BY` et `ORDER BY` les clauses et les `IN` et `JOIN` sous-requêtes, la requête sera complètement traitée en flux, en utilisant O (1) quantité de RAM.
Sinon, la requête peut consommer beaucoup de RAM si les restrictions appropriées ne sont pas spécifiées: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. Pour plus d’informations, consultez la section “Settings”. Il est possible d’utiliser le tri externe (sauvegarde des tables temporaires sur un disque) et l’agrégation externe. `The system does not have "merge join"`.

### AVEC La Clause {#with-clause}

Cette section prend en charge les Expressions de Table courantes ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), avec quelques limitations:
1. Les requêtes récursives ne sont pas prises en charge
2. Lorsque la sous-requête est utilisée à l’intérieur avec section, son résultat doit être scalaire avec exactement une ligne
3. Les résultats d’Expression ne sont pas disponibles dans les sous requêtes
Les résultats des expressions de clause WITH peuvent être utilisés dans la clause SELECT.

Exemple 1: Utilisation d’une expression constante comme “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Exemple 2: Expulsion de la somme(octets) résultat de l’expression de clause SELECT de la liste de colonnes

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

Exemple 3: Utilisation des résultats de la sous-requête scalaire

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

Exemple 4: réutilisation de l’expression dans la sous-requête
Comme solution de contournement pour la limitation actuelle de l’utilisation de l’expression dans les sous-requêtes, Vous pouvez la dupliquer.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### De La Clause {#select-from}

Si la clause FROM est omise, les données seront lues à partir `system.one` table.
Le `system.one` table contient exactement une ligne (cette table remplit le même but que la table double trouvée dans d’autres SGBD).

Le `FROM` clause spécifie la source à partir de laquelle lire les données:

-   Table
-   Sous-requête
-   [Fonction de Table](../table-functions/index.md#table-functions)

`ARRAY JOIN` et le régulier `JOIN` peuvent également être inclus (voir ci-dessous).

Au lieu d’une table, l’ `SELECT` sous-requête peut être spécifiée entre parenthèses.
Contrairement à SQL standard, un synonyme n’a pas besoin d’être spécifié après une sous-requête.

Pour exécuter une requête, toutes les colonnes mentionnées dans la requête sont extraites de la table appropriée. Toutes les colonnes non nécessaires pour la requête externe sont rejetées des sous-requêtes.
Si une requête ne répertorie aucune colonne (par exemple, `SELECT count() FROM t`), une colonne est extraite de la table de toute façon (la plus petite est préférée), afin de calculer le nombre de lignes.

#### Modificateur FINAL {#select-from-final}

Applicable lors de la sélection de données à partir de tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-famille de moteurs autres que `GraphiteMergeTree`. Lorsque `FINAL` est spécifié, ClickHouse fusionne complètement les données avant de renvoyer le résultat et effectue ainsi toutes les transformations de données qui se produisent lors des fusions pour le moteur de table donné.

Également pris en charge pour:
- [Répliqué](../../engines/table-engines/mergetree-family/replication.md) les versions de `MergeTree` moteur.
- [Vue](../../engines/table-engines/special/view.md), [Tampon](../../engines/table-engines/special/buffer.md), [Distribué](../../engines/table-engines/special/distributed.md), et [MaterializedView](../../engines/table-engines/special/materializedview.md) moteurs qui fonctionnent sur d’autres moteurs, à condition qu’ils aient été créés sur `MergeTree`-tables de moteur.

Requêtes qui utilisent `FINAL` sont exécutés pas aussi vite que les requêtes similaires qui ne le font pas, car:

-   La requête est exécutée dans un seul thread et les données sont fusionnées lors de l’exécution de la requête.
-   Les requêtes avec `FINAL` lire les colonnes de clé primaire en plus des colonnes spécifiées dans la requête.

Dans la plupart des cas, évitez d’utiliser `FINAL`.

### Exemple De Clause {#select-sample-clause}

Le `SAMPLE` la clause permet un traitement de requête approximatif.

Lorsque l’échantillonnage de données est activé, la requête n’est pas effectuée sur toutes les données, mais uniquement sur une certaine fraction de données (échantillon). Par exemple, si vous avez besoin de calculer des statistiques pour toutes les visites, il suffit d’exécuter la requête sur le 1/10 de la fraction de toutes les visites, puis multiplier le résultat par 10.

Le traitement approximatif des requêtes peut être utile dans les cas suivants:

-   Lorsque vous avez des exigences de synchronisation strictes (comme \<100ms), mais que vous ne pouvez pas justifier le coût des ressources matérielles supplémentaires pour y répondre.
-   Lorsque vos données brutes ne sont pas précises, l’approximation ne dégrade pas sensiblement la qualité.
-   Les exigences commerciales ciblent des résultats approximatifs (pour la rentabilité, ou afin de commercialiser des résultats exacts aux utilisateurs premium).

!!! note "Note"
    Vous ne pouvez utiliser l’échantillonnage qu’avec les tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) famille, et seulement si l’expression d’échantillonnage a été spécifiée lors de la création de la table (voir [Moteur MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Les caractéristiques de l’échantillonnage des données sont énumérées ci-dessous:

-   L’échantillonnage de données est un mécanisme déterministe. Le résultat de la même `SELECT .. SAMPLE` la requête est toujours le même.
-   L’échantillonnage fonctionne de manière cohérente pour différentes tables. Pour les tables avec une seule clé d’échantillonnage, un échantillon avec le même coefficient sélectionne toujours le même sous-ensemble de données possibles. Par exemple, un exemple d’ID utilisateur prend des lignes avec le même sous-ensemble de tous les ID utilisateur possibles de différentes tables. Cela signifie que vous pouvez utiliser l’exemple dans les sous-requêtes dans la [IN](#select-in-operators) clause. En outre, vous pouvez joindre des échantillons en utilisant le [JOIN](#select-join) clause.
-   L’échantillonnage permet de lire moins de données à partir d’un disque. Notez que vous devez spécifier l’échantillonnage clé correctement. Pour plus d’informations, voir [Création d’une Table MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Pour l’ `SAMPLE` clause la syntaxe suivante est prise en charge:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                                                 |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Ici `k` est le nombre de 0 à 1.</br>La requête est exécutée sur `k` fraction des données. Exemple, `SAMPLE 0.1` exécute la requête sur 10% des données. [Lire plus](#select-sample-k)                                                                                       |
| `SAMPLE n`           | Ici `n` est un entier suffisamment grand.</br>La requête est exécutée sur un échantillon d’au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes. [Lire plus](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Ici `k` et `m` sont les nombres de 0 à 1.</br>La requête est exécutée sur un échantillon de `k` fraction des données. Les données utilisées pour l’échantillon est compensée par `m` fraction. [Lire plus](#select-sample-offset)                                           |

#### SAMPLE K {#select-sample-k}

Ici `k` est le nombre de 0 à 1 (les notations fractionnaires et décimales sont prises en charge). Exemple, `SAMPLE 1/2` ou `SAMPLE 0.5`.

Dans un `SAMPLE k` clause, l’échantillon est prélevé à partir de la `k` fraction des données. L’exemple est illustré ci-dessous:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

Dans cet exemple, la requête est exécutée sur un échantillon de 0,1 (10%) de données. Les valeurs des fonctions d’agrégat ne sont pas corrigées automatiquement, donc pour obtenir un résultat approximatif, la valeur `count()` est multiplié manuellement par 10.

#### SAMPLE N {#select-sample-n}

Ici `n` est un entier suffisamment grand. Exemple, `SAMPLE 10000000`.

Dans ce cas, la requête est exécutée sur un échantillon d’au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes.

Puisque l’unité minimale pour la lecture des données est un granule (sa taille est définie par le `index_granularity` de réglage), il est logique de définir un échantillon beaucoup plus grand que la taille du granule.

Lors de l’utilisation de la `SAMPLE n` clause, vous ne savez pas quel pourcentage relatif de données a été traité. Donc, vous ne connaissez pas le coefficient par lequel les fonctions agrégées doivent être multipliées. L’utilisation de la `_sample_factor` colonne virtuelle pour obtenir le résultat approximatif.

Le `_sample_factor` colonne contient des coefficients relatifs qui sont calculés dynamiquement. Cette colonne est créée automatiquement lorsque vous [créer](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) une table avec la clé d’échantillonnage spécifiée. Les exemples d’utilisation de la `_sample_factor` colonne sont indiqués ci-dessous.

Considérons la table `visits` qui contient des statistiques sur les visites de site. Le premier exemple montre comment calculer le nombre de pages vues:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

L’exemple suivant montre comment calculer le nombre total de visites:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

L’exemple ci-dessous montre comment calculer la durée moyenne de la session. Notez que vous n’avez pas besoin d’utiliser le coefficient relatif pour calculer les valeurs moyennes.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

Ici `k` et `m` sont des nombres de 0 à 1. Des exemples sont présentés ci-dessous.

**Exemple 1**

``` sql
SAMPLE 1/10
```

Dans cet exemple, l’échantillon représente 1 / 10e de toutes les données:

`[++------------]`

**Exemple 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Ici, un échantillon de 10% est prélevé à partir de la seconde moitié des données.

`[------++------]`

### Clause De Jointure De Tableau {#select-array-join-clause}

Permet l’exécution de `JOIN` avec un tableau ou une structure de données imbriquée. L’intention est similaire à la [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin) la fonction, mais sa fonctionnalité est plus large.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Vous pouvez spécifier qu’un seul `ARRAY JOIN` la clause dans une requête.

L’ordre d’exécution de la requête est optimisé lors de l’exécution `ARRAY JOIN`. Bien `ARRAY JOIN` doit toujours être spécifié avant l’ `WHERE/PREWHERE` clause, il peut être effectué soit avant `WHERE/PREWHERE` (si le résultat est nécessaire dans cette clause), ou après l’avoir terminé (pour réduire le volume de calculs). L’ordre de traitement est contrôlée par l’optimiseur de requête.

Types pris en charge de `ARRAY JOIN` sont énumérés ci-dessous:

-   `ARRAY JOIN` - Dans ce cas, des tableaux vides ne sont pas inclus dans le résultat de `JOIN`.
-   `LEFT ARRAY JOIN` - Le résultat de `JOIN` contient des lignes avec des tableaux vides. La valeur d’un tableau vide est définie sur la valeur par défaut pour le type d’élément de tableau (généralement 0, chaîne vide ou NULL).

Les exemples ci-dessous illustrent l’utilisation de la `ARRAY JOIN` et `LEFT ARRAY JOIN` clause. Créons une table avec un [Tableau](../../sql-reference/data-types/array.md) tapez colonne et insérez des valeurs dedans:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

L’exemple ci-dessous utilise la `ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

L’exemple suivant utilise l’ `LEFT ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### À L’Aide D’Alias {#using-aliases}

Un alias peut être spécifié pour un tableau `ARRAY JOIN` clause. Dans ce cas, un élément de tableau peut être consulté par ce pseudonyme, mais le tableau lui-même est accessible par le nom d’origine. Exemple:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

En utilisant des alias, vous pouvez effectuer `ARRAY JOIN` avec un groupe externe. Exemple:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Plusieurs tableaux peuvent être séparés par des virgules `ARRAY JOIN` clause. Dans ce cas, `JOIN` est effectuée avec eux simultanément (la somme directe, pas le produit cartésien). Notez que tous les tableaux doivent avoir la même taille. Exemple:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

L’exemple ci-dessous utilise la [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### Jointure De Tableau Avec La Structure De données imbriquée {#array-join-with-nested-data-structure}

`ARRAY`Rejoindre " fonctionne également avec [structures de données imbriquées](../../sql-reference/data-types/nested-data-structures/nested.md). Exemple:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Lorsque vous spécifiez des noms de structures de données imbriquées dans `ARRAY JOIN` le sens est le même que `ARRAY JOIN` avec tous les éléments du tableau qui la compose. Des exemples sont énumérés ci-dessous:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Cette variation a également du sens:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Un alias peut être utilisé pour une structure de données imbriquée, afin de sélectionner `JOIN` le résultat ou le tableau source. Exemple:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Exemple d’utilisation de l’ [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### Clause De JOINTURE {#select-join}

Rejoint les données dans la normale [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) sens.

!!! info "Note"
    Pas liées à [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

Les noms de table peuvent être spécifiés au lieu de `<left_subquery>` et `<right_subquery>`. Ceci est équivalent à la `SELECT * FROM table` sous-requête, sauf dans un cas particulier lorsque la table a [Rejoindre](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### Types Pris En Charge De `JOIN` {#select-join-types}

-   `INNER JOIN` (ou `JOIN`)
-   `LEFT JOIN` (ou `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (ou `RIGHT OUTER JOIN`)
-   `FULL JOIN` (ou `FULL OUTER JOIN`)
-   `CROSS JOIN` (ou `,` )

Voir la norme [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) Description.

#### Plusieurs REJOINDRE {#multiple-join}

En effectuant des requêtes, ClickHouse réécrit les jointures multi-tables dans la séquence des jointures à deux tables. Par exemple, S’il y a quatre tables pour join clickhouse rejoint la première et la seconde, puis rejoint le résultat avec la troisième table, et à la dernière étape, il rejoint la quatrième.

Si une requête contient l’ `WHERE` clickhouse essaie de pousser les filtres de cette clause à travers la jointure intermédiaire. S’il ne peut pas appliquer le filtre à chaque jointure intermédiaire, ClickHouse applique les filtres une fois toutes les jointures terminées.

Nous recommandons l’ `JOIN ON` ou `JOIN USING` syntaxe pour créer des requêtes. Exemple:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

Vous pouvez utiliser des listes de tables séparées par des virgules `FROM` clause. Exemple:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

Ne mélangez pas ces syntaxes.

ClickHouse ne supporte pas directement la syntaxe avec des virgules, Nous ne recommandons donc pas de les utiliser. L’algorithme tente de réécrire la requête en termes de `CROSS JOIN` et `INNER JOIN` clauses et procède ensuite au traitement des requêtes. Lors de la réécriture de la requête, ClickHouse tente d’optimiser les performances et la consommation de mémoire. Par défaut, ClickHouse traite les virgules comme `INNER JOIN` clause et convertit `INNER JOIN` de `CROSS JOIN` lorsque l’algorithme ne peut pas garantir que `INNER JOIN` retourne les données requises.

#### Rigueur {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Produit cartésien](https://en.wikipedia.org/wiki/Cartesian_product) à partir des lignes correspondantes. C’est la norme `JOIN` comportement en SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` et `ALL` les mots clés sont les mêmes.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` l’utilisation est décrite ci-dessous.

**ASOF joindre L’utilisation**

`ASOF JOIN` est utile lorsque vous devez joindre des enregistrements qui n’ont pas de correspondance exacte.

Tables pour `ASOF JOIN` doit avoir une colonne de séquence ordonnée. Cette colonne ne peut pas être seule dans une table et doit être l’un des types de données: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, et `DateTime`.

Syntaxe `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Vous pouvez utiliser n’importe quel nombre de conditions d’égalité et exactement une condition de correspondance la plus proche. Exemple, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions prises en charge pour la correspondance la plus proche: `>`, `>=`, `<`, `<=`.

Syntaxe `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` utiliser `equi_columnX` pour rejoindre sur l’égalité et `asof_column` pour rejoindre le match le plus proche avec le `table_1.asof_column >= table_2.asof_column` condition. Le `asof_column` colonne toujours la dernière dans le `USING` clause.

Par exemple, considérez les tableaux suivants:

\`\`\` texte
table\_1 table\_2

événement \| ev\_time \| user\_id événement \| ev\_time \| user\_id
