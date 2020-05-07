---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
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

Toutes les clauses sont facultatives, à l'exception de la liste d'expressions requise immédiatement après SELECT.
Les clauses ci-dessous sont décrites dans presque le même ordre que dans l'exécution de la requête convoyeur.

Si la requête omet le `DISTINCT`, `GROUP BY` et `ORDER BY` les clauses et les `IN` et `JOIN` sous-requêtes, la requête sera complètement traitée en flux, en utilisant O (1) quantité de RAM.
Sinon, la requête peut consommer beaucoup de RAM si les restrictions appropriées ne sont pas spécifiées: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. Pour plus d'informations, consultez la section “Settings”. Il est possible d'utiliser le tri externe (sauvegarde des tables temporaires sur un disque) et l'agrégation externe. `The system does not have "merge join"`.

### AVEC la Clause {#with-clause}

Cette section prend en charge les Expressions de Table courantes ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), avec quelques limitations:
1. Les requêtes récursives ne sont pas prises en charge
2. Lorsque la sous-requête est utilisée à l'intérieur avec section, son résultat doit être scalaire avec exactement une ligne
3. Les résultats d'Expression ne sont pas disponibles dans les sous requêtes
Les résultats des expressions de clause WITH peuvent être utilisés dans la clause SELECT.

Exemple 1: Utilisation d'une expression constante comme “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Exemple 2: Expulsion de la somme(octets) résultat de l'expression de clause SELECT de la liste de colonnes

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

Exemple 4: réutilisation de l'expression dans la sous-requête
Comme solution de contournement pour la limitation actuelle de l'utilisation de l'expression dans les sous-requêtes, Vous pouvez la dupliquer.

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

### De la Clause {#select-from}

Si la clause FROM est omise, les données seront lues à partir `system.one` table.
Le `system.one` table contient exactement une ligne (cette table remplit le même but que la table double trouvée dans d'autres SGBD).

Le `FROM` clause spécifie la source à partir de laquelle lire les données:

-   Table
-   Sous-requête
-   [Fonction de Table](../table-functions/index.md#table-functions)

`ARRAY JOIN` et le régulier `JOIN` peuvent également être inclus (voir ci-dessous).

Au lieu d'une table, l' `SELECT` sous-requête peut être spécifiée entre parenthèses.
Contrairement à SQL standard, un synonyme n'a pas besoin d'être spécifié après une sous-requête.

Pour exécuter une requête, toutes les colonnes mentionnées dans la requête sont extraites de la table appropriée. Toutes les colonnes non nécessaires pour la requête externe sont rejetées des sous-requêtes.
Si une requête ne répertorie aucune colonne (par exemple, `SELECT count() FROM t`), une colonne est extraite de la table de toute façon (la plus petite est préférée), afin de calculer le nombre de lignes.

#### Modificateur FINAL {#select-from-final}

Applicable lors de la sélection de données à partir de tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-famille de moteurs autres que `GraphiteMergeTree`. Lorsque `FINAL` est spécifié, ClickHouse fusionne complètement les données avant de renvoyer le résultat et effectue ainsi toutes les transformations de données qui se produisent lors des fusions pour le moteur de table donné.

Également pris en charge pour:
- [Répliqué](../../engines/table-engines/mergetree-family/replication.md) les versions de `MergeTree` moteur.
- [Vue](../../engines/table-engines/special/view.md), [Tampon](../../engines/table-engines/special/buffer.md), [Distribué](../../engines/table-engines/special/distributed.md), et [MaterializedView](../../engines/table-engines/special/materializedview.md) moteurs qui fonctionnent sur d'autres moteurs, à condition qu'ils aient été créés sur `MergeTree`-tables de moteur.

Requêtes qui utilisent `FINAL` sont exécutés pas aussi vite que les requêtes similaires qui ne le font pas, car:

-   La requête est exécutée dans un seul thread et les données sont fusionnées lors de l'exécution de la requête.
-   Les requêtes avec `FINAL` lire les colonnes de clé primaire en plus des colonnes spécifiées dans la requête.

Dans la plupart des cas, évitez d'utiliser `FINAL`.

### Exemple de Clause {#select-sample-clause}

Le `SAMPLE` la clause permet un traitement de requête approximatif.

Lorsque l'échantillonnage de données est activé, la requête n'est pas effectuée sur toutes les données, mais uniquement sur une certaine fraction de données (échantillon). Par exemple, si vous avez besoin de calculer des statistiques pour toutes les visites, il suffit d'exécuter la requête sur le 1/10 de la fraction de toutes les visites, puis multiplier le résultat par 10.

Le traitement approximatif des requêtes peut être utile dans les cas suivants:

-   Lorsque vous avez des exigences de synchronisation strictes (comme \<100ms), mais que vous ne pouvez pas justifier le coût des ressources matérielles supplémentaires pour y répondre.
-   Lorsque vos données brutes ne sont pas précises, l'approximation ne dégrade pas sensiblement la qualité.
-   Les exigences commerciales ciblent des résultats approximatifs (pour la rentabilité, ou afin de commercialiser des résultats exacts aux utilisateurs premium).

!!! note "Note"
    Vous ne pouvez utiliser l'échantillonnage qu'avec les tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) famille, et seulement si l'expression d'échantillonnage a été spécifiée lors de la création de la table (voir [Moteur MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Les caractéristiques de l'échantillonnage des données sont énumérées ci-dessous:

-   L'échantillonnage de données est un mécanisme déterministe. Le résultat de la même `SELECT .. SAMPLE` la requête est toujours le même.
-   L'échantillonnage fonctionne de manière cohérente pour différentes tables. Pour les tables avec une seule clé d'échantillonnage, un échantillon avec le même coefficient sélectionne toujours le même sous-ensemble de données possibles. Par exemple, un exemple d'ID utilisateur prend des lignes avec le même sous-ensemble de tous les ID utilisateur possibles de différentes tables. Cela signifie que vous pouvez utiliser l'exemple dans les sous-requêtes dans la [IN](#select-in-operators) clause. En outre, vous pouvez joindre des échantillons en utilisant le [JOIN](#select-join) clause.
-   L'échantillonnage permet de lire moins de données à partir d'un disque. Notez que vous devez spécifier l'échantillonnage clé correctement. Pour plus d'informations, voir [Création d'une Table MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Pour l' `SAMPLE` clause la syntaxe suivante est prise en charge:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                                                 |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Ici `k` est le nombre de 0 à 1.</br>La requête est exécutée sur `k` fraction des données. Exemple, `SAMPLE 0.1` exécute la requête sur 10% des données. [Lire plus](#select-sample-k)                                                                                       |
| `SAMPLE n`           | Ici `n` est un entier suffisamment grand.</br>La requête est exécutée sur un échantillon d'au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes. [Lire plus](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Ici `k` et `m` sont les nombres de 0 à 1.</br>La requête est exécutée sur un échantillon de `k` fraction des données. Les données utilisées pour l'échantillon est compensée par `m` fraction. [Lire plus](#select-sample-offset)                                           |

#### SAMPLE K {#select-sample-k}

Ici `k` est le nombre de 0 à 1 (les notations fractionnaires et décimales sont prises en charge). Exemple, `SAMPLE 1/2` ou `SAMPLE 0.5`.

Dans un `SAMPLE k` clause, l'échantillon est prélevé à partir de la `k` fraction des données. L'exemple est illustré ci-dessous:

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

Dans cet exemple, la requête est exécutée sur un échantillon de 0,1 (10%) de données. Les valeurs des fonctions d'agrégat ne sont pas corrigées automatiquement, donc pour obtenir un résultat approximatif, la valeur `count()` est multiplié manuellement par 10.

#### SAMPLE N {#select-sample-n}

Ici `n` est un entier suffisamment grand. Exemple, `SAMPLE 10000000`.

Dans ce cas, la requête est exécutée sur un échantillon d'au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes.

Puisque l'unité minimale pour la lecture des données est un granule (sa taille est définie par le `index_granularity` de réglage), il est logique de définir un échantillon beaucoup plus grand que la taille du granule.

Lors de l'utilisation de la `SAMPLE n` clause, vous ne savez pas quel pourcentage relatif de données a été traité. Donc, vous ne connaissez pas le coefficient par lequel les fonctions agrégées doivent être multipliées. L'utilisation de la `_sample_factor` colonne virtuelle pour obtenir le résultat approximatif.

Le `_sample_factor` colonne contient des coefficients relatifs qui sont calculés dynamiquement. Cette colonne est créée automatiquement lorsque vous [créer](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) une table avec la clé d'échantillonnage spécifiée. Les exemples d'utilisation de la `_sample_factor` colonne sont indiqués ci-dessous.

Considérons la table `visits` qui contient des statistiques sur les visites de site. Le premier exemple montre comment calculer le nombre de pages vues:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

L'exemple suivant montre comment calculer le nombre total de visites:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

L'exemple ci-dessous montre comment calculer la durée moyenne de la session. Notez que vous n'avez pas besoin d'utiliser le coefficient relatif pour calculer les valeurs moyennes.

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

Dans cet exemple, l'échantillon représente 1 / 10e de toutes les données:

`[++------------]`

**Exemple 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Ici, un échantillon de 10% est prélevé à partir de la seconde moitié des données.

`[------++------]`

### Clause de jointure de tableau {#select-array-join-clause}

Permet l'exécution de `JOIN` avec un tableau ou une structure de données imbriquée. L'intention est similaire à la [arrayJoin](../functions/array-join.md#functions_arrayjoin) la fonction, mais sa fonctionnalité est plus large.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Vous pouvez spécifier qu'un seul `ARRAY JOIN` la clause dans une requête.

L'ordre d'exécution de la requête est optimisé lors de l'exécution `ARRAY JOIN`. Bien `ARRAY JOIN` doit toujours être spécifié avant l' `WHERE/PREWHERE` clause, il peut être effectué soit avant `WHERE/PREWHERE` (si le résultat est nécessaire dans cette clause), ou après l'avoir terminé (pour réduire le volume de calculs). L'ordre de traitement est contrôlée par l'optimiseur de requête.

Types pris en charge de `ARRAY JOIN` sont énumérés ci-dessous:

-   `ARRAY JOIN` - Dans ce cas, des tableaux vides ne sont pas inclus dans le résultat de `JOIN`.
-   `LEFT ARRAY JOIN` - Le résultat de `JOIN` contient des lignes avec des tableaux vides. La valeur d'un tableau vide est définie sur la valeur par défaut pour le type d'élément de tableau (généralement 0, chaîne vide ou NULL).

Les exemples ci-dessous illustrent l'utilisation de la `ARRAY JOIN` et `LEFT ARRAY JOIN` clause. Créons une table avec un [Tableau](../../sql-reference/data-types/array.md) tapez colonne et insérez des valeurs dedans:

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

L'exemple ci-dessous utilise la `ARRAY JOIN` clause:

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

L'exemple suivant utilise l' `LEFT ARRAY JOIN` clause:

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

#### À L'Aide D'Alias {#using-aliases}

Un alias peut être spécifié pour un tableau `ARRAY JOIN` clause. Dans ce cas, un élément de tableau peut être consulté par ce pseudonyme, mais le tableau lui-même est accessible par le nom d'origine. Exemple:

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

L'exemple ci-dessous utilise la [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

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

#### Jointure de tableau avec la Structure de données imbriquée {#array-join-with-nested-data-structure}

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

Exemple d'utilisation de l' [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

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

### Clause de JOINTURE {#select-join}

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

#### Types pris en charge de `JOIN` {#select-join-types}

-   `INNER JOIN` (ou `JOIN`)
-   `LEFT JOIN` (ou `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (ou `RIGHT OUTER JOIN`)
-   `FULL JOIN` (ou `FULL OUTER JOIN`)
-   `CROSS JOIN` (ou `,` )

Voir la norme [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) Description.

#### Plusieurs REJOINDRE {#multiple-join}

En effectuant des requêtes, ClickHouse réécrit les jointures multi-tables dans la séquence des jointures à deux tables. Par exemple, S'il y a quatre tables pour join clickhouse rejoint la première et la seconde, puis rejoint le résultat avec la troisième table, et à la dernière étape, il rejoint la quatrième.

Si une requête contient l' `WHERE` clickhouse essaie de pousser les filtres de cette clause à travers la jointure intermédiaire. S'il ne peut pas appliquer le filtre à chaque jointure intermédiaire, ClickHouse applique les filtres une fois toutes les jointures terminées.

Nous recommandons l' `JOIN ON` ou `JOIN USING` syntaxe pour créer des requêtes. Exemple:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

Vous pouvez utiliser des listes de tables séparées par des virgules `FROM` clause. Exemple:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

Ne mélangez pas ces syntaxes.

ClickHouse ne supporte pas directement la syntaxe avec des virgules, Nous ne recommandons donc pas de les utiliser. L'algorithme tente de réécrire la requête en termes de `CROSS JOIN` et `INNER JOIN` clauses et procède ensuite au traitement des requêtes. Lors de la réécriture de la requête, ClickHouse tente d'optimiser les performances et la consommation de mémoire. Par défaut, ClickHouse traite les virgules comme `INNER JOIN` clause et convertit `INNER JOIN` de `CROSS JOIN` lorsque l'algorithme ne peut pas garantir que `INNER JOIN` retourne les données requises.

#### Rigueur {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Produit cartésien](https://en.wikipedia.org/wiki/Cartesian_product) à partir des lignes correspondantes. C'est la norme `JOIN` comportement en SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` et `ALL` les mots clés sont les mêmes.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` l'utilisation est décrite ci-dessous.

**ASOF joindre L'utilisation**

`ASOF JOIN` est utile lorsque vous devez joindre des enregistrements qui n'ont pas de correspondance exacte.

Tables pour `ASOF JOIN` doit avoir une colonne de séquence ordonnée. Cette colonne ne peut pas être seule dans une table et doit être l'un des types de données: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, et `DateTime`.

Syntaxe `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Vous pouvez utiliser n'importe quel nombre de conditions d'égalité et exactement une condition de correspondance la plus proche. Exemple, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions prises en charge pour la correspondance la plus proche: `>`, `>=`, `<`, `<=`.

Syntaxe `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` utiliser `equi_columnX` pour rejoindre sur l'égalité et `asof_column` pour rejoindre le match le plus proche avec le `table_1.asof_column >= table_2.asof_column` condition. Le `asof_column` colonne toujours la dernière dans le `USING` clause.

Par exemple, considérez les tableaux suivants:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` peut prendre la date d'un événement utilisateur de `table_1` et trouver un événement dans `table_2` où le timestamp est plus proche de l'horodatage de l'événement à partir de `table_1` correspondant à la condition de correspondance la plus proche. Les valeurs d'horodatage égales sont les plus proches si elles sont disponibles. Ici, l' `user_id` la colonne peut être utilisée pour joindre sur l'égalité et le `ev_time` la colonne peut être utilisée pour se joindre à la correspondance la plus proche. Dans notre exemple, `event_1_1` peut être jointe à `event_2_1` et `event_1_2` peut être jointe à `event_2_3`, mais `event_2_2` ne peut pas être rejoint.

!!! note "Note"
    `ASOF` jointure est **pas** pris en charge dans le [Rejoindre](../../engines/table-engines/special/join.md) tableau moteur.

Pour définir la valeur de rigueur par défaut, utilisez le paramètre de configuration de session [join\_default\_strictness](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

Lors de l'utilisation normale `JOIN` la requête est envoyée aux serveurs distants. Les sous-requêtes sont exécutées sur chacune d'elles afin de créer la bonne table, et la jointure est effectuée avec cette table. En d'autres termes, la table de droite est formée sur chaque serveur séparément.

Lors de l'utilisation de `GLOBAL ... JOIN`, d'abord le serveur demandeur exécute une sous-requête pour calculer la bonne table. Cette table temporaire est transmise à chaque serveur distant, et les requêtes sont exécutées sur eux en utilisant les données temporaires qui ont été transmises.

Soyez prudent lorsque vous utilisez `GLOBAL`. Pour plus d'informations, consultez la section [Sous-requêtes distribuées](#select-distributed-subqueries).

#### Recommandations D'Utilisation {#usage-recommendations}

Lors de l'exécution d'un `JOIN`, il n'y a pas d'optimisation de la commande d'exécution par rapport aux autres stades de la requête. La jointure (une recherche dans la table de droite) est exécutée avant de filtrer `WHERE` et avant l'agrégation. Afin de définir explicitement l'ordre de traitement, nous vous recommandons d'exécuter une `JOIN` sous-requête avec une sous-requête.

Exemple:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

Les sous-requêtes ne vous permettent pas de définir des noms ou de les utiliser pour référencer une colonne à partir d'une sous-requête spécifique.
Les colonnes spécifiées dans `USING` doit avoir les mêmes noms dans les deux sous-requêtes, et les autres colonnes doivent être nommées différemment. Vous pouvez utiliser des alias pour les noms des colonnes dans les sous-requêtes (l'exemple utilise l'alias `hits` et `visits`).

Le `USING` clause spécifie une ou plusieurs colonnes de jointure, qui établit l'égalité de ces colonnes. La liste des colonnes est définie sans crochets. Les conditions de jointure plus complexes ne sont pas prises en charge.

La table de droite (le résultat de la sous-requête) réside dans la RAM. S'il n'y a pas assez de mémoire, vous ne pouvez pas exécuter un `JOIN`.

Chaque fois qu'une requête est exécutée avec la même `JOIN`, la sous-requête est exécutée à nouveau car le résultat n'est pas mis en cache. Pour éviter cela, utilisez la spéciale [Rejoindre](../../engines/table-engines/special/join.md) table engine, qui est un tableau préparé pour l'assemblage qui est toujours en RAM.

Dans certains cas, il est plus efficace d'utiliser `IN` plutôt `JOIN`.
Parmi les différents types de `JOIN`, le plus efficace est d' `ANY LEFT JOIN`, puis `ANY INNER JOIN`. Les moins efficaces sont `ALL LEFT JOIN` et `ALL INNER JOIN`.

Si vous avez besoin d'un `JOIN` pour se joindre à des tables de dimension (ce sont des tables relativement petites qui contiennent des propriétés de dimension, telles que des noms pour des campagnes publicitaires), un `JOIN` peut-être pas très pratique en raison du fait que la bonne table est ré-accédée pour chaque requête. Pour de tels cas, il y a un “external dictionaries” la fonctionnalité que vous devez utiliser à la place de `JOIN`. Pour plus d'informations, consultez la section [Dictionnaires externes](../dictionaries/external-dictionaries/external-dicts.md).

**Limitations De Mémoire**

ClickHouse utilise le [jointure de hachage](https://en.wikipedia.org/wiki/Hash_join) algorithme. ClickHouse prend le `<right_subquery>` et crée une table de hachage pour cela dans la RAM. Si vous devez restreindre la consommation de mémoire de l'opération join utilisez les paramètres suivants:

-   [max\_rows\_in\_join](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

Lorsque l'une de ces limites est atteinte, ClickHouse agit comme [join\_overflow\_mode](../../operations/settings/query-complexity.md#settings-join_overflow_mode) réglage des instructions.

#### Traitement des cellules vides ou nulles {#processing-of-empty-or-null-cells}

Lors de la jonction de tables, les cellules vides peuvent apparaître. Paramètre [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) définir comment clickhouse remplit ces cellules.

Si l' `JOIN` les touches sont [Nullable](../data-types/nullable.md) champs, les lignes où au moins une des clés a la valeur [NULL](../syntax.md#null-literal) ne sont pas jointes.

#### Limitations De Syntaxe {#syntax-limitations}

Pour plusieurs `JOIN` clauses dans un seul `SELECT` requête:

-   Prendre toutes les colonnes via `*` n'est disponible que si les tables sont jointes, pas les sous-requêtes.
-   Le `PREWHERE` la clause n'est pas disponible.

Pour `ON`, `WHERE`, et `GROUP BY` clause:

-   Les expressions arbitraires ne peuvent pas être utilisées dans `ON`, `WHERE`, et `GROUP BY` mais vous pouvez définir une expression dans un `SELECT` clause et ensuite l'utiliser dans ces clauses via un alias.

### Clause where {#select-where}

S'il existe une clause WHERE, elle doit contenir une expression de type UInt8. C'est généralement une expression avec comparaison et opérateurs logiques.
Cette expression est utilisée pour filtrer les données avant toutes les autres transformations.

Si les index sont pris en charge par le moteur de table de base de données, l'expression est évaluée sur la possibilité d'utiliser des index.

### Clause PREWHERE {#prewhere-clause}

Cette clause a le même sens que la clause WHERE. La différence est dans laquelle les données sont lues à partir de la table.
Lors de L'utilisation de PREWHERE, d'abord, seules les colonnes nécessaires à L'exécution de PREWHERE sont lues. Ensuite, les autres colonnes sont lues qui sont nécessaires pour exécuter la requête, mais seulement les blocs où L'expression PREWHERE est vraie.

Il est logique d'utiliser PREWHERE s'il existe des conditions de filtration qui sont utilisées par une minorité de colonnes dans la requête, mais qui fournissent une filtration de données forte. Cela réduit le volume de données à lire.

Par exemple, il est utile d'écrire PREWHERE pour les requêtes qui extraient un grand nombre de colonnes, mais qui n'ont que la filtration pour quelques colonnes.

PREWHERE est uniquement pris en charge par les tables `*MergeTree` famille.

Une requête peut spécifier simultanément PREWHERE et WHERE. Dans ce cas, PREWHERE précède WHERE.

Si l' ‘optimize\_move\_to\_prewhere’ le paramètre est défini sur 1 et PREWHERE est omis, le système utilise des heuristiques pour déplacer automatiquement des parties d'expressions D'où vers PREWHERE.

### Clause GROUP BY {#select-group-by-clause}

C'est l'une des parties les plus importantes d'un SGBD orienté colonne.

S'il existe une clause GROUP BY, elle doit contenir une liste d'expressions. Chaque expression sera appelée ici comme “key”.
Toutes les expressions des clauses SELECT, HAVING et ORDER BY doivent être calculées à partir de clés ou de fonctions d'agrégation. En d'autres termes, chaque colonne sélectionnée dans la table doit être utilisée soit dans les clés, soit dans les fonctions d'agrégation.

Si une requête ne contient que des colonnes de table dans les fonctions d'agrégation, la clause GROUP BY peut être omise et l'agrégation par un ensemble de clés vide est supposée.

Exemple:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Cependant, contrairement au SQL standard, si la table n'a pas de lignes (soit il n'y en a pas du tout, soit il n'y en a pas après avoir utilisé WHERE to filter), un résultat vide est renvoyé, et non le résultat d'une des lignes contenant les valeurs initiales des fonctions d'agrégat.

Contrairement à MySQL (et conforme à SQL standard), vous ne pouvez pas obtenir une valeur d'une colonne qui n'est pas dans une fonction clé ou agrégée (sauf les expressions constantes). Pour contourner ce problème, vous pouvez utiliser le ‘any’ fonction d'agrégation (récupère la première valeur rencontrée) ou ‘min/max’.

Exemple:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Pour chaque valeur de clé différente rencontrée, GROUP BY calcule un ensemble de valeurs de fonction d'agrégation.

GROUP BY n'est pas pris en charge pour les colonnes de tableau.

Une constante ne peut pas être spécifiée comme arguments pour les fonctions d'agrégation. Exemple: somme(1). Au lieu de cela, vous pouvez vous débarrasser de la constante. Exemple: `count()`.

#### Le Traitement NULL {#null-processing}

Pour le regroupement, ClickHouse interprète [NULL](../syntax.md#null-literal) comme une valeur, et `NULL=NULL`.

Voici un exemple pour montrer ce que cela signifie.

Supposons que vous avez cette table:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Requête `SELECT sum(x), y FROM t_null_big GROUP BY y` résultats dans:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Vous pouvez voir que `GROUP BY` pour `y = NULL` résumer `x` comme si `NULL` a cette valeur.

Si vous passez plusieurs clés `GROUP BY` le résultat vous donnera toutes les combinaisons de la sélection, comme si `NULL` ont une valeur spécifique.

#### Avec modificateur de totaux {#with-totals-modifier}

Si le modificateur avec totaux est spécifié, une autre ligne sera calculée. Cette ligne aura des colonnes clés contenant des valeurs par défaut (zéros ou lignes vides), et des colonnes de fonctions d'agrégat avec les valeurs calculées sur toutes les lignes (le “total” valeur).

Cette ligne supplémentaire est sortie dans les formats JSON\*, TabSeparated\* et Pretty\*, séparément des autres lignes. Dans les autres formats, cette ligne n'est pas sortie.

Dans les formats JSON\*, cette ligne est sortie en tant que ‘totals’ champ. Dans les formats TabSeparated\*, la ligne vient après le résultat principal, précédée d'une ligne vide (après les autres données). Dans les formats Pretty\*, la ligne est sortie sous forme de table séparée après le résultat principal.

`WITH TOTALS` peut être exécuté de différentes manières lorsqu'il est présent. Le comportement dépend de l' ‘totals\_mode’ paramètre.
Par défaut, `totals_mode = 'before_having'`. Dans ce cas, ‘totals’ est calculé sur toutes les lignes, y compris celles qui ne passent pas par ‘max\_rows\_to\_group\_by’.

Les autres alternatives incluent uniquement les lignes qui passent à travers avoir dans ‘totals’, et se comporter différemment avec le réglage `max_rows_to_group_by` et `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. En d'autres termes, ‘totals’ aura moins ou le même nombre de lignes que si `max_rows_to_group_by` ont été omis.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ dans ‘totals’. En d'autres termes, ‘totals’ aura plus ou le même nombre de lignes que si `max_rows_to_group_by` ont été omis.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ dans ‘totals’. Sinon, ne pas les inclure.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

Si `max_rows_to_group_by` et `group_by_overflow_mode = 'any'` ne sont pas utilisés, toutes les variations de `after_having` sont les mêmes, et vous pouvez utiliser l'un d'eux (par exemple, `after_having_auto`).

Vous pouvez utiliser avec les totaux dans les sous-requêtes, y compris les sous-requêtes dans la clause JOIN (dans ce cas, les valeurs totales respectives sont combinées).

#### Groupe par dans la mémoire externe {#select-group-by-in-external-memory}

Vous pouvez activer le dumping des données temporaires sur le disque pour limiter l'utilisation de la mémoire pendant `GROUP BY`.
Le [max\_bytes\_before\_external\_group\_by](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) réglage détermine le seuil de consommation de RAM pour le dumping `GROUP BY` données temporaires dans le système de fichiers. Si elle est définie sur 0 (valeur par défaut), elle est désactivée.

Lors de l'utilisation de `max_bytes_before_external_group_by`, nous vous recommandons de définir `max_memory_usage` environ deux fois plus élevé. Ceci est nécessaire car il y a deux étapes à l'agrégation: la lecture de la date et la formation des données intermédiaires (1) et la fusion des données intermédiaires (2). Le Dumping des données dans le système de fichiers ne peut se produire qu'au cours de l'étape 1. Si les données temporaires n'ont pas été vidées, l'étape 2 peut nécessiter jusqu'à la même quantité de mémoire qu'à l'étape 1.

Par exemple, si [max\_memory\_usage](../../operations/settings/settings.md#settings_max_memory_usage) a été défini sur 10000000000 et que vous souhaitez utiliser l'agrégation externe, il est logique de définir `max_bytes_before_external_group_by` à 10000000000, et max\_memory\_usage à 20000000000. Lorsque l'agrégation externe est déclenchée (s'il y a eu au moins un vidage de données temporaires), la consommation maximale de RAM n'est que légèrement supérieure à `max_bytes_before_external_group_by`.

Avec le traitement des requêtes distribuées, l'agrégation externe est effectuée sur des serveurs distants. Pour que le serveur demandeur n'utilise qu'une petite quantité de RAM, définissez `distributed_aggregation_memory_efficient` 1.

Lors de la fusion de données vidées sur le disque, ainsi que lors de la fusion des résultats de serveurs distants lorsque `distributed_aggregation_memory_efficient` paramètre est activé, consomme jusqu'à `1/256 * the_number_of_threads` à partir de la quantité totale de mémoire RAM.

Lorsque l'agrégation externe est activée, s'il y a moins de `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

Si vous avez un `ORDER BY` avec un `LIMIT` après `GROUP BY` puis la quantité de RAM dépend de la quantité de données dans `LIMIT`, pas dans l'ensemble de la table. Mais si l' `ORDER BY` n'a pas `LIMIT`, n'oubliez pas d'activer externe de tri (`max_bytes_before_external_sort`).

### Limite par Clause {#limit-by-clause}

Une requête avec l' `LIMIT n BY expressions` la clause sélectionne le premier `n` lignes pour chaque valeur distincte de `expressions`. La clé pour `LIMIT BY` peut contenir n'importe quel nombre de [expression](../syntax.md#syntax-expressions).

ClickHouse prend en charge la syntaxe suivante:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Pendant le traitement de la requête, ClickHouse sélectionne les données classées par clé de tri. La clé de tri est définie explicitement à l'aide [ORDER BY](#select-order-by) clause ou implicitement en tant que propriété du moteur de table. Puis clickhouse s'applique `LIMIT n BY expressions` et renvoie le premier `n` lignes pour chaque combinaison distincte de `expressions`. Si `OFFSET` est spécifié, puis pour chaque bloc de données qui appartient à une combinaison particulière de `expressions`, Clickhouse saute `offset_value` nombre de lignes depuis le début du bloc et renvoie un maximum de `n` les lignes en conséquence. Si `offset_value` est plus grand que le nombre de lignes dans le bloc de données, ClickHouse renvoie zéro lignes du bloc.

`LIMIT BY` n'est pas liée à `LIMIT`. Ils peuvent tous deux être utilisés dans la même requête.

**Exemple**

Exemple de table:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Requête:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Le `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` requête renvoie le même résultat.

La requête suivante renvoie les 5 principaux référents pour chaque `domain, device_type` paire avec un maximum de 100 lignes au total (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### Clause HAVING {#having-clause}

Permet de filtrer le résultat reçu après GROUP BY, similaire à la clause WHERE.
Où et ayant diffèrent en ce que Où est effectué avant l'agrégation (GROUP BY), tout en ayant est effectué après.
Si l'agrégation n'est pas effectuée, HAVING ne peut pas être utilisé.

### Clause ORDER BY {#select-order-by}

La clause ORDER BY contient une liste d'expressions, qui peuvent chacune être affectées à DESC ou ASC (la direction de tri). Si la direction n'est pas spécifiée, ASC est supposé. ASC est trié dans l'ordre croissant, et DESC dans l'ordre décroissant. La direction de tri s'applique à une seule expression, pas à la liste entière. Exemple: `ORDER BY Visits DESC, SearchPhrase`

Pour le tri par valeurs de chaîne, vous pouvez spécifier le classement (comparaison). Exemple: `ORDER BY SearchPhrase COLLATE 'tr'` - pour le tri par mot-clé dans l'ordre croissant, en utilisant l'alphabet turc, insensible à la casse, en supposant que les chaînes sont encodées en UTF-8. COLLATE peut être spécifié ou non pour chaque expression dans L'ordre par indépendamment. Si ASC ou DESC est spécifié, COLLATE est spécifié après. Lors de L'utilisation de COLLATE, le tri est toujours insensible à la casse.

Nous recommandons uniquement D'utiliser COLLATE pour le tri final d'un petit nombre de lignes, car le tri avec COLLATE est moins efficace que le tri normal par octets.

Les lignes qui ont des valeurs identiques pour la liste des expressions de tri sont sorties dans un ordre arbitraire, qui peut également être non déterministe (différent à chaque fois).
Si la clause ORDER BY est omise, l'ordre des lignes est également indéfini et peut également être non déterministe.

`NaN` et `NULL` ordre de tri:

-   Avec le modificateur `NULLS FIRST` — First `NULL`, puis `NaN` puis d'autres valeurs.
-   Avec le modificateur `NULLS LAST` — First the values, then `NaN`, puis `NULL`.
-   Default — The same as with the `NULLS LAST` modificateur.

Exemple:

Pour la table

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Exécuter la requête `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` obtenir:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Lorsque les nombres à virgule flottante sont triés, les Nan sont séparés des autres valeurs. Quel que soit l'ordre de tri, NaNs viennent à la fin. En d'autres termes, pour le Tri ascendant, ils sont placés comme s'ils étaient plus grands que tous les autres nombres, tandis que pour le Tri descendant, ils sont placés comme s'ils étaient plus petits que les autres.

Moins de RAM est utilisé si une limite assez petite est spécifiée en plus de ORDER BY. Sinon, la quantité de mémoire dépensée est proportionnelle au volume de données à trier. Pour le traitement des requêtes distribuées, si GROUP BY est omis, le tri est partiellement effectué sur des serveurs distants et les résultats sont fusionnés sur le serveur demandeur. Cela signifie que pour le tri distribué, le volume de données à trier peut être supérieur à la quantité de mémoire sur un seul serveur.

S'il N'y a pas assez de RAM, il est possible d'effectuer un tri dans la mémoire externe (création de fichiers temporaires sur un disque). Utilisez le paramètre `max_bytes_before_external_sort` pour ce but. S'il est défini sur 0 (par défaut), le tri externe est désactivé. Si elle est activée, lorsque le volume de données à trier atteint le nombre spécifié d'octets, les données collectées sont triés et déposés dans un fichier temporaire. Une fois toutes les données lues, tous les fichiers triés sont fusionnés et les résultats sont générés. Les fichiers sont écrits dans le répertoire/var/lib / clickhouse / tmp / dans la configuration (par défaut, mais vous pouvez ‘tmp\_path’ paramètre pour modifier ce paramètre).

L'exécution d'une requête peut utiliser plus de mémoire que ‘max\_bytes\_before\_external\_sort’. Pour cette raison, ce paramètre doit avoir une valeur significativement inférieure à ‘max\_memory\_usage’. Par exemple, si votre serveur dispose de 128 Go de RAM et que vous devez exécuter une seule requête, définissez ‘max\_memory\_usage’ à 100 Go, et ‘max\_bytes\_before\_external\_sort’ à 80 Go.

Le tri externe fonctionne beaucoup moins efficacement que le tri dans la RAM.

### Clause SELECT {#select-select}

[Expression](../syntax.md#syntax-expressions) spécifié dans le `SELECT` clause sont calculés après toutes les opérations dans les clauses décrites ci-dessus sont terminés. Ces expressions fonctionnent comme si elles s'appliquaient à des lignes séparées dans le résultat. Si les expressions dans le `SELECT` la clause contient des fonctions d'agrégation, puis clickhouse traite les fonctions d'agrégation et les expressions utilisées [GROUP BY](#select-group-by-clause) agrégation.

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

### La Clause DISTINCT {#select-distinct}

Si DISTINCT est spécifié, une seule ligne restera hors de tous les ensembles de lignes entièrement correspondantes dans le résultat.
Le résultat sera le même que si GROUP BY était spécifié dans tous les champs spécifiés dans SELECT without aggregate functions. Mais il y a plusieurs différences de GROUP BY:

-   DISTINCT peut être appliqué avec GROUP BY.
-   Lorsque ORDER BY est omis et que LIMIT est défini, la requête s'arrête immédiatement après la lecture du nombre requis de lignes différentes.
-   Les blocs de données sont produits au fur et à mesure qu'ils sont traités, sans attendre que la requête entière se termine.

DISTINCT n'est pas pris en charge si SELECT a au moins une colonne de tableau.

`DISTINCT` fonctionne avec [NULL](../syntax.md#null-literal) comme si `NULL` ont une valeur spécifique, et `NULL=NULL`. En d'autres termes, dans le `DISTINCT` résultats, différentes combinaisons avec `NULL` qu'une seule fois.

Clickhouse prend en charge l'utilisation du `DISTINCT` et `ORDER BY` clauses pour différentes colonnes dans une requête. Le `DISTINCT` la clause est exécutée avant la `ORDER BY` clause.

Exemple de table:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Lors de la sélection de données avec le `SELECT DISTINCT a FROM t1 ORDER BY b ASC` requête, nous obtenons le résultat suivant:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Si nous changeons la direction de tri `SELECT DISTINCT a FROM t1 ORDER BY b DESC`, nous obtenons le résultat suivant:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Rangée `2, 4` a été coupé avant de les trier.

Prenez en compte cette spécificité d'implémentation lors de la programmation des requêtes.

### Clause LIMIT {#limit-clause}

`LIMIT m` vous permet de sélectionner la première `m` lignes du résultat.

`LIMIT n, m` vous permet de sélectionner la première `m` lignes du résultat après avoir sauté le premier `n` rangée. Le `LIMIT m OFFSET n` la syntaxe est également prise en charge.

`n` et `m` doivent être des entiers non négatifs.

Si il n'y a pas un `ORDER BY` clause qui trie explicitement les résultats, le résultat peut être arbitraire et non déterministe.

### Clause UNION ALL {#union-all-clause}

Vous pouvez utiliser UNION ALL pour combiner n'importe quel nombre de requêtes. Exemple:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Seule UNION ALL est prise en charge. L'UNION régulière (Union distincte) n'est pas prise en charge. Si vous avez besoin D'UNION DISTINCT, vous pouvez écrire SELECT DISTINCT à partir d'une sous-requête contenant UNION ALL.

Les requêtes qui font partie de L'UNION peuvent toutes être exécutées simultanément et leurs résultats peuvent être mélangés.

La structure des résultats (le nombre et le type de colonnes) doit correspondre aux requêtes. Mais les noms des colonnes peuvent différer. Dans ce cas, les noms de colonne pour le résultat final seront tirés de la première requête. La coulée de Type est effectuée pour les syndicats. Par exemple, si deux requêtes combinées ont le même champ avec non-`Nullable` et `Nullable` types d'un type compatible, la `UNION ALL` a un `Nullable` type de champ.

Les requêtes qui font partie de UNION ALL ne peuvent pas être placées entre crochets. ORDER BY et LIMIT sont appliqués à des requêtes distinctes, pas au résultat final. Si vous devez appliquer une conversion au résultat final, vous pouvez placer toutes les requêtes avec UNION ALL dans une sous-requête de la clause FROM.

### Dans OUTFILE Clause {#into-outfile-clause}

Ajouter l' `INTO OUTFILE filename` clause (où filename est un littéral de chaîne) pour rediriger la sortie de la requête vers le fichier spécifié.
Contrairement à MySQL, le fichier est créé du côté client. La requête échouera si un fichier portant le même nom existe déjà.
Cette fonctionnalité est disponible dans le client de ligne de commande et clickhouse-local (une requête envoyée via L'interface HTTP échouera).

Le format de sortie par défaut est TabSeparated (le même que dans le mode batch client de ligne de commande).

### FORMAT de la Clause {#format-clause}

Spécifier ‘FORMAT format’ pour obtenir des données dans n'importe quel format spécifié.
Vous pouvez l'utiliser pour plus de commodité, ou pour créer des vidages.
Pour plus d'informations, consultez la section “Formats”.
Si la clause FORMAT est omise, le format par défaut est utilisé, ce qui dépend à la fois des paramètres et de l'interface utilisée pour accéder à la base de données. Pour L'interface HTTP et le client de ligne de commande en mode batch, le format par défaut est TabSeparated. Pour le client de ligne de commande en mode interactif, le format par défaut est PrettyCompact (il a des tables attrayantes et compactes).

Lors de l'utilisation du client de ligne de commande, les données sont transmises au client dans un format efficace interne. Le client interprète indépendamment la clause de FORMAT de la requête et formate les données elles-mêmes (soulageant ainsi le réseau et le serveur de la charge).

### Dans les opérateurs {#select-in-operators}

Le `IN`, `NOT IN`, `GLOBAL IN`, et `GLOBAL NOT IN` les opérateurs sont traitées séparément, car leur fonctionnalité est assez riche.

Le côté gauche de l'opérateur, soit une seule colonne ou un tuple.

Exemple:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Si le côté gauche est une colonne unique qui est dans l'index, et le côté droit est un ensemble de constantes, le système utilise l'index pour le traitement de la requête.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”), puis utiliser une sous-requête.

Le côté droit de l'opérateur peut être un ensemble d'expressions constantes, un ensemble de tuples avec des expressions constantes (illustrées dans les exemples ci-dessus), ou le nom d'une table de base de données ou une sous-requête SELECT entre parenthèses.

Si le côté droit de l'opérateur est le nom d'une table (par exemple, `UserID IN users`), ceci est équivalent à la sous-requête `UserID IN (SELECT * FROM users)`. Utilisez ceci lorsque vous travaillez avec des données externes envoyées avec la requête. Par exemple, la requête peut être envoyée avec un ensemble d'ID utilisateur chargés dans le ‘users’ table temporaire, qui doit être filtrée.

Si le côté droit de l'opérateur est un nom de table qui a le moteur Set (un ensemble de données préparé qui est toujours en RAM), l'ensemble de données ne sera pas créé à nouveau pour chaque requête.

La sous-requête peut spécifier plusieurs colonnes pour filtrer les tuples.
Exemple:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

Les colonnes à gauche et à droite de l'opérateur doit avoir le même type.

L'opérateur IN et la sous-requête peuvent se produire dans n'importe quelle partie de la requête, y compris dans les fonctions d'agrégation et les fonctions lambda.
Exemple:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

Pour chaque jour après le 17 mars, comptez le pourcentage de pages vues par les utilisateurs qui ont visité le site le 17 mars.
Une sous-requête dans la clause est toujours exécuter une seule fois sur un seul serveur. Il n'y a pas de sous-requêtes dépendantes.

#### Le Traitement NULL {#null-processing-1}

Pendant le traitement de la demande, l'opérateur n'assume que le résultat d'une opération avec [NULL](../syntax.md#null-literal) est toujours égale à `0` indépendamment de savoir si `NULL` est sur le côté droit ou gauche de l'opérateur. `NULL` les valeurs ne sont incluses dans aucun jeu de données, ne correspondent pas entre elles et ne peuvent pas être comparées.

Voici un exemple avec le `t_null` table:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

L'exécution de la requête `SELECT x FROM t_null WHERE y IN (NULL,3)` vous donne le résultat suivant:

``` text
┌─x─┐
│ 2 │
└───┘
```

Vous pouvez voir que la ligne dans laquelle `y = NULL` est jeté hors de résultats de la requête. C'est parce que ClickHouse ne peut pas décider si `NULL` est inclus dans le `(NULL,3)` ensemble, les retours `0` comme le résultat de l'opération, et `SELECT` exclut cette ligne de la sortie finale.

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### Sous-Requêtes Distribuées {#select-distributed-subqueries}

Il y a deux options pour IN-S avec des sous-requêtes (similaires aux jointures): normal `IN` / `JOIN` et `GLOBAL IN` / `GLOBAL JOIN`. Ils diffèrent dans la façon dont ils sont exécutés pour le traitement des requêtes distribuées.

!!! attention "Attention"
    Rappelez-vous que les algorithmes décrits ci-dessous peuvent travailler différemment en fonction de la [paramètre](../../operations/settings/settings.md) `distributed_product_mode` paramètre.

Lors de l'utilisation de l'IN régulier, la requête est envoyée à des serveurs distants, et chacun d'eux exécute les sous-requêtes dans le `IN` ou `JOIN` clause.

Lors de l'utilisation de `GLOBAL IN` / `GLOBAL JOINs`, d'abord toutes les sous-requêtes sont exécutées pour `GLOBAL IN` / `GLOBAL JOINs`, et les résultats sont recueillis dans des tableaux temporaires. Ensuite, les tables temporaires sont envoyés à chaque serveur distant, où les requêtes sont exécutées à l'aide temporaire de données.

Pour une requête non distribuée, utilisez `IN` / `JOIN`.

Soyez prudent lorsque vous utilisez des sous-requêtes dans le `IN` / `JOIN` clauses pour le traitement des requêtes distribuées.

Regardons quelques exemples. Supposons que chaque serveur du cluster a un **local\_table**. Chaque serveur dispose également d'une **table distributed\_table** table avec le **Distribué** type, qui regarde tous les serveurs du cluster.

Pour une requête à l' **table distributed\_table**, la requête sera envoyée à tous les serveurs distants et exécutée sur eux en utilisant le **local\_table**.

Par exemple, la requête

``` sql
SELECT uniq(UserID) FROM distributed_table
```

sera envoyé à tous les serveurs distants

``` sql
SELECT uniq(UserID) FROM local_table
```

et l'exécuter sur chacun d'eux en parallèle, jusqu'à ce qu'il atteigne le stade où les résultats intermédiaires peuvent être combinés. Ensuite, les résultats intermédiaires seront retournés au demandeur de serveur et de fusion, et le résultat final sera envoyé au client.

Examinons maintenant une requête avec IN:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   Calcul de l'intersection des audiences de deux sites.

Cette requête sera envoyée à tous les serveurs distants

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

En d'autres termes, l'ensemble de données de la clause IN sera collecté sur chaque serveur indépendamment, uniquement à travers les données stockées localement sur chacun des serveurs.

Cela fonctionnera correctement et de manière optimale si vous êtes prêt pour ce cas et que vous avez réparti les données entre les serveurs de cluster de telle sorte que les données d'un seul ID utilisateur résident entièrement sur un seul serveur. Dans ce cas, toutes les données nécessaires seront disponibles localement sur chaque serveur. Sinon, le résultat sera erroné. Nous nous référons à cette variation de la requête que “local IN”.

Pour corriger le fonctionnement de la requête lorsque les données sont réparties aléatoirement sur les serveurs de cluster, vous pouvez spécifier **table distributed\_table** à l'intérieur d'une sous-requête. La requête ressemblerait à ceci:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Cette requête sera envoyée à tous les serveurs distants

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

La sous-requête commencera à s'exécuter sur chaque serveur distant. Étant donné que la sous-requête utilise une table distribuée, la sous-requête qui se trouve sur chaque serveur distant sera renvoyée à chaque serveur distant comme

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Par exemple, si vous avez un cluster de 100 SERVEURS, l'exécution de la requête entière nécessitera 10 000 requêtes élémentaires, ce qui est généralement considéré comme inacceptable.

Dans de tels cas, vous devez toujours utiliser GLOBAL IN au lieu de IN. Voyons comment cela fonctionne pour la requête

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Le serveur demandeur exécutera la sous requête

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

et le résultat sera mis dans une table temporaire en RAM. Ensuite, la demande sera envoyée à chaque serveur distant

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

et la table temporaire `_data1` sera envoyé à chaque serveur distant avec la requête (le nom de la table temporaire est défini par l'implémentation).

Ceci est plus optimal que d'utiliser la normale dans. Cependant, gardez les points suivants à l'esprit:

1.  Lors de la création d'une table temporaire, les données ne sont pas uniques. Pour réduire le volume de données transmises sur le réseau, spécifiez DISTINCT dans la sous-requête. (Vous n'avez pas besoin de le faire pour un IN normal.)
2.  La table temporaire sera envoyé à tous les serveurs distants. La Transmission ne tient pas compte de la topologie du réseau. Par exemple, si 10 serveurs distants résident dans un centre de données très distant par rapport au serveur demandeur, les données seront envoyées 10 fois sur le canal au centre de données distant. Essayez d'éviter les grands ensembles de données lorsque vous utilisez GLOBAL IN.
3.  Lors de la transmission de données à des serveurs distants, les restrictions sur la bande passante réseau ne sont pas configurables. Vous pourriez surcharger le réseau.
4.  Essayez de distribuer les données entre les serveurs afin que vous n'ayez pas besoin D'utiliser GLOBAL IN sur une base régulière.
5.  Si vous devez utiliser GLOBAL in souvent, planifiez l'emplacement du cluster ClickHouse de sorte qu'un seul groupe de répliques ne réside pas dans plus d'un centre de données avec un réseau rapide entre eux, de sorte qu'une requête puisse être traitée entièrement dans un seul centre de données.

Il est également judicieux de spécifier une table locale dans le `GLOBAL IN` clause, dans le cas où cette table locale est uniquement disponible sur le serveur demandeur et que vous souhaitez utiliser les données de celui-ci sur des serveurs distants.

### Les Valeurs Extrêmes {#extreme-values}

En plus des résultats, vous pouvez également obtenir des valeurs minimales et maximales pour les colonnes de résultats. Pour ce faire, définissez la **extrême** réglage sur 1. Les Minimums et les maximums sont calculés pour les types numériques, les dates et les dates avec des heures. Pour les autres colonnes, les valeurs par défaut sont sorties.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, et `Pretty*` [format](../../interfaces/formats.md), séparés des autres lignes. Ils ne sont pas Produits pour d'autres formats.

Dans `JSON*` formats, les valeurs extrêmes sont sorties dans un ‘extremes’ champ. Dans `TabSeparated*` formats, la ligne vient après le résultat principal, et après ‘totals’ si elle est présente. Elle est précédée par une ligne vide (après les autres données). Dans `Pretty*` formats, la ligne est sortie comme une table séparée après le résultat principal, et après `totals` si elle est présente.

Les valeurs extrêmes sont calculées pour les lignes avant `LIMIT` mais après `LIMIT BY`. Cependant, lors de l'utilisation de `LIMIT offset, size`, les lignes avant de les `offset` sont inclus dans `extremes`. Dans les requêtes de flux, le résultat peut également inclure un petit nombre de lignes qui ont traversé `LIMIT`.

### Note {#notes}

Le `GROUP BY` et `ORDER BY` les clauses ne supportent pas les arguments positionnels. Cela contredit MySQL, mais est conforme à SQL standard.
Exemple, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

Vous pouvez utiliser des synonymes (`AS` alias) dans n'importe quelle partie d'une requête.

Vous pouvez mettre un astérisque dans quelque partie de la requête au lieu d'une expression. Lorsque la requête est analysée, l'astérisque est étendu à une liste de toutes les colonnes `MATERIALIZED` et `ALIAS` colonne). Il n'y a que quelques cas où l'utilisation d'un astérisque est justifiée:

-   Lors de la création d'un vidage de table.
-   Pour les tables contenant seulement quelques colonnes, comme les tables système.
-   Pour obtenir des informations sur ce que sont les colonnes dans une table. Dans ce cas, la valeur `LIMIT 1`. Mais il est préférable d'utiliser la `DESC TABLE` requête.
-   Quand il y a une forte filtration sur un petit nombre de colonnes en utilisant `PREWHERE`.
-   Dans les sous-requêtes (puisque les colonnes qui ne sont pas nécessaires pour la requête externe sont exclues des sous-requêtes).

Dans tous les autres cas, nous ne recommandons pas d'utiliser l'astérisque, car il ne vous donne que les inconvénients d'un SGBD colonnaire au lieu des avantages. En d'autres termes, l'utilisation de l'astérisque n'est pas recommandée.

[Article Original](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
