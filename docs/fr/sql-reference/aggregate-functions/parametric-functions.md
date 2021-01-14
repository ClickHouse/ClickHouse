---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "Param\xE9trique"
---

# Fonctions D'Agrégat Paramétriques {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## histogramme {#histogram}

Calcule un histogramme adaptatif. Cela ne garantit pas des résultats précis.

``` sql
histogram(number_of_bins)(values)
```

Les fonctions utilise [Un Algorithme D'Arbre De Décision Parallèle En Continu](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Les bordures des bacs d'histogramme sont ajustées au fur et à mesure que de nouvelles données entrent dans une fonction. Dans le cas courant, les largeurs des bacs ne sont pas égales.

**Paramètre**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [Expression](../syntax.md#syntax-expressions) résultant en valeurs d'entrée.

**Valeurs renvoyées**

-   [Tableau](../../sql-reference/data-types/array.md) de [Tuple](../../sql-reference/data-types/tuple.md) de le format suivant:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**Exemple**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Vous pouvez visualiser un histogramme avec la [bar](../../sql-reference/functions/other-functions.md#function-bar) fonction, par exemple:

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

Dans ce cas, vous devez vous rappeler que vous ne connaissez pas les frontières de la corbeille d'histogramme.

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

Vérifie si la séquence contient une chaîne d'événements qui correspond au modèle.

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "Avertissement"
    Les événements qui se produisent à la même seconde peuvent se situer dans la séquence dans un ordre indéfini affectant le résultat.

**Paramètre**

-   `pattern` — Pattern string. See [Syntaxe du motif](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` et `DateTime`. Vous pouvez également utiliser les prises en charge [UInt](../../sql-reference/data-types/int-uint.md) types de données.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. Vous pouvez passer jusqu'à 32 arguments de condition. La fonction ne prend en compte que les événements décrits dans ces conditions. Si la séquence contient des données qui ne sont pas décrites dans une condition, la fonction les ignore.

**Valeurs renvoyées**

-   1, si le profil correspond.
-   0, si le motif ne correspond pas.

Type: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**Syntaxe du motif**

-   `(?N)` — Matches the condition argument at position `N`. Les Conditions sont numérotées dans le `[1, 32]` gamme. Exemple, `(?1)` correspond à l'argument passé au `cond1` paramètre.

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` correspond à des événements qui se produisent plus de 1800 secondes les uns des autres. Un nombre arbitraire d'événements peut se trouver entre ces événements. Vous pouvez utiliser l' `>=`, `>`, `<`, `<=` opérateur.

**Exemple**

Considérer les données dans le `t` table:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Effectuer la requête:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

La fonction a trouvé la chaîne d'événements où le numéro 2 suit le numéro 1. Il a sauté le numéro 3 entre eux, car le nombre n'est pas décrit comme un événement. Si nous voulons prendre ce nombre en compte lors de la recherche de l'événement de la chaîne donnée dans l'exemple, nous devrions en faire une condition.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

Dans ce cas, la fonction n'a pas pu trouver la chaîne d'événements correspondant au modèle, car l'événement pour le numéro 3 s'est produit entre 1 et 2. Si dans le même cas nous vérifions la condition pour le numéro 4, la séquence correspondrait au motif.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Voir Aussi**

-   [sequenceCount](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

Compte le nombre de chaînes d'événements correspondant au motif. La fonction recherche les chaînes d'événements qui ne se chevauchent pas. Il commence à rechercher la chaîne suivante après que la chaîne actuelle est appariée.

!!! warning "Avertissement"
    Les événements qui se produisent à la même seconde peuvent se situer dans la séquence dans un ordre indéfini affectant le résultat.

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Paramètre**

-   `pattern` — Pattern string. See [Syntaxe du motif](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` et `DateTime`. Vous pouvez également utiliser les prises en charge [UInt](../../sql-reference/data-types/int-uint.md) types de données.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. Vous pouvez passer jusqu'à 32 arguments de condition. La fonction ne prend en compte que les événements décrits dans ces conditions. Si la séquence contient des données qui ne sont pas décrites dans une condition, la fonction les ignore.

**Valeurs renvoyées**

-   Nombre de chaînes d'événements qui ne se chevauchent pas et qui sont mises en correspondance.

Type: `UInt64`.

**Exemple**

Considérer les données dans le `t` table:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Comptez combien de fois le nombre 2 se produit après le nombre 1 avec n'importe quelle quantité d'autres nombres entre eux:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Voir Aussi**

-   [sequenceMatch](#function-sequencematch)

## fenêtrefunnel {#windowfunnel}

Recherche les chaînes d'événements dans une fenêtre de temps coulissante et calcule le nombre maximum d'événements qui se sont produits à partir de la chaîne.

La fonction fonctionne selon l'algorithme:

-   La fonction recherche les données qui déclenchent la première condition de la chaîne et définit le compteur d'événements sur 1. C'est le moment où la fenêtre coulissante commence.

-   Si les événements de la chaîne se produisent séquentiellement dans la fenêtre, le compteur est incrémenté. Si la séquence d'événements est perturbée, le compteur n'est pas incrémenté.

-   Si les données ont plusieurs chaînes d'événements à différents points d'achèvement, la fonction affichera uniquement la taille de la chaîne la plus longue.

**Syntaxe**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**Paramètre**

-   `window` — Length of the sliding window in seconds.
-   `mode` - C'est un argument facultatif.
    -   `'strict'` - Lorsque le `'strict'` est défini, le windowFunnel() applique des conditions uniquement pour les valeurs uniques.
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) et d'autres types entiers non signés (notez que même si timestamp prend en charge le `UInt64` type, sa valeur ne peut pas dépasser le maximum Int64, qui est 2^63 - 1).
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeur renvoyée**

Nombre maximal de conditions déclenchées consécutives de la chaîne dans la fenêtre de temps de glissement.
Toutes les chaînes de la sélection sont analysés.

Type: `Integer`.

**Exemple**

Déterminer si une période de temps est suffisant pour l'utilisateur de sélectionner un téléphone et d'acheter deux fois dans la boutique en ligne.

Définissez la chaîne d'événements suivante:

1.  L'utilisateur s'est connecté à son compte sur le magasin (`eventID = 1003`).
2.  L'utilisateur recherche un téléphone (`eventID = 1007, product = 'phone'`).
3.  Toute commande de l'utilisateur (`eventID = 1009`).
4.  L'Utilisateur a fait la commande à nouveau (`eventID = 1010`).

Table d'entrée:

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

Savoir dans quelle mesure l'utilisateur `user_id` pourrait passer à travers la chaîne dans une période en Janvier-Février de 2019.

Requête:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC
```

Résultat:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## rétention {#retention}

La fonction prend comme arguments un ensemble de conditions de 1 à 32 arguments de type `UInt8` qui indiquent si une certaine condition est remplie pour l'événement.
Toute condition peut être spécifiée comme argument (comme dans [WHERE](../../sql-reference/statements/select/where.md#select-where)).

Les conditions, à l'exception de la première, s'appliquent par paires: le résultat de la seconde sera vrai si la première et la deuxième sont remplies, le troisième si la première et la fird sont vraies, etc.

**Syntaxe**

``` sql
retention(cond1, cond2, ..., cond32);
```

**Paramètre**

-   `cond` — an expression that returns a `UInt8` résultat (1 ou 0).

**Valeur renvoyée**

Le tableau de 1 ou 0.

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

Type: `UInt8`.

**Exemple**

Prenons un exemple de calcul de la `retention` fonction pour déterminer le trafic du site.

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Table d'entrée:

Requête:

``` sql
SELECT * FROM retention_test
```

Résultat:

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** Grouper les utilisateurs par ID unique `uid` à l'aide de la `retention` fonction.

Requête:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

Résultat:

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** Calculer le nombre total de visites par jour.

Requête:

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

Résultat:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Où:

-   `r1`- le nombre de visiteurs uniques qui ont visité le site au cours du 2020-01-01 (le `cond1` condition).
-   `r2`- le nombre de visiteurs uniques qui ont visité le site au cours d'une période donnée entre 2020-01-01 et 2020-01-02 (`cond1` et `cond2` condition).
-   `r3`- le nombre de visiteurs uniques qui ont visité le site au cours d'une période donnée entre 2020-01-01 et 2020-01-03 (`cond1` et `cond3` condition).

## uniqUpTo (N) (x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

Recommandé pour une utilisation avec de petites Ns, jusqu'à 10. La valeur maximale de N est de 100.

Pour l'état d'une fonction d'agrégation, il utilise la quantité de mémoire égale à 1 + N \* de la taille d'une valeur d'octets.
Pour les chaînes, il stocke un hachage non cryptographique de 8 octets. Soit le calcul est approchée pour les chaînes.

La fonction fonctionne également pour plusieurs arguments.

Cela fonctionne aussi vite que possible, sauf dans les cas où une grande valeur N est utilisée et le nombre de valeurs uniques est légèrement inférieur à N.

Exemple d'utilisation:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[Article Original](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys_to_keep) (clés, valeurs) {#summapfilteredkeys-to-keepkeys-values}

Même comportement que [sumMap](reference.md#agg_functions-summap) sauf qu'un tableau de clés est passé en paramètre. Cela peut être particulièrement utile lorsque vous travaillez avec une forte cardinalité de touches.
