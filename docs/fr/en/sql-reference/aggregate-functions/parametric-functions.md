---
description: 'Documentation des fonctions d’agrégation paramétriques'
sidebar_label: 'Paramétriques'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: 'Fonctions d’agrégation paramétriques'
doc_type: 'reference'
---

Certaines fonctions d’agrégation peuvent accepter non seulement des colonnes en argument (utilisées pour la compression), mais aussi un ensemble de paramètres — des constantes d’initialisation. La syntaxe utilise deux paires de parenthèses au lieu d’une seule. La première est destinée aux paramètres, la seconde aux arguments.

<div id="histogram">
  ## histogram
</div>

Calcule un histogramme adaptatif. Cette fonction ne garantit pas des résultats précis.

```sql
histogram(number_of_bins)(values)
```

La fonction utilise [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Les bornes des classes de l’histogramme sont ajustées à mesure que de nouvelles données arrivent dans la fonction. En général, la largeur des classes n’est pas uniforme.

**Arguments**

`values` — [Expression](/fr/sql-reference/syntax#expressions) produisant des valeurs d’entrée.

**Paramètres**

`number_of_bins` — Limite supérieure du nombre de classes dans l’histogramme. La fonction calcule automatiquement le nombre de classes. Elle essaie d’atteindre le nombre de classes spécifié, mais si elle n’y parvient pas, elle en utilise moins.

**Valeurs renvoyées**

* [Array](../../sql-reference/data-types/array.md) de [Tuples](../../sql-reference/data-types/tuple.md) au format suivant :

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — Borne inférieure de la classe.
  * `upper` — Borne supérieure de la classe.
  * `height` — Hauteur calculée de la classe.

**Exemple**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Vous pouvez représenter un histogramme à l’aide de la fonction [bar](/fr/sql-reference/functions/other-functions#bar), par exemple :

```sql
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

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

Dans ce cas, gardez à l’esprit que vous ne connaissez pas les bornes des classes de l’histogramme.

<div id="sequencematch">
  ## sequenceMatch
</div>

Vérifie si la séquence contient une chaîne d&#39;événements correspondant au motif.

**Syntaxe**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
Les événements qui se produisent à la même seconde peuvent apparaître dans la séquence dans un ordre indéfini, ce qui peut affecter le résultat.
:::

**Arguments**

* `timestamp` — Colonne considérée comme contenant des données temporelles. Les types de données habituels sont `Date` et `DateTime`. Vous pouvez également utiliser n’importe lequel des types de données [UInt](../../sql-reference/data-types/int-uint.md) pris en charge.

* `cond1`, `cond2` — Conditions décrivant la chaîne d’événements. Type de données : `UInt8`. Vous pouvez transmettre jusqu’à 32 arguments de condition. La fonction ne prend en compte que les événements décrits par ces conditions. Si la séquence contient des données qui ne sont décrites dans aucune condition, la fonction les ignore.

**Paramètres**

* `pattern` — Chaîne du motif. Voir [Syntaxe du motif](#pattern-syntax).

**Valeurs renvoyées**

* 1, si le motif correspond.
* 0, si le motif ne correspond pas.

Type : `UInt8`.

<div id="pattern-syntax">
  #### Syntaxe du motif
</div>

* `(?N)` — Correspond à l&#39;argument de condition à la position `N`. Les conditions sont numérotées dans l&#39;intervalle `[1, 32]`. Par exemple, `(?1)` correspond à l&#39;argument transmis au paramètre `cond1`.

* `.*` — Correspond à un nombre quelconque d&#39;événements. Vous n&#39;avez pas besoin d&#39;arguments de condition pour que cet élément du motif corresponde.

* `(?t operator value)` — Définit, en secondes, l&#39;intervalle qui doit séparer deux événements. Par exemple, le motif `(?1)(?t>1800)(?2)` correspond à des événements espacés de plus de 1800 secondes. Un nombre arbitraire d&#39;événements quelconques peut se trouver entre eux. Vous pouvez utiliser les opérateurs `>=`, `>`, `<`, `<=`, `==`.

**Exemples**

Considérez les données de la table `t` :

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Exécutez la requête :

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

La fonction a trouvé la chaîne d’événements dans laquelle le nombre 2 suit le nombre 1. Elle a ignoré le nombre 3 entre les deux, car ce nombre n’est pas défini comme un événement. Si nous voulons tenir compte de ce nombre lors de la recherche de la chaîne d’événements donnée dans l’exemple, nous devons lui définir une condition.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

Dans ce cas, la fonction n’a pas pu trouver de chaîne d’événements correspondant au motif, car l’événement correspondant au nombre 3 s’est produit entre 1 et 2. Si, dans ce même cas, nous vérifiions la condition pour le nombre 4, la séquence correspondrait au motif.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Voir aussi**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

Compte le nombre de chaînes d’événements correspondant au motif. La fonction recherche des chaînes d’événements qui ne se chevauchent pas. Elle commence à rechercher la chaîne suivante après la correspondance de la chaîne en cours.

:::note
Les événements qui se produisent au cours de la même seconde peuvent apparaître dans la séquence dans un ordre indéfini, ce qui affecte le résultat.
:::

**Syntax**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Arguments**

* `timestamp` — Colonne considérée comme contenant des données temporelles. Les types de données les plus courants sont `Date` et `DateTime`. Vous pouvez également utiliser n’importe lequel des types de données [UInt](../../sql-reference/data-types/int-uint.md) pris en charge.

* `cond1`, `cond2` — Conditions qui décrivent la chaîne d’événements. Type de données : `UInt8`. Vous pouvez transmettre jusqu’à 32 arguments de condition. La fonction prend uniquement en compte les événements décrits par ces conditions. Si la séquence contient des données qui ne sont décrites dans aucune condition, la fonction les ignore.

**Paramètres**

* `pattern` — Chaîne du motif. Voir [Syntaxe du motif](#pattern-syntax).

**Valeurs renvoyées**

* Nombre de chaînes d’événements non chevauchantes correspondantes.

Type : `UInt64`.

**Exemple**

Considérez les données de la table `t` :

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Comptez le nombre de fois où le chiffre 2 apparaît après le chiffre 1, quel que soit le nombre d’autres valeurs entre les deux :

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

Renvoie les horodatages des événements des plus longues chaînes d’événements correspondant au motif.

:::note
Les événements qui se produisent dans la même seconde peuvent se retrouver dans la séquence dans un ordre indéfini, ce qui affecte le résultat.
:::

**Syntaxe**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**Arguments**

* `timestamp` — Colonne considérée comme contenant des données temporelles. Les types de données habituels sont `Date` et `DateTime`. Vous pouvez également utiliser n’importe lequel des types de données [UInt](../../sql-reference/data-types/int-uint.md) pris en charge.

* `cond1`, `cond2` — Conditions qui décrivent la chaîne d’événements. Type de données : `UInt8`. Vous pouvez fournir jusqu’à 32 arguments de condition. La fonction prend uniquement en compte les événements décrits dans ces conditions. Si la séquence contient des données qui ne sont pas décrites dans une condition, la fonction les ignore.

**Paramètres**

* `pattern` — Chaîne du motif. Voir [Syntaxe du motif](#pattern-syntax).

**Valeurs renvoyées**

* Tableau des horodatages pour les arguments de condition correspondants (?N) de la chaîne d’événements. La position dans le tableau correspond à la position de l’argument de condition dans le motif.

Type : Array.

**Exemple**

Considérons les données de la table `t` :

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Renvoie les horodatages des événements de la chaîne la plus longue

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Voir aussi**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

Recherche des chaînes d’événements dans une fenêtre temporelle glissante et calcule le nombre maximal d’événements de la chaîne qui se sont produits.

La fonction repose sur l’algorithme suivant :

* La fonction recherche les données qui déclenchent la première condition de la chaîne et initialise le compteur d’événements à 1. C’est à ce moment que la fenêtre glissante commence.

* Si des événements de la chaîne se produisent dans l’ordre à l’intérieur de la fenêtre, le compteur est incrémenté. Si la séquence d’événements est interrompue, le compteur n’est pas incrémenté.

* Si les données comportent plusieurs chaînes d’événements à différents stades d’achèvement, la fonction renvoie uniquement la longueur de la chaîne la plus longue.

**Syntaxe**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**Arguments**

* `timestamp` — Nom de la colonne contenant l’horodatage. Types de données pris en charge : [Date](../../sql-reference/data-types/date.md), [DateTime](/fr/sql-reference/data-types/datetime) et autres types entiers non signés (notez que, même si `timestamp` prend en charge le type `UInt64`, sa valeur ne peut pas dépasser la valeur maximale de `Int64`, soit 2^63 - 1).
* `cond` — Conditions ou données décrivant la chaîne d’événements. [UInt8](../../sql-reference/data-types/int-uint.md).

**Paramètres**

* `window` — Longueur de la fenêtre glissante ; il s’agit de l’intervalle de temps entre la première et la dernière condition. L’unité de `window` dépend de `timestamp` et varie selon le cas. Elle est déterminée à l’aide de l’expression `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.
* `mode` — Argument facultatif. Vous pouvez définir un ou plusieurs modes.
  * `'strict_deduplication'` — Si une même condition est remplie dans la séquence d’événements, cet événement répété interrompt alors le traitement ultérieur. Remarque : le comportement peut être inattendu si plusieurs conditions sont remplies pour le même événement.
  * `'strict_order'` — N’autorise pas l’intercalation d’autres événements. Par exemple, dans le cas de `A->B->D->C`, la détection de `A->B->C` s’arrête à `D` et le niveau d’événement maximal est 2.
  * `'strict_increase'` — Applique les conditions uniquement aux événements dont les horodatages sont strictement croissants.
  * `'strict_once'` — Compte chaque événement une seule fois dans la chaîne, même s’il remplit la condition plusieurs fois.
  * `'allow_reentry'` — Ignore les événements qui ne respectent pas l’ordre strict. Par exemple, dans le cas de A-&gt;A-&gt;B-&gt;C, il trouve A-&gt;B-&gt;C en ignorant le A redondant, et le niveau d’événement maximal est 3.

**Valeur renvoyée**

Le nombre maximal de conditions consécutives déclenchées dans la chaîne à l’intérieur de la fenêtre de temps glissante.
Toutes les chaînes de la sélection sont analysées.

Type : `Integer`.

**Exemple**

Déterminez si une période donnée est suffisante pour qu’un utilisateur sélectionne un téléphone et l’achète deux fois dans la boutique en ligne.

Définissez la chaîne d’événements suivante :

1. L’utilisateur s’est connecté à son compte sur la boutique (`eventID = 1003`).
2. L’utilisateur recherche un téléphone (`eventID = 1007, product = 'phone'`).
3. L’utilisateur a passé une commande (`eventID = 1009`).
4. L’utilisateur a repassé la commande (`eventID = 1010`).

Table d’entrée :

```text
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

Déterminez jusqu’où l’utilisateur `user_id` a pu aller dans la chaîne au cours de la période janvier-février 2019.

```sql title="Query"
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
ORDER BY level ASC;
```

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**Exemple du mode allow&#95;reentry**

Cet exemple montre comment le mode `allow_reentry` fonctionne avec les schémas de réentrée des utilisateurs :

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

La fonction prend en argument un ensemble de conditions, de 1 à 32 arguments de type `UInt8`, qui indiquent si une condition donnée a été remplie pour l&#39;événement.
N&#39;importe quelle condition peut être spécifiée comme argument (comme dans [WHERE](/fr/sql-reference/statements/select/where)).

Les conditions, à l&#39;exception de la première, s&#39;appliquent par paires : le résultat de la deuxième sera `true` si la première et la deuxième sont `true`, celui de la troisième si la première et la troisième sont `true`, etc.

**Syntaxe**

```sql
retention(cond1, cond2, ..., cond32);
```

**Arguments**

* `cond` — Une expression qui renvoie un résultat `UInt8` (1 ou 0).

**Valeur renvoyée**

Le tableau de 1 ou de 0.

* 1 — La condition est remplie pour l’événement.
* 0 — La condition n’est pas remplie pour l’événement.

Type : `UInt8`.

**Exemple**

Prenons un exemple de calcul de la fonction `retention` pour déterminer le trafic du site.

**1.** Créez une table pour illustrer l’exemple.

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Table d’entrée :

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
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

**2.** Regroupez les utilisateurs selon leur identifiant unique `uid` à l’aide de la fonction `retention`.

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
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

**3.** Calculez le nombre total de visites sur le site par jour.

```sql title="Query"
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

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Où :

* `r1`- le nombre de visiteurs uniques ayant visité le site le 2020-01-01 (condition `cond1`).
* `r2`- le nombre de visiteurs uniques ayant visité le site pendant une période spécifique comprise entre le 2020-01-01 et le 2020-01-02 (conditions `cond1` et `cond2`).
* `r3`- le nombre de visiteurs uniques ayant visité le site pendant une période spécifique les 2020-01-01 et 2020-01-03 (conditions `cond1` et `cond3`).

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

Calcule le nombre de valeurs distinctes de l’argument jusqu’à une limite donnée, `N`. Si le nombre de valeurs d’argument distinctes est supérieur à `N`, cette fonction renvoie `N` + 1 ; sinon, elle calcule la valeur exacte.

Son utilisation est recommandée pour de petites valeurs de `N`, jusqu’à 10. La valeur maximale de `N` est 100.

Pour l’état d’une fonction d’agrégation, cette fonction utilise une quantité de mémoire égale à 1 + `N` * la taille en octets d’une valeur.
Lorsqu’elle traite des chaînes de caractères, cette fonction stocke un hash non cryptographique de 8 octets ; le calcul est approximatif pour les chaînes de caractères.

Par exemple, si vous aviez une table qui enregistre chaque requête de recherche effectuée par les utilisateurs sur votre site web. Chaque ligne de la table représente une seule requête de recherche, avec des colonnes pour l’ID utilisateur, la requête de recherche et le timestamp de la requête. Vous pouvez utiliser `uniqUpTo` pour générer un rapport qui n’affiche que les mots-clés ayant généré au moins 5 utilisateurs uniques.

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` calcule le nombre de valeurs `UserID` distinctes pour chaque `SearchPhrase`, mais seulement jusqu’à 4 valeurs distinctes. S’il existe plus de 4 valeurs `UserID` distinctes pour un `SearchPhrase`, la fonction renvoie 5 (4 + 1). La clause `HAVING` filtre ensuite les valeurs `SearchPhrase` pour lesquelles le nombre de valeurs `UserID` distinctes est inférieur à 5. Vous obtiendrez ainsi une liste de mots-clés de recherche utilisés par au moins 5 utilisateurs distincts.

<div id="summapfiltered">
  ## sumMapFiltered
</div>

Cette fonction se comporte comme [sumMap](/fr/sql-reference/aggregate-functions/reference/summap), sauf qu’elle accepte aussi, en paramètre, un Array de clés servant de filtre. Cela peut être particulièrement utile en présence d’une forte cardinalité de clés.

**Syntaxe**

`sumMapFiltered(keys_to_keep)(keys, values)`

**Paramètres**

* `keys_to_keep` : [Array](../data-types/array.md) de clés à utiliser comme filtre.
* `keys` : [Array](../data-types/array.md) de clés.
* `values` : [Array](../data-types/array.md) de valeurs.

**Valeur renvoyée**

* Renvoie un tuple de deux Array : les clés triées et les valeurs sommées pour les clés correspondantes.

**Exemple**

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

Cette fonction se comporte comme [sumMap](/fr/sql-reference/aggregate-functions/reference/summap), à ceci près qu&#39;elle accepte aussi en paramètre un Array de clés à utiliser comme filtre. Cela peut être particulièrement utile lorsque vous travaillez avec une forte cardinalité de clés. Elle diffère de la fonction [sumMapFiltered](#summapfiltered) en ce qu&#39;elle effectue une sommation avec dépassement de capacité — c.-à-d. qu&#39;elle renvoie pour la sommation le même type de données que celui de l&#39;argument.

**Syntaxe**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**Paramètres**

* `keys_to_keep` : [Array](../data-types/array.md) de clés à utiliser comme filtre.
* `keys` : [Array](../data-types/array.md) de clés.
* `values` : [Array](../data-types/array.md) de valeurs.

**Valeur renvoyée**

* Renvoie un tuple de deux Array : les clés dans l&#39;ordre trié, et les valeurs additionnées pour les clés correspondantes.

**Exemple**

Dans cet exemple, nous créons une table `sum_map`, y insérons des données, puis utilisons `sumMapFilteredWithOverflow`, `sumMapFiltered` et la fonction `toTypeName` afin de comparer les résultats. Alors que `requests` était de type `UInt8` dans la table créée, `sumMapFiltered` a promu le type des valeurs additionnées en `UInt64` pour éviter un dépassement de capacité, tandis que `sumMapFilteredWithOverflow` a conservé le type `UInt8`, qui n&#39;est pas assez grand pour stocker le résultat — c.-à-d. qu&#39;un dépassement de capacité s&#39;est produit.

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

Renvoie la valeur de l’événement suivant correspondant à une chaîne d’événements.

*Fonction expérimentale, `SET allow_experimental_funnel_functions = 1` pour l’activer.*

**Syntaxe**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**Paramètres**

* `direction` — Utilisé pour indiquer le sens.
  * forward — Déplacement vers l’avant.
  * backward — Déplacement vers l’arrière.

* `base` — Utilisé pour définir le point de base.
  * head — Définit le point de base sur le premier événement.
  * tail — Définit le point de base sur le dernier événement.
  * first&#95;match — Définit le point de base sur le premier `event1` correspondant.
  * last&#95;match — Définit le point de base sur le dernier `event1` correspondant.

**Arguments**

* `timestamp` — Nom de la colonne contenant l’horodatage. Types de données pris en charge : [Date](../../sql-reference/data-types/date.md), [DateTime](/fr/sql-reference/data-types/datetime) et d’autres types entiers non signés.
* `event_column` — Nom de la colonne contenant la valeur du prochain événement à renvoyer. Types de données pris en charge : [String](../../sql-reference/data-types/string.md) et [Nullable(String)](../../sql-reference/data-types/nullable.md).
* `base_condition` — Condition que doit remplir le point de base.
* `event1`, `event2`, ... — Conditions décrivant la chaîne d’événements. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

* `event_column[next_index]` — Si le motif correspond et que la valeur suivante existe.
* `NULL` - Si le motif ne correspond pas ou que la valeur suivante n’existe pas.

Type : [Nullable(String)](../../sql-reference/data-types/nullable.md).

**Exemple**

Cela peut être utilisé lorsque les événements sont A-&gt;B-&gt;C-&gt;D-&gt;E et que vous voulez connaître l’événement qui suit B-&gt;C, c’est-à-dire D.

La requête qui recherche l’événement suivant A-&gt;B :

```sql title="Query"
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**Comportement de `forward` et de `head`**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // Base point, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // Base point, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // Base point, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**Comportement de `backward` et de `tail`**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // Base point, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // Base point, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point, Matched with Gift
1970-01-01 09:00:04    3   Basket // Base point, Matched with Basket
```

**Comportement de `forward` et `first_match`**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**Comportement de `backward` et de `last_match`**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

**Comportement de `base_condition`**

```sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be base point because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be base point because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be base point because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // Base point
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // Base point
 1970-01-01 09:00:04    1   B      ref1 // This row can not be base point because the ref column unmatched with 'ref2'.
```