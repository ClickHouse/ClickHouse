---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

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

## Le Traitement NULL {#null-processing-1}

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

## Sous-Requêtes Distribuées {#select-distributed-subqueries}

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
