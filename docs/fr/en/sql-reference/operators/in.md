---
description: 'Documentation sur les opérateurs IN, à l'exclusion des opérateurs NOT IN, GLOBAL IN et GLOBAL NOT IN qui sont traités séparément'
slug: /sql-reference/operators/in
title: 'Opérateurs IN'
doc_type: 'reference'
---

Les opérateurs `IN`, `NOT IN`, `GLOBAL IN` et `GLOBAL NOT IN` font l&#39;objet d&#39;une section distincte, car leurs fonctionnalités sont particulièrement étendues.

Le côté gauche de l&#39;opérateur est soit une colonne unique, soit un Tuple.

Exemples :

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Si le membre gauche est une colonne unique présente dans l&#39;index et que le membre droit est un ensemble de constantes, le système utilise l&#39;index pour traiter la requête.

N&#39;énumérez pas trop de valeurs explicitement (c&#39;est-à-dire des millions). Si un ensemble de données est volumineux, placez-le dans une table temporaire (par exemple, consultez la section [External data for query processing](../../engines/table-engines/special/external-data.md)), puis utilisez une sous-requête.

Le côté droit de l&#39;opérateur peut être un ensemble d&#39;expressions constantes, un ensemble de tuples avec des expressions constantes (comme indiqué dans les exemples ci-dessus), ou le nom d&#39;une table de base de données ou une sous-requête `SELECT` entre crochets.

Pour des raisons de compatibilité historique, lorsque le membre droit est une expression `tuple` unique, celle-ci peut être interprétée soit comme un ensemble de valeurs, soit comme une valeur de tuple, selon le membre gauche de l&#39;opérateur `IN`. Si le membre gauche est une valeur scalaire, ClickHouse traite les éléments de cette expression `tuple` unique côté droit comme des valeurs `IN` distinctes :

```sql title="Query"
SELECT
    1 IN (tuple(1, 2)) AS one_in_tuple,
    2 IN (tuple(1, 2)) AS two_in_tuple,
    3 IN (tuple(1, 2)) AS three_in_tuple;
```

```text title="Response"
┌─one_in_tuple─┬─two_in_tuple─┬─three_in_tuple─┐
│            1 │            1 │              0 │
└──────────────┴──────────────┴────────────────┘
```

Cela se comporte comme `SELECT 1 IN (1, 2)`. Si le côté gauche est également un tuple, le côté droit est interprété comme un ensemble de valeurs de tuple :

```sql title="Query"
SELECT tuple(1, 2) IN (tuple(1, 2)) AS tuple_in_tuple;
```

```text title="Response"
┌─tuple_in_tuple─┐
│              1 │
└────────────────┘
```

Ce traitement spécial s&#39;applique uniquement lorsque le côté droit est une expression `tuple` unique. Un côté gauche scalaire ne peut pas être mis en correspondance avec un côté droit contenant plusieurs valeurs de tuple :

```sql title="Query"
SELECT 1 IN (tuple(1, 2), tuple(3, 4));
```

```text title="Response"
Code: 43. DB::Exception: Unsupported types for IN. First argument type UInt8. Second argument type Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8)). (ILLEGAL_TYPE_OF_ARGUMENT)
```

ClickHouse autorise les types à différer dans les parties gauche et droite de la sous-requête `IN`.
Dans ce cas, il convertit la valeur du côté droit vers le type du côté gauche, comme si la fonction [accurateCastOrNull](/fr/sql-reference/functions/type-conversion-functions#accurateCastOrNull) était appliquée au côté droit.

Cela signifie que le type de données devient [Nullable](../../sql-reference/data-types/nullable.md), et si la conversion
ne peut pas être effectuée, la fonction renvoie [NULL](/fr/operations/settings/formats#input_format_null_as_default).

**Exemple**

```sql title="Query"
SELECT '1' IN (SELECT 1);
```

```text title="Response"
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

Si le côté droit de l&#39;opérateur est le nom d&#39;une table (par exemple, `UserID IN users`), cela est équivalent à la sous-requête `UserID IN (SELECT * FROM users)`. Utilisez cette syntaxe lorsque vous travaillez avec des données externes envoyées avec la requête. Par exemple, la requête peut être envoyée avec un ensemble d&#39;identifiants utilisateur chargés dans la table temporaire &#39;users&#39;, qui doit être filtrée.

Si le côté droit de l&#39;opérateur est un nom de table utilisant le moteur Set (un ensemble de données préparé toujours conservé en RAM), l&#39;ensemble de données ne sera pas recréé à chaque requête.

La sous-requête peut spécifier plusieurs colonnes pour filtrer les tuples.

Exemple :

```sql title="Query"
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

Les colonnes à gauche et à droite de l’opérateur `IN` doivent être du même type.

L’opérateur `IN` et la sous-requête peuvent apparaître dans n’importe quelle partie de la requête, y compris dans les fonctions d’agrégation et les fonctions lambda.
Exemple :

```sql title="Query"
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

```text title="Response"
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

Pour chaque jour après le 17 mars, comptez le pourcentage de pages vues générées par des utilisateurs ayant visité le site le 17 mars.
Une sous-requête dans la clause `IN` est toujours exécutée une seule fois sur un seul serveur. Il n’y a pas de sous-requêtes dépendantes.

<div id="null-processing">
  ## Traitement des valeurs NULL
</div>

Lors du traitement des requêtes, l’opérateur `IN` considère que le résultat d’une opération avec [NULL](/fr/operations/settings/formats#input_format_null_as_default) est toujours égal à `0`, que `NULL` se trouve à droite ou à gauche de l’opérateur. Les valeurs `NULL` ne sont incluses dans aucun jeu de données, ne correspondent pas les unes aux autres et ne peuvent pas être comparées si [transform&#95;null&#95;in = 0](../../operations/settings/settings.md#transform_null_in).

Voici un exemple avec la table `t_null` :

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

L’exécution de la requête `SELECT x FROM t_null WHERE y IN (NULL,3)` donne le résultat suivant :

```text
┌─x─┐
│ 2 │
└───┘
```

Vous pouvez constater que la ligne dans laquelle `y = NULL` est exclue du résultat de la requête. Cela s&#39;explique par le fait que ClickHouse ne peut pas déterminer si `NULL` est inclus dans l&#39;ensemble `(NULL,3)`, renvoie `0` comme résultat de l&#39;opération, et que `SELECT` exclut cette ligne de la sortie finale.

```sql
SELECT y IN (NULL, 3)
FROM t_null
```

```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

<div id="distributed-subqueries">
  ## Sous-requêtes distribuées
</div>

Il existe deux options pour les opérateurs `IN` avec des sous-requêtes (similaires aux opérateurs `JOIN`) : `IN` / `JOIN` ordinaire et `GLOBAL IN` / `GLOBAL JOIN`. Elles diffèrent dans leur mode d&#39;exécution pour le traitement distribué des requêtes.

:::note
Notez que les algorithmes décrits ci-dessous peuvent se comporter différemment selon le paramètre [settings](../../operations/settings/settings.md) `distributed_product_mode`.
:::

Lors de l&#39;utilisation de l&#39;opérateur `IN` standard, la requête est envoyée aux serveurs distants, et chacun d&#39;eux exécute les sous-requêtes dans la clause `IN` ou `JOIN`.

Lors de l&#39;utilisation de `GLOBAL IN` / `GLOBAL JOIN`, toutes les sous-requêtes sont d&#39;abord exécutées pour `GLOBAL IN` / `GLOBAL JOIN`, et les résultats sont collectés dans des tables temporaires. Ces tables temporaires sont ensuite envoyées à chaque serveur distant, où les requêtes sont exécutées à partir de ces données temporaires.

Pour `GLOBAL ... JOIN`, le côté de la jointure calculé en tant que sous-requête dépend du type de jointure : pour les jointures `LEFT` et `INNER`, c&#39;est la table de droite qui est calculée ; pour les jointures `RIGHT`, c&#39;est la table de gauche, car la table de droite est le côté préservé et doit être lue depuis les segments.

Pour une requête non distribuée, utilisez `IN` / `JOIN` classiques.

Soyez vigilant lors de l&#39;utilisation de sous-requêtes dans les clauses `IN` / `JOIN` pour le traitement distribué de requêtes.

Examinons quelques exemples. Supposons que chaque serveur du cluster possède une **local&#95;table** ordinaire. Chaque serveur dispose également d&#39;une table **distributed&#95;table** de type **Distributed**, qui couvre l&#39;ensemble des serveurs du cluster.

Pour une requête adressée à la **distributed&#95;table**, la requête sera envoyée à tous les serveurs distants et exécutée sur chacun d&#39;eux via la **local&#95;table**.

Par exemple, la requête

```sql
SELECT uniq(UserID) FROM distributed_table
```

sera envoyé à tous les serveurs distants en tant que

```sql
SELECT uniq(UserID) FROM local_table
```

et s&#39;exécutent sur chacun d&#39;eux en parallèle, jusqu&#39;à atteindre l&#39;étape où les résultats intermédiaires peuvent être combinés. Ces résultats intermédiaires sont alors renvoyés au serveur demandeur et fusionnés sur celui-ci, puis le résultat final est transmis au client.

Examinons maintenant une requête avec `IN` :

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

* Calcul de l’intersection des audiences de deux sites.

Cette requête sera envoyée à tous les serveurs distants en tant que

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

En d&#39;autres termes, le jeu de données de la clause `IN` sera collecté sur chaque serveur de manière indépendante, uniquement à partir des données stockées localement sur chacun des serveurs.

Cela fonctionnera correctement et de manière optimale si vous avez anticipé ce cas et distribué les données sur les serveurs du cluster de façon à ce que les données d&#39;un même UserID résident entièrement sur un seul serveur. Dans ce cas, toutes les données nécessaires seront disponibles localement sur chaque serveur. Dans le cas contraire, le résultat sera inexact. Nous appelons cette variante de la requête &quot;local IN&quot;.

Pour corriger le comportement de la requête lorsque les données sont réparties aléatoirement sur les serveurs du cluster, vous pouvez spécifier **distributed&#95;table** dans une sous-requête. La requête se présenterait alors comme suit :

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Cette requête sera envoyée à tous les serveurs distants en tant que

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

La sous-requête commencera à s&#39;exécuter sur chaque serveur distant. Étant donné que la sous-requête utilise une table distribuée, la sous-requête présente sur chaque serveur distant sera renvoyée à tous les serveurs distants sous la forme suivante :

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Par exemple, si vous disposez d&#39;un cluster de 100 serveurs, l&#39;exécution de la requête complète nécessitera 10 000 requêtes élémentaires, ce qui est généralement jugé inacceptable.

Dans de tels cas, vous devez toujours utiliser `GLOBAL IN` plutôt que `IN`. Voyons comment cela fonctionne pour la requête :

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Le serveur demandeur exécutera la sous-requête :

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

et le résultat sera placé dans une table temporaire en RAM. La requête sera ensuite envoyée à chaque serveur distant sous la forme :

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

La table temporaire `_data1` sera envoyée à chaque serveur distant avec la requête (le nom de la table temporaire est défini par l’implémentation).

Cette méthode est plus efficace que l’utilisation de `IN` ordinaire. Toutefois, gardez les points suivants à l’esprit :

1. Lors de la création d’une table temporaire, les données ne sont pas dédupliquées. Pour réduire le volume de données transmis sur le réseau, spécifiez DISTINCT dans la sous-requête. (Vous n’avez pas besoin de le faire pour `IN` ordinaire.)
2. La table temporaire sera envoyée à tous les serveurs distants. La transmission ne tient pas compte de la topologie du réseau. Par exemple, si 10 serveurs distants se trouvent dans un centre de données très éloigné du serveur demandeur, les données seront envoyées 10 fois sur le lien vers ce centre de données distant. Essayez d’éviter les jeux de données volumineux lorsque vous utilisez `GLOBAL IN`.
3. Lors de la transmission de données vers des serveurs distants, les limites du débit réseau ne sont pas configurables. Vous risquez de surcharger le réseau.
4. Essayez de répartir les données entre les serveurs afin de ne pas avoir à utiliser `GLOBAL IN` régulièrement.
5. Si vous devez utiliser souvent `GLOBAL IN`, planifiez l’emplacement du cluster ClickHouse de sorte qu’un même groupe de répliques ne soit pas réparti sur plusieurs centres de données, avec un réseau rapide entre eux, afin qu’une requête puisse être traitée entièrement dans un seul centre de données.

Il est également judicieux de spécifier une table locale dans la clause `GLOBAL IN`, si cette table locale n’est disponible que sur le serveur demandeur et que vous souhaitez utiliser ses données sur des serveurs distants.

<div id="distributed-subqueries-and-max_rows_in_set">
  ### Sous-requêtes distribuées et max_rows_in_set
</div>

Vous pouvez utiliser [`max_rows_in_set`](/fr/operations/settings/settings#max_rows_in_set) et [`max_bytes_in_set`](/fr/operations/settings/settings#max_bytes_in_set) pour contrôler la quantité de données transférées lors des requêtes distribuées.

C’est particulièrement important si la requête `GLOBAL IN` renvoie une grande quantité de données. Examinez la requête SQL suivante :

```sql
SELECT * FROM table1 WHERE col1 GLOBAL IN (SELECT col1 FROM table2 WHERE <some_predicate>)
```

Si `some_predicate` n’est pas suffisamment sélectif, il renverra un volume important de données et entraînera des problèmes de performances. Dans ce cas, il est judicieux de limiter le transfert de données sur le réseau. Notez également que [`set_overflow_mode`](/fr/operations/settings/settings#set_overflow_mode) est défini sur `throw` (par défaut), ce qui signifie qu’une exception est levée lorsque ces seuils sont atteints.

<div id="distributed-subqueries-and-max_parallel_replicas">
  ### Sous-requêtes distribuées et max_parallel_replicas
</div>

Lorsque [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) est supérieur à 1, les requêtes distribuées subissent des transformations supplémentaires.

Par exemple :

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

est transformée sur chaque serveur en :

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

où `M` est compris entre `1` et `3` selon la réplique sur laquelle la requête locale s’exécute.

Ces paramètres affectent chaque table de la famille MergeTree dans la requête et ont le même effet que l’application de `SAMPLE 1/3 OFFSET (M-1)/3` à chaque table.

Par conséquent, l’ajout du paramètre [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) ne produira des résultats corrects que si les deux tables ont le même schéma de réplication et sont échantillonnées par UserID ou par une sous-clé de celui-ci. En particulier, si `local_table_2` n’a pas de clé d’échantillonnage, des résultats incorrects seront produits. La même règle s’applique à `JOIN`.

Une solution de contournement, si `local_table_2` ne remplit pas les conditions requises, consiste à utiliser `GLOBAL IN` ou `GLOBAL JOIN`.

Si une table n’a pas de clé d’échantillonnage, il est possible d’utiliser des options plus souples pour [parallel&#95;replicas&#95;custom&#95;key](/fr/operations/settings/settings#parallel_replicas_custom_key), ce qui peut produire un comportement différent et plus efficace.