---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause de JOINTURE {#select-join}

Join produit une nouvelle table en combinant des colonnes d'une ou plusieurs tables en utilisant des valeurs communes à chacune. C'est une opération courante dans les bases de données avec support SQL, ce qui correspond à [l'algèbre relationnelle](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) rejoindre. Le cas particulier d'une jointure de table est souvent appelé “self-join”.

Syntaxe:

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Les Expressions de `ON` clause et colonnes de `USING` clause sont appelés “join keys”. Sauf indication contraire, joindre un produit [Produit cartésien](https://en.wikipedia.org/wiki/Cartesian_product) des lignes, avec correspondance “join keys”, ce qui pourrait produire des résultats avec beaucoup plus de lignes que les tables source.

## Types de jointure pris en charge {#select-join-types}

Tous les standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) les types sont pris en charge:

-   `INNER JOIN`, seules les lignes correspondantes sont retournés.
-   `LEFT OUTER JOIN`, les lignes non correspondantes de la table de gauche sont retournées en plus des lignes correspondantes.
-   `RIGHT OUTER JOIN`, les lignes non correspondantes de la table de gauche sont retournées en plus des lignes correspondantes.
-   `FULL OUTER JOIN`, les lignes non correspondantes des deux tables sont renvoyées en plus des lignes correspondantes.
-   `CROSS JOIN`, produit le produit cartésien des tables entières, “join keys” être **pas** défini.

`JOIN` sans type spécifié implique `INNER`. Mot `OUTER` peut les oublier. Syntaxe Alternative pour `CROSS JOIN` spécifie plusieurs tables dans [De la clause](from.md) séparés par des virgules.

Autres types de jointure disponibles dans ClickHouse:

-   `LEFT SEMI JOIN` et `RIGHT SEMI JOIN` une liste blanche sur “join keys”, sans produire un produit cartésien.
-   `LEFT ANTI JOIN` et `RIGHT ANTI JOIN` une liste noire sur “join keys”, sans produire un produit cartésien.
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` et `INNER ANY JOIN`, partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types.
-   `ASOF JOIN` et `LEFT ASOF JOIN`, joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

## Setting {#join-settings}

!!! note "Note"
    La valeur de rigueur par défaut peut être remplacée à l'aide [join_default_strictness](../../../operations/settings/settings.md#settings-join_default_strictness) paramètre.

### ASOF joindre L'utilisation {#asof-join-usage}

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
    `ASOF` jointure est **pas** pris en charge dans le [Rejoindre](../../../engines/table-engines/special/join.md) tableau moteur.

## Jointure Distribuée {#global-join}

Il existe deux façons d'exécuter join impliquant des tables distribuées:

-   Lors de l'utilisation normale `JOIN` la requête est envoyée aux serveurs distants. Les sous-requêtes sont exécutées sur chacune d'elles afin de créer la bonne table, et la jointure est effectuée avec cette table. En d'autres termes, la table de droite est formée sur chaque serveur séparément.
-   Lors de l'utilisation de `GLOBAL ... JOIN`, d'abord le serveur demandeur exécute une sous-requête pour calculer la bonne table. Cette table temporaire est transmise à chaque serveur distant, et les requêtes sont exécutées sur eux en utilisant les données temporaires qui ont été transmises.

Soyez prudent lorsque vous utilisez `GLOBAL`. Pour plus d'informations, voir le [Sous-requêtes distribuées](../../operators/in.md#select-distributed-subqueries) section.

## Recommandations D'Utilisation {#usage-recommendations}

### Traitement des cellules vides ou nulles {#processing-of-empty-or-null-cells}

Lors de la jonction de tables, les cellules vides peuvent apparaître. Paramètre [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) définir comment clickhouse remplit ces cellules.

Si l' `JOIN` les touches sont [Nullable](../../data-types/nullable.md) champs, les lignes où au moins une des clés a la valeur [NULL](../../../sql-reference/syntax.md#null-literal) ne sont pas jointes.

### Syntaxe {#syntax}

Les colonnes spécifiées dans `USING` doit avoir les mêmes noms dans les deux sous-requêtes, et les autres colonnes doivent être nommées différemment. Vous pouvez utiliser des alias pour les noms des colonnes dans les sous-requêtes.

Le `USING` clause spécifie une ou plusieurs colonnes de jointure, qui établit l'égalité de ces colonnes. La liste des colonnes est définie sans crochets. Les conditions de jointure plus complexes ne sont pas prises en charge.

### Limitations De Syntaxe {#syntax-limitations}

Pour plusieurs `JOIN` clauses dans un seul `SELECT` requête:

-   Prendre toutes les colonnes via `*` n'est disponible que si les tables sont jointes, pas les sous-requêtes.
-   Le `PREWHERE` la clause n'est pas disponible.

Pour `ON`, `WHERE`, et `GROUP BY` clause:

-   Les expressions arbitraires ne peuvent pas être utilisées dans `ON`, `WHERE`, et `GROUP BY` mais vous pouvez définir une expression dans un `SELECT` clause et ensuite l'utiliser dans ces clauses via un alias.

### Performance {#performance}

Lors de l'exécution d'un `JOIN`, il n'y a pas d'optimisation de la commande d'exécution par rapport aux autres stades de la requête. La jointure (une recherche dans la table de droite) est exécutée avant de filtrer `WHERE` et avant l'agrégation.

Chaque fois qu'une requête est exécutée avec la même `JOIN`, la sous-requête est exécutée à nouveau car le résultat n'est pas mis en cache. Pour éviter cela, utilisez la spéciale [Rejoindre](../../../engines/table-engines/special/join.md) table engine, qui est un tableau préparé pour l'assemblage qui est toujours en RAM.

Dans certains cas, il est plus efficace d'utiliser [IN](../../operators/in.md) plutôt `JOIN`.

Si vous avez besoin d'un `JOIN` pour se joindre à des tables de dimension (ce sont des tables relativement petites qui contiennent des propriétés de dimension, telles que des noms pour des campagnes publicitaires), un `JOIN` peut-être pas très pratique en raison du fait que la bonne table est ré-accédée pour chaque requête. Pour de tels cas, il y a un “external dictionaries” la fonctionnalité que vous devez utiliser à la place de `JOIN`. Pour plus d'informations, voir le [Dictionnaires externes](../../dictionaries/external-dictionaries/external-dicts.md) section.

### Limitations De Mémoire {#memory-limitations}

Par défaut, ClickHouse utilise [jointure de hachage](https://en.wikipedia.org/wiki/Hash_join) algorithme. ClickHouse prend le `<right_table>` et crée une table de hachage pour cela dans la RAM. Après un certain seuil de consommation de mémoire, ClickHouse revient à fusionner l'algorithme de jointure.

Si vous devez restreindre la consommation de mémoire de l'opération join utilisez les paramètres suivants:

-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

Lorsque l'une de ces limites est atteinte, ClickHouse agit comme [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) réglage des instructions.

## Exemple {#examples}

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
