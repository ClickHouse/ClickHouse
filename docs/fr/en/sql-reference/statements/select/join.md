---
description: 'Documentation de la clause JOIN'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'Clause JOIN'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'reference'
---

La clause `JOIN` produit une nouvelle table en combinant les colonnes d&#39;une ou de plusieurs tables à partir de valeurs communes. Il s&#39;agit d&#39;une opération courante dans les bases de données prenant en charge SQL, qui correspond à l&#39;opération de jointure de l&#39;[algèbre relationnelle](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators). Le cas particulier d&#39;une jointure sur une seule table est souvent appelé « self-join ».

**Syntaxe**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Les expressions de la clause `ON` et les colonnes de la clause `USING` sont appelées « clés de jointure ». Sauf indication contraire, un `JOIN` produit un [produit cartésien](https://en.wikipedia.org/wiki/Cartesian_product) des lignes dont les « clés de jointure » correspondent, ce qui peut générer des résultats contenant bien plus de lignes que les tables sources.

<div id="supported-types-of-join">
  ## Types de JOIN pris en charge
</div>

Tous les types standard de [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)) sont pris en charge :

| Type               | Description                                                                                                                                                                                                                                                                                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | seules les lignes correspondantes sont renvoyées.                                                                                                                                                                                                                                                                                                                   |
| `LEFT OUTER JOIN`  | les lignes non correspondantes de la table de gauche sont renvoyées en plus des lignes correspondantes.                                                                                                                                                                                                                                                             |
| `RIGHT OUTER JOIN` | les lignes non correspondantes de la table de droite sont renvoyées en plus des lignes correspondantes.                                                                                                                                                                                                                                                             |
| `FULL OUTER JOIN`  | les lignes non correspondantes des deux tables sont renvoyées en plus des lignes correspondantes.                                                                                                                                                                                                                                                                   |
| `CROSS JOIN`       | produit le produit cartésien des tables entières ; les « clés de jointure » ne sont **pas** spécifiées.                                                                                                                                                                                                                                                             |
| `NATURAL JOIN`     | effectue automatiquement la jointure sur toutes les colonnes portant le même nom dans les deux tables ; chaque colonne commune n’apparaît qu’une seule fois dans le résultat. Prend en charge les variantes `INNER` (par défaut), `LEFT`, `RIGHT` et `FULL`. Équivalent à `JOIN ... USING (col1, col2, ...)`, où la liste des colonnes est déduite automatiquement. |

* `JOIN` sans type spécifié implique `INNER`.
* Le mot-clé `OUTER` peut être omis sans risque.
* Une autre syntaxe pour `CROSS JOIN` consiste à spécifier plusieurs tables dans la [clause `FROM`](../../../sql-reference/statements/select/from.md), séparées par des virgules.
* S’il n’existe aucune colonne correspondante pour un `NATURAL JOIN`, il se comporte comme un `CROSS JOIN`.

Les types de jointure supplémentaires disponibles dans ClickHouse sont :

| Type                                                | Description                                                                                                                                                         |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | Une allowlist sur les « clés de jointure », sans produire de produit cartésien.                                                                                     |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | Une liste d’exclusion sur les « clés de jointure », sans produire de produit cartésien.                                                                             |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Désactive partiellement (pour le côté opposé de `LEFT` et `RIGHT`) ou complètement (pour `INNER` et `FULL`) le produit cartésien pour les types de `JOIN` standard. |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | Joint des séquences avec une correspondance non exacte. L’utilisation de `ASOF JOIN` est décrite ci-dessous.                                                        |
| `PASTE JOIN`                                        | Effectue une concaténation horizontale de deux tables.                                                                                                              |

:::note
Lorsque [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) est défini sur `partial_merge`, `RIGHT JOIN` et `FULL JOIN` sont pris en charge uniquement avec la strictness `ALL` (`SEMI`, `ANTI`, `ANY` et `ASOF` ne sont pas pris en charge).
:::

<div id="settings">
  ## Paramètres
</div>

Le type de JOIN par défaut peut être remplacé à l’aide du paramètre [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness).

Le comportement du serveur ClickHouse pour les opérations `ANY JOIN` dépend du paramètre [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys).

**Voir aussi**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

Utilisez le paramètre `cross_to_inner_join_rewrite` pour définir le comportement lorsque ClickHouse ne parvient pas à réécrire un `CROSS JOIN` en `INNER JOIN`. La valeur par défaut est `1`, ce qui permet à la jointure de s’exécuter, mais plus lentement. Définissez `cross_to_inner_join_rewrite` sur `0` si vous voulez générer une erreur, et sur `2` pour ne pas exécuter les jointures croisées, mais forcer à la place la réécriture de tous les JOIN par virgule ou `CROSS JOIN`. Si la réécriture échoue lorsque la valeur est `2`, vous recevrez un message d’erreur indiquant &quot;Please, try to simplify `WHERE` section&quot;.

<div id="on-section-conditions">
  ## Conditions de la section `ON`
</div>

Une section `ON` peut contenir plusieurs conditions combinées à l’aide des opérateurs `AND` et `OR`. Les conditions qui spécifient des clés de jointure doivent :

* référencer à la fois la table de gauche et celle de droite
* utiliser l’opérateur d’égalité

Les autres conditions peuvent utiliser d’autres opérateurs logiques, mais elles doivent référencer soit la table de gauche, soit la table de droite d’une requête.

Les lignes sont jointes si l’ensemble de la condition complexe est satisfait. Si les conditions ne sont pas satisfaites, les lignes peuvent tout de même être incluses dans le résultat selon le type de `JOIN`. Notez que si les mêmes conditions sont placées dans une section `WHERE` et qu’elles ne sont pas satisfaites, les lignes sont alors toujours exclues du résultat.

L’opérateur `OR` dans la clause `ON` fonctionne avec l’algorithme de jointure par hachage : pour chaque argument `OR` avec des clés de jointure pour `JOIN`, une table de hachage distincte est créée. La consommation de mémoire et le temps d’exécution de la requête augmentent donc linéairement à mesure que le nombre d’expressions `OR` dans la clause `ON` augmente.

:::note
Si une condition référence des colonnes de tables différentes, alors seul l’opérateur d’égalité (`=`) est pris en charge pour le moment.
:::

**Exemple**

Considérez `table_1` et `table_2` :

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Requête avec une condition de clé de jointure et une condition supplémentaire sur `table_2` :

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Notez que le résultat contient la ligne avec le nom `C` et la colonne de texte vide. Elle est incluse dans le résultat, car une jointure de type `OUTER` est utilisée.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Requête avec une jointure de type `INNER` et plusieurs conditions :

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

Requête avec une jointure de type `INNER` et une condition avec `OR` :

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Requête avec une jointure de type `INNER` et des conditions avec `OR` et `AND` :

:::note

Par défaut, les conditions d’inégalité sont prises en charge tant qu’elles utilisent des colonnes de la même table.
Par exemple, `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, car `t1.b > 0` n’utilise que des colonnes de `t1` et `t2.b > t2.c` n’utilise que des colonnes de `t2`.
Vous pouvez toutefois essayer la prise en charge expérimentale de conditions comme `t1.a = t2.key AND t1.b > t2.key` ; consultez la section ci-dessous pour en savoir plus.

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## JOIN avec des conditions d’inégalité pour des colonnes issues de tables différentes
</div>

ClickHouse prend actuellement en charge `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` avec des conditions d’inégalité, en plus des conditions d’égalité. Les conditions d’inégalité sont prises en charge uniquement par les algorithmes de jointure `hash` et `grace_hash`. Les conditions d’inégalité ne sont pas prises en charge avec `join_use_nulls`.

**Exemple**

Table `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

Table `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## Valeurs NULL dans les clés de JOIN
</div>

`NULL` n’est égal à aucune valeur, y compris à lui-même. Cela signifie que si une clé de `JOIN` a une valeur `NULL` dans une table, elle ne correspondra pas à une valeur `NULL` dans l’autre table.

**Exemple**

Table `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

Table `B` :

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

Notez que la row contenant `Charlie` de la table `A` et la row avec le score 88 de la table `B` n’apparaissent pas dans le résultat à cause de la valeur `NULL` dans la clé de `JOIN`.

Si vous souhaitez faire correspondre des valeurs `NULL`, utilisez la fonction `isNotDistinctFrom` pour comparer les clés de `JOIN`.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## Utilisation d’ASOF JOIN
</div>

`ASOF JOIN` est utile lorsque vous devez joindre des enregistrements qui n’ont pas de correspondance exacte.

Cet algorithme JOIN nécessite une colonne spéciale dans les tables. Cette colonne :

* Doit contenir une séquence ordonnée.
* Peut être de l’un des types suivants : [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md).
* Pour l’algorithme de jointure `hash`, elle ne peut pas être la seule colonne de la clause `JOIN`.

Syntaxe `ASOF JOIN ... ON` :

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Vous pouvez utiliser autant de conditions d’égalité que nécessaire et exactement une condition de correspondance la plus proche. Par exemple, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions prises en charge pour la correspondance la plus proche : `>`, `>=`, `<`, `<=`.

Syntaxe `ASOF JOIN ... USING` :

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` utilise `equi_columnX` pour une jointure par égalité et `asof_column` pour une jointure sur la correspondance la plus proche, avec la condition `table_1.asof_column >= table_2.asof_column`. La colonne `asof_column` est toujours la dernière de la clause `USING`.

Par exemple, considérons les tables suivantes :

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` peut prendre le timestamp d’un événement utilisateur de `table_1` et trouver, dans `table_2`, un événement dont le timestamp est le plus proche de celui de l’événement de `table_1`, selon la condition de correspondance la plus proche. Lorsque des valeurs de timestamp identiques sont disponibles, ce sont elles qui sont considérées comme les plus proches. Ici, la colonne `user_id` peut être utilisée pour effectuer une jointure sur l’égalité, et la colonne `ev_time` pour effectuer la jointure sur la correspondance la plus proche. Dans notre exemple, `event_1_1` peut être joint à `event_2_1` et `event_1_2` peut être joint à `event_2_3`, mais `event_2_2` ne peut pas être joint.

:::note
`ASOF JOIN` est pris en charge uniquement par les algorithmes de jointure `hash` et `full_sorting_merge`.
Il n’est **pas** pris en charge par le [moteur de table Join](../../../engines/table-engines/special/join.md).
:::

<div id="paste-join-usage">
  ## Utilisation de PASTE JOIN
</div>

Le résultat de `PASTE JOIN` est une table qui contient toutes les colonnes de la sous-requête de gauche, suivies de toutes les colonnes de la sous-requête de droite.
Les lignes sont appariées en fonction de leur position dans les tables d’origine (l’ordre des lignes doit être défini).
Si les sous-requêtes renvoient un nombre de lignes différent, les lignes supplémentaires seront tronquées.

Exemple :

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

Note : dans ce cas, le résultat peut être non déterministe si la lecture est effectuée en parallèle. Par exemple :

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## JOIN distribué
</div>

Il existe deux façons d’exécuter un JOIN impliquant des tables distribuées :

* Lorsqu’on utilise un `JOIN` classique, la requête est envoyée aux serveurs distants. Des sous-requêtes sont exécutées sur chacun d’eux afin de constituer la table de droite, puis la jointure est effectuée avec cette table. Autrement dit, la table de droite est construite séparément sur chaque serveur.
* Lorsqu’on utilise `GLOBAL ... JOIN`, le serveur demandeur exécute d’abord une sous-requête pour calculer l’un des côtés de la jointure et stocke le résultat dans une table temporaire. Cette table temporaire est ensuite transmise à chaque serveur distant, et des requêtes y sont exécutées à l’aide des données temporaires transmises. Pour les jointures `LEFT` et `INNER`, la table de droite est calculée par la sous-requête. Pour les jointures `RIGHT`, c’est au contraire la table de gauche qui est calculée, puisque la table de droite est celle qui est conservée et doit être lue depuis les shards.

Soyez prudent lorsque vous utilisez `GLOBAL`. Pour plus d’informations, consultez la section [Sous-requêtes distribuées](/fr/sql-reference/operators/in#distributed-subqueries).

<div id="implicit-type-conversion">
  ## Conversion implicite des types
</div>

Les requêtes `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` et `FULL JOIN` prennent en charge la conversion implicite des types pour les &quot;clés de jointure&quot;. Cependant, la requête ne peut pas être exécutée si les clés de jointure des tables de gauche et de droite ne peuvent pas être converties vers un type unique (par exemple, il n’existe aucun type de données capable de contenir toutes les valeurs de `UInt64` et `Int64`, ou de `String` et `Int32`).

**Exemple**

Considérons la table `t_1` :

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

et la table `t_2` :

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

La requête

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

renvoie l’ensemble :

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## Recommandations d’usage
</div>

<div id="processing-of-empty-or-null-cells">
  ### Traitement des cellules vides ou NULL
</div>

Lors de la jointure de tables, des cellules vides peuvent apparaître. Le paramètre [join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) définit comment ClickHouse remplit ces cellules.

Si les clés de `JOIN` sont des champs [Nullable](../../../sql-reference/data-types/nullable.md), les lignes dans lesquelles au moins une des clés a la valeur [NULL](/fr/sql-reference/syntax#null) ne participent pas à la jointure.

<div id="syntax">
  ### Syntaxe
</div>

Les colonnes spécifiées dans `USING` doivent porter les mêmes noms dans les deux sous-requêtes, et les autres colonnes doivent avoir des noms différents. Vous pouvez utiliser des alias pour modifier les noms des colonnes dans les sous-requêtes.

La clause `USING` spécifie une ou plusieurs colonnes sur lesquelles effectuer la jointure, ce qui implique l&#39;égalité de ces colonnes. La liste des colonnes s&#39;écrit sans parenthèses. Les conditions de jointure plus complexes ne sont pas prises en charge.

<div id="syntax-limitations">
  ### Limites de la syntaxe
</div>

Pour plusieurs clauses `JOIN` dans une seule requête `SELECT` :

* La sélection de toutes les colonnes via `*` n’est possible que lorsque ce sont des tables qui sont jointes, et non des sous-requêtes.
* La clause `PREWHERE` n’est pas disponible.
* La clause `USING` n’est pas disponible.

Pour les clauses `ON`, `WHERE` et `GROUP BY` :

* Les expressions arbitraires ne peuvent pas être utilisées dans les clauses `ON`, `WHERE` et `GROUP BY`, mais vous pouvez définir une expression dans une clause `SELECT`, puis l’utiliser dans ces clauses à l’aide d’un alias.

<div id="performance">
  ### Performances
</div>

Lors de l’exécution d’un `JOIN`, l’ordre d’exécution n’est pas optimisé par rapport aux autres étapes de la requête. La jointure (recherche dans la table de droite) est exécutée avant le filtrage dans `WHERE` et avant l’agrégation.

Chaque fois qu’une requête avec le même `JOIN` est exécutée, la sous-requête est relancée, car le résultat n’est pas mis en cache. Pour éviter cela, utilisez le moteur de table spécial [Join](../../../engines/table-engines/special/join.md), qui est une structure préparée pour les jointures et qui réside toujours en RAM.

Dans certains cas, il est plus efficace d’utiliser [IN](../../../sql-reference/operators/in.md) plutôt que `JOIN`.

Si vous avez besoin d’un `JOIN` pour joindre des tables de dimensions (il s’agit de tables relativement petites qui contiennent des propriétés de dimension, comme les noms de campagnes publicitaires), un `JOIN` peut ne pas être très pratique, car la table de droite est consultée à nouveau pour chaque requête. Dans ce cas, utilisez plutôt la fonctionnalité « dictionaries ». Pour plus d’informations, consultez la section [Dictionaries](/fr/sql-reference/statements/create/dictionary/overview.md).

<div id="memory-limitations">
  ### Limites de mémoire
</div>

Par défaut, ClickHouse utilise l&#39;algorithme de [jointure de hachage](https://en.wikipedia.org/wiki/Hash_join). ClickHouse prend right&#95;table et crée pour celle-ci une table de hachage en RAM. Si `join_algorithm = 'auto'` est activé, après un certain seuil de consommation de mémoire, ClickHouse se rabat sur l&#39;algorithme de jointure par [fusion](https://en.wikipedia.org/wiki/Sort-merge_join). Pour une description des algorithmes `JOIN`, consultez le paramètre [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm).

Si vous devez limiter la consommation de mémoire de l&#39;opération `JOIN`, utilisez les paramètres suivants :

* [max&#95;rows&#95;in&#95;join](/fr/operations/settings/settings#max_rows_in_join) — Limite le nombre de lignes dans la table de hachage.
* [max&#95;bytes&#95;in&#95;join](/fr/operations/settings/settings#max_bytes_in_join) — Limite la taille de la table de hachage.

Lorsque l&#39;une de ces limites est atteinte, ClickHouse agit selon les instructions du paramètre [join&#95;overflow&#95;mode](/fr/operations/settings/settings#join_overflow_mode).

<div id="examples">
  ## Exemples
</div>

Exemple :

```sql
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

```text
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

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [ClickHouse : un DBMS ultra-rapide avec une prise en charge complète des JOIN SQL - Partie 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* Blog : [ClickHouse : un DBMS ultra-rapide avec une prise en charge complète des JOIN SQL - En coulisses - Partie 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* Blog : [ClickHouse : un DBMS ultra-rapide avec une prise en charge complète des JOIN SQL - En coulisses - Partie 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* Blog : [ClickHouse : un DBMS ultra-rapide avec une prise en charge complète des JOIN SQL - En coulisses - Partie 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)