---
description: 'Documentation pour la clause WITH'
sidebar_label: 'WITH'
slug: /sql-reference/statements/select/with
title: 'Clause WITH'
doc_type: 'reference'
---

ClickHouse prend en charge les expressions de table communes ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), les expressions scalaires communes et les requêtes récursives.

<div id="common-table-expressions">
  ## Expressions de table communes
</div>

Les expressions de table communes représentent des sous-requêtes nommées.
Elles peuvent être référencées par leur nom partout dans une requête `SELECT` où une expression de table est autorisée.
Les sous-requêtes nommées peuvent être référencées par leur nom dans le contexte de la requête en cours ou dans celui des sous-requêtes imbriquées.

Chaque référence à une expression de table commune dans les requêtes `SELECT` est toujours remplacée par la sous-requête définie dans celle-ci si la CTE n&#39;est pas explicitement définie comme matérialisée (voir [Expressions de table communes matérialisées](#materialized-common-table-expressions)).
La récursion est évitée en masquant la CTE actuelle du processus de résolution des identifiants.

Veuillez noter que les CTE ne garantissent pas les mêmes résultats partout où elles sont appelées, car la requête est réexécutée à chaque utilisation.

<div id="common-table-expressions-syntax">
  ### Syntaxe
</div>

```sql
WITH <identifier> AS [MATERIALIZED] <subquery expression>
```

<div id="common-table-expressions-example">
  ### Exemple
</div>

Voici un exemple de cas où une sous-requête est réexécutée :

```sql
WITH cte_numbers AS
(
    SELECT
        num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT
    count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers)
```

Si les CTE transmettaient exactement les résultats, et pas seulement un fragment de code, vous verriez toujours `1000000`

Cependant, comme nous faisons référence deux fois à `cte_numbers`, des nombres aléatoires sont générés à chaque fois et, par conséquent, nous obtenons des résultats aléatoires différents : `280501, 392454, 261636, 196227`, etc.

<div id="materialized-common-table-expressions">
  ## Expressions de table communes matérialisées
</div>

Par défaut, ClickHouse insère la sous-requête d’une CTE à chaque point de référence et la réexécute donc à chaque fois.
L’ajout du mot-clé `MATERIALIZED` indique à ClickHouse d’exécuter la sous-requête de la CTE **une seule et unique fois**, de stocker les résultats dans une table temporaire, puis de servir toutes les références à partir de cette table.
Cela est particulièrement utile lorsque la même CTE est référencée plusieurs fois dans une requête (par exemple, dans des auto-jointures ou dans plusieurs sous-requêtes `IN`), car le calcul sous-jacent n’est effectué qu’une seule fois.

:::note
Les CTE matérialisées sont une fonctionnalité **expérimentale**.
Elles nécessitent que l’[analyseur](/fr/operations/analyzer) et le paramètre `enable_materialized_cte` soient activés.
:::

<div id="common-table-expressions-syntax">
  ### Syntaxe
</div>

```sql
WITH <identifier> AS MATERIALIZED (<subquery>)
SELECT ...
```

<div id="materialized-cte-when-to-use">
  ### Quand utiliser
</div>

Les CTE matérialisées sont particulièrement utiles lorsque :

* La même CTE est référencée **plus d&#39;une fois** dans une requête.
  Sans `MATERIALIZED`, chaque référence réexécute la sous-requête de manière indépendante.
* La CTE contient des fonctions **non déterministes** comme `generateRandom`.
  La matérialisation garantit que toutes les références voient les mêmes données.
* La CTE implique des **calculs coûteux** (agrégations, jointures, parcours de grands volumes de données) qu&#39;il ne faut pas répéter.

:::tip
Si une CTE matérialisée n&#39;est référencée qu&#39;une seule fois, ClickHouse la réintègre automatiquement dans une sous-requête classique afin d&#39;éviter tout surcoût inutile.
:::

<div id="materialized-common-table-expressions-examples">
  ### Exemples
</div>

**Exemple 1 :** Auto-jointure sur une CTE matérialisée

Sans `MATERIALIZED`, les deux branches de la jointure exécuteraient la sous-requête indépendamment.
Avec `MATERIALIZED`, la table n’est parcourue qu’une seule fois et les deux branches de la jointure lisent depuis la même table temporaire.

```sql
SET enable_materialized_cte = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE = Memory;
INSERT INTO users VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

WITH
    a AS MATERIALIZED (SELECT * FROM users WHERE name = 'Alice')
SELECT count() FROM a AS l JOIN a AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       1 │
└─────────┘
```

**Exemple 2 :** Résultats déterministes avec des fonctions non déterministes

Les CTE classiques avec `generateRandom` produisent des résultats différents à chaque référence.
La matérialisation de la CTE garantit la cohérence :

```sql
SET enable_materialized_cte = 1;

WITH cte_numbers AS MATERIALIZED
(
    SELECT num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers);
```

Comme les deux références lisent les mêmes données matérialisées, le résultat est toujours `1000000`.

**Exemple 3 :** Enchaînement de CTE matérialisées

Les CTE matérialisées peuvent faire référence à d&#39;autres CTE matérialisées.
ClickHouse résout les dépendances et les matérialise dans le bon ordre :

```sql
SET enable_materialized_cte = 1;

WITH
    a AS MATERIALIZED (SELECT uid, name FROM users),
    b AS MATERIALIZED (SELECT uid FROM a)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

L’ordre des définitions de CTE n’a pas d’importance — les références à des CTE définies plus loin sont autorisées :

```sql
SET enable_materialized_cte = 1;

WITH
    b AS MATERIALIZED (SELECT uid FROM a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

<div id="materialized-cte-restrictions">
  ### Restrictions
</div>

* **Paramètre expérimental requis** : le paramètre `enable_materialized_cte` doit être activé.
* **Analyseur requis** : les CTE matérialisées fonctionnent uniquement lorsque l’[analyseur](/fr/operations/analyzer) est activé (`enable_analyzer = 1`).
* **Non pris en charge avec `RECURSIVE`** : la combinaison des mots-clés `MATERIALIZED` et `RECURSIVE` n’est pas autorisée et entraîne une exception `UNSUPPORTED_METHOD`.
* **Les CTE corrélées sont interdites** : une CTE matérialisée ne peut pas référencer des colonnes issues de portées externes de la requête.

<div id="common-scalar-expressions">
  ## Expressions scalaires communes
</div>

ClickHouse vous permet de déclarer des alias pour des expressions scalaires arbitraires dans la clause `WITH`.
Les expressions scalaires communes peuvent être référencées à n’importe quel endroit de la requête.

:::note
Si une expression scalaire commune fait référence à autre chose qu’un littéral constant, l’expression peut entraîner la présence de [variables libres](https://en.wikipedia.org/wiki/Free_variables_and_bound_variables).
ClickHouse résout chaque identifiant dans la portée la plus proche possible, ce qui signifie que les variables libres peuvent faire référence à des entités inattendues en cas de conflit de noms, ou conduire à une sous-requête corrélée.
Il est recommandé de définir une CSE sous forme de [fonction lambda](/fr/sql-reference/functions/overview#arrow-operator-and-lambda) (possible uniquement lorsque l’[analyseur](/fr/operations/analyzer) est activé), en liant tous les identifiants utilisés, afin d’obtenir un comportement plus prévisible lors de la résolution des identifiants d’expression.
:::

<div id="common-table-expressions-syntax">
  ### Syntaxe
</div>

```sql
WITH <expression> AS <identifier>
```

<div id="materialized-common-table-expressions-examples">
  ### Exemples
</div>

**Exemple 1 :** Utilisation d’une expression constante comme « variable »

```sql
WITH '2019-08-01 15:23:00' AS ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**Exemple 2 :** Utilisation de fonctions d’ordre supérieur pour lier des identifiants

```sql
WITH
    '.txt' as extension,
    (id, extension) -> concat(lower(id), extension) AS gen_name
SELECT gen_name('test', '.sql') as file_name;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Exemple 3 :** Utilisation de fonctions d’ordre supérieur avec des variables libres

Les requêtes d’exemple suivantes montrent que les identifiants non liés sont résolus en l’entité de la portée la plus proche.
Ici, `extension` n’est pas liée dans le corps de la fonction lambda `gen_name`.
Bien que `extension` soit définie comme `'.txt'` en tant qu’expression scalaire commune dans la portée de la définition et de l’utilisation de `generated_names`, elle est résolue comme une colonne de la table `extension_list`, car elle est disponible dans la sous-requête `generated_names`.

```sql
CREATE TABLE extension_list
(
    extension String
)
ORDER BY extension
AS SELECT '.sql';

WITH
    '.txt' as extension,
    generated_names as (
        WITH
            (id) -> concat(lower(id), extension) AS gen_name
        SELECT gen_name('test') as file_name FROM extension_list
    )
SELECT file_name FROM generated_names;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Exemple 4 :** Suppression du résultat de l’expression sum(bytes) de la liste des colonnes de la clause SELECT

```sql
WITH sum(bytes) AS s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**Exemple 5 :** Utilisation des résultats d’une sous-requête scalaire

```sql
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
LIMIT 10;
```

**Exemple 6 :** Réutiliser une expression dans une sous-requête

```sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

<div id="recursive-queries">
  ## Requêtes récursives
</div>

Le modificateur facultatif `RECURSIVE` permet à une requête WITH de faire référence à son propre résultat. Exemple :

**Exemple :** Additionner les entiers de 1 à 100

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

:::note
Les CTE récursives s’appuient sur l’[analyseur de requêtes](/fr/operations/analyzer), introduit dans la version **`24.3`**. Si vous utilisez la version **`24.3+`** et rencontrez une exception **`(UNKNOWN_TABLE)`** ou **`(UNSUPPORTED_METHOD)`**, cela suggère que l’analyseur est désactivé pour votre instance, votre rôle ou votre profil. Pour activer l’analyseur, activez le paramètre **`allow_experimental_analyzer`** ou mettez à jour le paramètre **`compatibility`** vers une version plus récente.
À partir de la version `24.8`, l’analyseur a été entièrement promu en production, et le paramètre `allow_experimental_analyzer` a été renommé `enable_analyzer`.
:::

La forme générale d’une requête récursive `WITH` est toujours la suivante : un terme non récursif, puis `UNION ALL`, puis un terme récursif, où seul le terme récursif peut contenir une référence à la propre sortie de la requête. Une requête CTE récursive s’exécute comme suit :

1. Évaluer le terme non récursif. Placer le résultat de la requête du terme non récursif dans une table de travail temporaire.
2. Tant que la table de travail n’est pas vide, répéter les étapes suivantes :
   1. Évaluer le terme récursif, en remplaçant l’auto-référence récursive par le contenu actuel de la table de travail. Placer le résultat de la requête du terme récursif dans une table intermédiaire temporaire.
   2. Remplacer le contenu de la table de travail par celui de la table intermédiaire, puis vider la table intermédiaire.

Les requêtes récursives sont généralement utilisées pour travailler avec des données hiérarchiques ou arborescentes. Par exemple, nous pouvons écrire une requête qui effectue un parcours d’arbre :

**Exemple :** Parcours d’arbre

Commençons par créer une table d’arbre :

```sql
DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    parent_id Nullable(UInt64),
    data String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');
```

Nous pouvons parcourir cet arbre à l’aide de la requête suivante :

**Exemple :** Parcours d’arbre

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree;
```

```text
┌─id─┬─parent_id─┬─data──────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │
│  1 │         0 │ Child_1   │
│  2 │         0 │ Child_2   │
│  3 │         1 │ Child_1_1 │
└────┴───────────┴───────────┘
```

<div id="search-order">
  ### Ordre de parcours
</div>

Pour établir un ordre en profondeur, nous calculons pour chaque ligne de résultat un tableau des lignes déjà visitées :

**Exemple :** parcours en profondeur d’un arbre

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY path;
```

```text
┌─id─┬─parent_id─┬─data──────┬─path────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │ [0]     │
│  1 │         0 │ Child_1   │ [0,1]   │
│  3 │         1 │ Child_1_1 │ [0,1,3] │
│  2 │         0 │ Child_2   │ [0,2]   │
└────┴───────────┴───────────┴─────────┘
```

Pour obtenir un parcours en largeur, l’approche standard consiste à ajouter une colonne qui indique la profondeur de la recherche :

**Exemple :** Parcours d’arbre en largeur

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path, toUInt64(0) AS depth
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id]), depth + 1
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY depth;
```

```text
┌─id─┬─link─┬─data──────┬─path────┬─depth─┐
│  0 │ ᴺᵁᴸᴸ │ ROOT      │ [0]     │     0 │
│  1 │    0 │ Child_1   │ [0,1]   │     1 │
│  2 │    0 │ Child_2   │ [0,2]   │     1 │
│  3 │    1 │ Child_1_1 │ [0,1,3] │     2 │
└────┴──────┴───────────┴─────────┴───────┘
```

<div id="cycle-detection">
  ### Détection des cycles
</div>

Commençons par créer une table de graphe :

```sql
DROP TABLE IF EXISTS graph;
CREATE TABLE graph
(
    from UInt64,
    to UInt64,
    label String
) ENGINE = MergeTree ORDER BY (from, to);

INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');
```

Nous pouvons parcourir ce graphe à l’aide de la requête suivante :

**Exemple :** Parcours de graphe sans détection de cycles

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
    UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┐
│    1 │  4 │ 1 -> 4 │
│    1 │  2 │ 1 -> 2 │
│    1 │  3 │ 1 -> 3 │
│    2 │  3 │ 2 -> 3 │
│    4 │  5 │ 4 -> 5 │
└──────┴────┴────────┘
```

Mais si nous ajoutons un cycle à ce graphe, la requête précédente échouera avec l’erreur `Maximum recursive CTE evaluation depth` :

```sql
INSERT INTO graph VALUES (5, 1, '5 -> 1');

WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
Code: 306. DB::Exception: Received from localhost:9000. DB::Exception: Maximum recursive CTE evaluation depth (1000) exceeded, during evaluation of search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to). Consider raising max_recursive_cte_evaluation_depth setting.: While executing RecursiveCTESource. (TOO_DEEP_RECURSION)
```

La méthode standard pour gérer les cycles consiste à calculer un tableau des nœuds déjà visités :

**Exemple :** Parcours de graphe avec détection de cycles

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label, false AS is_cycle, [tuple(g.from, g.to)] AS path FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label, has(path, tuple(g.from, g.to)), arrayConcat(sg.path, [tuple(g.from, g.to)])
    FROM graph g, search_graph sg
    WHERE g.from = sg.to AND NOT is_cycle
)
SELECT * FROM search_graph WHERE is_cycle ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┬─is_cycle─┬─path──────────────────────┐
│    1 │  4 │ 1 -> 4 │ true     │ [(1,4),(4,5),(5,1),(1,4)] │
│    4 │  5 │ 4 -> 5 │ true     │ [(4,5),(5,1),(1,4),(4,5)] │
│    5 │  1 │ 5 -> 1 │ true     │ [(5,1),(1,4),(4,5),(5,1)] │
└──────┴────┴────────┴──────────┴───────────────────────────┘
```

<div id="infinite-queries">
  ### Requêtes infinies
</div>

Il est également possible d&#39;utiliser des requêtes CTE récursives infinies si `LIMIT` est utilisé dans la requête englobante :

**Exemple :** Requête CTE récursive infinie

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table
)
SELECT sum(number) FROM (SELECT number FROM test_table LIMIT 100);
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

<div id="trailing-comma">
  ## Virgule finale
</div>

Une virgule est autorisée après le dernier élément de la clause `WITH` :

```sql
WITH
    (SELECT sum(number) FROM numbers(10)) AS total,
    total * 2 AS doubled,
SELECT total, doubled;
```