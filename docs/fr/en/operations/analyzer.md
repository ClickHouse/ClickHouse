---
description: "Page détaillant l’analyseur de requêtes ClickHouse"
keywords: ['analyzer']
sidebar_label: 'Analyseur'
slug: /operations/analyzer
title: 'Analyseur'
doc_type: 'reference'
---

Dans ClickHouse version `24.3`, le nouvel analyseur de requêtes a été activé par défaut.
Vous trouverez plus de détails sur son fonctionnement [ici](/fr/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

<div id="known-incompatibilities">
  ## Incompatibilités connues
</div>

Bien qu’il corrige un grand nombre de bogues et apporte de nouvelles optimisations, il introduit également certains changements non rétrocompatibles dans le comportement de ClickHouse. Veuillez prendre connaissance des modifications ci-dessous afin de déterminer comment réécrire vos requêtes pour l’analyseur.

<div id="invalid-queries-are-no-longer-optimized">
  ### Les requêtes invalides ne sont plus optimisées
</div>

L’ancienne infrastructure de planification des requêtes appliquait des optimisations au niveau de l’AST avant l’étape de validation de la requête.
Ces optimisations pouvaient réécrire la requête initiale pour la rendre valide et exécutable.

Dans l’analyseur, la validation de la requête a lieu avant l’étape d’optimisation.
Cela signifie que les requêtes invalides qu’il était auparavant possible d’exécuter ne sont plus prises en charge.
Dans ce cas, la requête doit être corrigée manuellement.

<div id="example-1">
  #### Exemple 1
</div>

La requête suivante utilise la colonne `number` dans la liste de projection, alors que seul `toString(number)` est disponible après l’agrégation.
Dans l’ancien analyseur, `GROUP BY toString(number)` était optimisé en `GROUP BY number,`, ce qui rendait la requête valide.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### Exemple 2
</div>

Le même problème se pose dans cette requête. La colonne `number` est utilisée après l’agrégation avec une autre clé.
L’ancien analyseur de requêtes corrigeait cette requête en déplaçant le filtre `number > 5` de la clause `HAVING` vers la clause `WHERE`.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

Pour corriger la requête, vous devez déplacer toutes les conditions qui s&#39;appliquent à des colonnes non agrégées dans la clause `WHERE` afin de respecter la syntaxe SQL standard :

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### `CREATE VIEW` avec une requête non valide
</div>

L’analyseur effectue toujours une vérification des types.
Auparavant, il était possible de créer une `VIEW` avec une requête `SELECT` non valide.
Elle échouait ensuite lors du premier `SELECT` ou `INSERT` (dans le cas d’une `MATERIALIZED VIEW`).

Il n’est désormais plus possible de créer une `VIEW` de cette manière.

<div id="example-view">
  #### Exemple
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### Incompatibilités connues de la clause `JOIN`
</div>

<div id="join-using-column-from-projection">
  #### `JOIN` avec une colonne issue d’une projection
</div>

Par défaut, un alias de la liste `SELECT` ne peut pas être utilisé comme clé `JOIN USING`.

Lorsqu’il est activé, le nouveau paramètre `analyzer_compatibility_join_using_top_level_identifier` modifie le comportement de `JOIN USING` pour privilégier la résolution des identifiants à partir des expressions de la liste de projection de la requête `SELECT`, au lieu d’utiliser directement les colonnes de la table de gauche.

Par exemple :

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

Avec `analyzer_compatibility_join_using_top_level_identifier` défini sur `true`, la condition de jointure est interprétée comme `t1.a + 1 = t2.b`, conformément au comportement des versions antérieures.
Le résultat sera `2, 'two'`.
Lorsque le paramètre est défini sur `false`, la condition de jointure devient par défaut `t1.b = t2.b`, et la requête renverra `2, 'one'`.
Si `b` n’est pas présent dans `t1`, la requête échouera avec une erreur.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### Changements de comportement avec `JOIN USING` et les colonnes `ALIAS`/`MATERIALIZED`
</div>

Dans l’analyseur, l’utilisation de `*` dans une requête `JOIN USING` faisant intervenir des colonnes `ALIAS` ou `MATERIALIZED` inclut ces colonnes dans le jeu de résultats par défaut.

Par exemple :

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

Dans l’analyseur, le résultat de cette requête inclura la colonne `payload` ainsi que `id` provenant des deux tables.
En revanche, l’analyseur précédent n’incluait ces colonnes `ALIAS` que si des paramètres spécifiques (`asterisk_include_alias_columns` ou `asterisk_include_materialized_columns`) étaient activés,
et ces colonnes pouvaient apparaître dans un ordre différent.

Afin de garantir des résultats cohérents et prévisibles, en particulier lors de la migration d’anciennes requêtes vers l’analyseur, il est conseillé de spécifier explicitement les colonnes dans la clause `SELECT` plutôt que d’utiliser `*`.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### Gestion des modificateurs de type pour les colonnes dans la clause `USING`
</div>

Dans la nouvelle version de l’analyseur, les règles de détermination du supertype commun pour les colonnes spécifiées dans la clause `USING` ont été harmonisées afin de produire des résultats plus prévisibles,
en particulier lors de l’utilisation de modificateurs de type comme `LowCardinality` et `Nullable`.

* `LowCardinality(T)` et `T` : lorsqu’une colonne de type `LowCardinality(T)` est jointe à une colonne de type `T`, le supertype commun obtenu sera `T`, ce qui supprime effectivement le modificateur `LowCardinality`.
* `Nullable(T)` et `T` : lorsqu’une colonne de type `Nullable(T)` est jointe à une colonne de type `T`, le supertype commun obtenu sera `Nullable(T)`, ce qui garantit la conservation du caractère nullable.

Par exemple :

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

Dans cette requête, le supertype commun de `id` est défini comme `String`, le modificateur `LowCardinality` de `t1` étant ignoré.

<div id="projection-column-names-changes">
  ### Modifications des noms de colonnes des projections
</div>

Lors du calcul des noms de projection, les alias ne sont pas substitués.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### Types d&#39;arguments de fonction incompatibles
</div>

Dans l&#39;analyseur, l&#39;inférence de types a lieu lors de l&#39;analyse initiale de la requête.
Cette modification signifie que les vérifications de type sont effectuées avant l&#39;évaluation à court-circuit ; les arguments de la fonction `if` doivent donc toujours avoir un supertype commun.

Par exemple, la requête suivante échoue avec `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not` :

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### Clusters hétérogènes
</div>

L’analyseur modifie considérablement le protocole de communication entre les serveurs du cluster. Il est donc impossible d’exécuter des requêtes distribuées sur des serveurs avec des valeurs différentes du paramètre `enable_analyzer`.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### Les mutations sont interprétées par l’ancien analyseur
</div>

Les mutations utilisent encore l’ancien analyseur.
Cela signifie que certaines nouvelles fonctionnalités de ClickHouse SQL ne peuvent pas être utilisées dans les mutations. Par exemple, la clause `QUALIFY`.
Vous pouvez suivre l’avancement [ici](https://github.com/ClickHouse/ClickHouse/issues/61563).

<div id="unsupported-features">
  ### Fonctionnalités non prises en charge
</div>

Voici la liste des fonctionnalités que l’analyseur ne prend pas encore en charge :

* Index Annoy.
* Index Hypothesis. Développement en cours [ici](https://github.com/ClickHouse/ClickHouse/pull/48381).
* La vue fenêtrée n’est pas prise en charge. Aucun support n’est prévu à l’avenir.

<div id="cloud-migration">
  ## Migration vers Cloud
</div>

Nous activons le nouvel analyseur de requêtes sur toutes les instances où il est actuellement désactivé afin de permettre de nouvelles optimisations fonctionnelles et de performances. Cette modification applique des règles de portée en SQL plus strictes, ce qui oblige les clients à mettre manuellement à jour les requêtes non conformes.

<div id="migration-workflow">
  ### Workflow de migration
</div>

1. Identifiez la requête en filtrant `system.query_log` à l’aide de `normalized_query_hash` :

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. Exécutez la requête en activant l’analyseur à l’aide de ces paramètres.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. Refactorisez et vérifiez le résultat de la requête afin de vous assurer qu’il correspond à la sortie générée lorsque l’analyseur est désactivé.

Veuillez consulter les incompatibilités les plus fréquentes rencontrées lors des tests internes.

<div id="unknown-expression-identifier">
  ### Identifiant d&#39;expression inconnu
</div>

Erreur : `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. Code d&#39;exception : 47

Cause : Les requêtes qui reposent sur des comportements legacy permissifs et non standard — par exemple la référence à des alias calculés dans des filtres, des projections de sous-requête ambiguës ou une portée de CTE « dynamique » — sont désormais correctement identifiées comme invalides et rejetées immédiatement.

Solution : Mettez à jour vos modèles SQL comme suit :

* Logique de filtre : déplacez la logique de WHERE vers HAVING si vous filtrez sur des résultats, ou dupliquez l&#39;expression dans WHERE si vous filtrez sur les données source.
* Portée de sous-requête : sélectionnez explicitement toutes les colonnes nécessaires à la requête externe.
* Clés de JOIN : utilisez ON avec des expressions complètes au lieu de USING si la clé est un alias.
* Dans les requêtes externes, faites référence à l&#39;alias de la sous-requête/CTE elle-même, et non aux tables qu&#39;elle contient.

<div id="non-aggregated-columns-in-group-by">
  ### Colonnes non agrégées dans GROUP BY
</div>

Erreur : `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. Code d’exception : 215

Cause : L’ancien analyseur permettait de sélectionner des colonnes absentes de la clause GROUP BY (en prenant souvent une valeur arbitraire). L’analyseur suit le SQL standard : chaque colonne sélectionnée doit être soit un agrégat, soit une clé de regroupement.

Solution : encapsulez la colonne dans `any()`, `argMax()`, ou ajoutez-la au GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### Noms de CTE en double
</div>

Erreur : `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Code d&#39;exception : 179

Cause : L’ancien analyseur permettait de définir plusieurs expressions de table communes (WITH ...) avec le même nom, en masquant la précédente. L’analyseur interdit cette ambiguïté.

Solution : Renommez les CTE en double afin qu’ils aient chacun un nom unique.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### Identifiants de colonne ambigus
</div>

Erreur : `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Code d’exception : 207

Cause : La requête fait référence à un nom de colonne présent dans plusieurs tables d’un JOIN sans préciser la table source. L’ancien analyseur déduisait souvent la colonne selon une logique interne ; l’analyseur exige un nom explicite.

Solution : Qualifiez entièrement la colonne avec table&#95;alias.column&#95;name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### Utilisation invalide de FINAL
</div>

Erreur : `Table expression modifiers FINAL are not supported for subquery...` ou `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). Codes d’exception : 1, 181

Cause : FINAL est un modificateur du stockage des tables (plus précisément [Shared]ReplacingMergeTree). L’analyseur rejette FINAL lorsqu’il est appliqué à :

* des sous-requêtes ou des tables dérivées (par ex., FROM (SELECT ...) FINAL) ;
* des moteurs de table qui ne le prennent pas en charge (par ex., SharedMergeTree).

Solution : appliquez FINAL uniquement à la table source dans la sous-requête, ou supprimez-le si le moteur ne le prend pas en charge.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### Insensibilité à la casse de la fonction `countDistinct()`
</div>

Erreur : `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. Code d’exception : 46

Cause : les noms de fonction sont sensibles à la casse ou font l’objet d’un mappage strict dans l’analyseur. `countdistinct` (entièrement en minuscules) n’est plus résolu automatiquement.

Solution : utilisez le `countDistinct` standard (camelCase) ou `uniq`, spécifique à ClickHouse.