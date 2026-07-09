---
description: 'Documentation de la recherche vectorielle exacte et approximative'
keywords: ['recherche de similarité vectorielle', 'ann', 'knn', 'hnsw', 'indices', 'index', 'plus proche voisin', 'recherche vectorielle']
sidebar_label: 'Recherche vectorielle exacte et approximative'
slug: /engines/table-engines/mergetree-family/annindexes
title: 'Recherche vectorielle exacte et approximative'
doc_type: 'guide'
---

Le problème qui consiste à trouver, pour un point donné, les N points les plus proches dans un espace multidimensionnel (vectoriel) est appelé [recherche des plus proches voisins](https://en.wikipedia.org/wiki/Nearest_neighbor_search) ou, plus simplement, recherche vectorielle.
Il existe deux grandes approches pour effectuer une recherche vectorielle :

* La recherche vectorielle exacte calcule la distance entre le point donné et tous les points de l&#39;espace vectoriel. Cela garantit la meilleure précision possible, c.-à-d. que les points renvoyés sont bien les véritables plus proches voisins. Comme l&#39;espace vectoriel est parcouru de manière exhaustive, la recherche vectorielle exacte peut être trop lente pour une utilisation en conditions réelles.
* La recherche vectorielle approximative désigne un ensemble de techniques (par ex. des structures de données spécialisées comme des graphes et des forêts aléatoires) qui calculent les résultats bien plus rapidement que la recherche vectorielle exacte. La précision des résultats est généralement &quot;suffisamment bonne&quot; pour un usage pratique. Bon nombre de techniques approximatives fournissent des paramètres permettant d&#39;ajuster le compromis entre la précision des résultats et le temps de recherche.

Une recherche vectorielle (exacte ou approximative) peut s&#39;écrire en SQL comme suit :

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

Les points de l’espace vectoriel sont stockés dans une colonne `vectors` de type tableau, par ex. [Array(Float64)](../../../sql-reference/data-types/array.md), [Array(Float32)](../../../sql-reference/data-types/array.md) ou [Array(BFloat16)](../../../sql-reference/data-types/array.md).
Le vecteur de référence est un tableau constant, fourni sous la forme d’une expression de table commune.
`<DistanceFunction>` calcule la distance entre le point de référence et tous les points stockés.
N’importe laquelle des [fonctions de distance](/fr/sql-reference/functions/distance-functions) disponibles peut être utilisée à cette fin.
`<N>` indique le nombre de voisins à renvoyer.

<div id="exact-nearest-neighbor-search">
  ## Recherche vectorielle exacte
</div>

Une recherche vectorielle exacte peut être effectuée à l’aide de la requête SELECT ci-dessus, telle quelle.
Le temps d’exécution de telles requêtes est généralement proportionnel au nombre de vecteurs stockés et à leur dimension, c’est-à-dire au nombre d’éléments du tableau.
De plus, comme ClickHouse effectue un balayage exhaustif de tous les vecteurs, le temps d’exécution dépend également du nombre de threads utilisés par la requête (voir le paramètre [max&#95;threads](../../../operations/settings/settings.md#max_threads)).

<div id="exact-nearest-neighbor-search-example">
  ### Exemple
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

renvoie

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## Recherche vectorielle approximative
</div>

<div id="vector-similarity-index">
  ### Index de similarité vectorielle
</div>

ClickHouse fournit un index spécial de « similarité vectorielle » pour effectuer une recherche vectorielle approximative.

:::note
Les index de similarité vectorielle sont disponibles à partir de la version 25.8 de ClickHouse.
Si vous rencontrez des problèmes, veuillez ouvrir une issue sur le [dépôt ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

<div id="creating-a-vector-similarity-index">
  #### Création d&#39;un index de similarité vectorielle
</div>

Un index de similarité vectorielle peut être créé sur une nouvelle table comme suit :

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

Sinon, pour ajouter un index de similarité vectorielle à une table existante :

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

Les index de similarité vectorielle sont des types particuliers d&#39;index de saut (voir [ici](mergetree.md#table_engine-mergetree-data_skipping-indexes) et [ici](../../../optimize/skipping-indexes)).
Par conséquent, l&#39;instruction `ALTER TABLE` ci-dessus ne déclenche la construction de l&#39;index que pour les nouvelles données insérées dans la table par la suite.
Pour construire l&#39;index sur les données existantes également, vous devez le matérialiser :

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

La fonction `<distance_function>` doit être

* `L2Distance`, la [distance euclidienne](https://en.wikipedia.org/wiki/Euclidean_distance), représentant la longueur du segment entre deux points dans l&#39;espace euclidien,
* `cosineDistance`, la [distance cosinus](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance), représentant l&#39;angle entre deux vecteurs non nuls, ou
* `dotProduct`, le [produit scalaire](https://en.wikipedia.org/wiki/Dot_product) (produit intérieur), représentant la somme des produits terme à terme de deux vecteurs. Équivalent à `cosineDistance` sur des données normalisées.

Pour les données normalisées, `L2Distance` est généralement le meilleur choix ; dans le cas contraire, `cosineDistance` est recommandé pour compenser les différences d&#39;échelle.

:::note
Pour les fonctions de distance `L2Distance` et `cosineDistance`, une valeur plus faible indique une similarité plus élevée, tandis que pour `dotProduct`, c&#39;est une valeur plus élevée qui indique une similarité plus élevée.
Par conséquent, les index vectoriels utilisant `L2Distance` et `cosineDistance` ne peuvent être utilisés que par des requêtes `SELECT [...] ORDER BY [...] ASC` (`ASC` étant la valeur par défaut pour `ORDER BY`), tandis que les index vectoriels construits pour `dotProduct` ne peuvent être utilisés que par des requêtes `SELECT [...] ORDER BY [...] DESC`.
:::

`<dimensions>` spécifie la cardinalité du tableau (nombre d&#39;éléments) dans la colonne sous-jacente.
Si ClickHouse rencontre un tableau avec une cardinalité différente lors de la création de l&#39;index, l&#39;index est ignoré et une erreur est renvoyée.

Le paramètre optionnel GRANULARITY `<N>` désigne la taille des granules d&#39;index (voir [ici](../../../optimize/skipping-indexes)).
Contrairement aux skip indexes classiques, qui utilisent une granularité d&#39;index par défaut de 1, les vector similarity indexes utilisent 100 millions comme granularité d&#39;index par défaut.
Cette valeur garantit que seul un petit nombre d&#39;indexes sont construits en interne, même pour les parts volumineuses.
Nous recommandons de ne modifier la granularité d&#39;index qu&#39;aux utilisateurs avancés qui comprennent les implications de ce qu&#39;ils font (voir [ci-dessous](#differences-to-regular-skipping-indexes)).

Les index de similarité vectorielle sont génériques dans le sens où ils peuvent prendre en charge différentes méthodes de recherche approximative.
La méthode effectivement utilisée est spécifiée par le paramètre `<type>`.
À ce jour, la seule méthode disponible est HNSW ([article académique](https://arxiv.org/abs/1603.09320)), une technique populaire et de pointe pour la recherche vectorielle approximative, basée sur des graphes de proximité hiérarchiques.
Si HNSW est utilisé comme type, les utilisateurs peuvent éventuellement spécifier des paramètres supplémentaires propres à HNSW :

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

Les paramètres spécifiques à HNSW suivants sont disponibles :

* `<quantization>` contrôle la quantification des vecteurs dans le graphe de proximité. Les valeurs possibles sont `f64`, `f32`, `f16`, `bf16`, `i8` ou `b1`. La valeur par défaut est `bf16`. Notez que ce paramètre n’affecte pas la représentation des vecteurs dans la colonne sous-jacente.
* `<hnsw_max_connections_per_layer>` contrôle le nombre de voisins par nœud du graphe, également appelé hyperparamètre HNSW `M`. La valeur par défaut est `32`. La valeur `0` signifie que la valeur par défaut est utilisée.
* `<hnsw_candidate_list_size_for_construction>` contrôle la taille de la liste dynamique de candidats lors de la construction du graphe HNSW, également appelée hyperparamètre HNSW `ef_construction`. La valeur par défaut est `128`. La valeur `0` signifie que la valeur par défaut est utilisée.

Les valeurs par défaut de tous les paramètres spécifiques à HNSW fonctionnent suffisamment bien dans la majorité des cas d’usage.
Nous ne recommandons donc pas de personnaliser les paramètres spécifiques à HNSW.

D’autres restrictions s’appliquent :

* Les index de similarité vectorielle ne peuvent être construits que sur des colonnes de type [Array(Float32)](../../../sql-reference/data-types/array.md), [Array(Float64)](../../../sql-reference/data-types/array.md) ou [Array(BFloat16)](../../../sql-reference/data-types/array.md). Les Array de flottants Nullable et à faible cardinalité, comme `Array(Nullable(Float32))` et `Array(LowCardinality(Float32))`, ne sont pas autorisés.
* Les index de similarité vectorielle doivent être construits sur une seule colonne.
* Les index de similarité vectorielle peuvent être construits sur des expressions calculées (par ex., `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`), mais ces index ne peuvent pas être utilisés par la suite pour une recherche approximative de voisins.
* Les index de similarité vectorielle exigent que tous les Array de la colonne sous-jacente contiennent `<dimension>` éléments — ce point est vérifié lors de la création de l’index. Pour détecter toute violation de cette exigence le plus tôt possible, les utilisateurs peuvent ajouter une [contrainte](/fr/sql-reference/statements/create/table.md#constraints) sur la colonne vectorielle, par ex., `CONSTRAINT same_length CHECK length(vectors) = 256`.
* De même, les valeurs de tableau dans la colonne sous-jacente ne doivent pas être vides (`[]`) ni avoir une valeur par défaut (également `[]`).

**Estimation de la consommation de stockage et de mémoire**

Un vecteur généré pour être utilisé avec un modèle d’IA classique (par ex. un Large Language Model, [LLM](https://en.wikipedia.org/wiki/Large_language_model)) se compose de centaines, voire de milliers de valeurs à virgule flottante.
Ainsi, une seule valeur vectorielle peut consommer plusieurs kilo-octets de mémoire.
Les utilisateurs qui souhaitent estimer l’espace de stockage requis pour la colonne vectorielle sous-jacente dans la table, ainsi que la mémoire principale nécessaire pour l’index de similarité vectorielle, peuvent utiliser les deux formules ci-dessous :

Consommation de stockage de la colonne vectorielle dans la table (non compressée) :

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

Exemple avec le [jeu de données DBpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) :

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

L’index de similarité vectorielle doit être entièrement chargé depuis le disque vers la mémoire principale pour pouvoir effectuer des recherches.
De même, l’index vectoriel est lui aussi entièrement construit en mémoire avant d’être enregistré sur disque.

Consommation mémoire requise pour charger un index vectoriel :

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

Exemple avec le [jeu de données dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) :

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

La formule ci-dessus ne prend pas en compte la mémoire supplémentaire nécessaire aux index de similarité vectorielle pour allouer des structures de données d’exécution, telles que des tampons préalloués et des caches.

<div id="using-a-vector-similarity-index">
  #### Utilisation d’un index de similarité vectorielle
</div>

:::note
Pour utiliser les index de similarité vectorielle, le paramètre [compatibility](../../../operations/settings/settings.md) doit être défini sur `''` (la valeur par défaut), ou sur `'25.1'` ou une version ultérieure.
:::

Les index de similarité vectorielle prennent en charge les requêtes SELECT de la forme suivante :

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

L&#39;optimiseur de requêtes de ClickHouse essaie de reconnaître le modèle de requête ci-dessus et d&#39;utiliser les index de similarité vectorielle disponibles.
Une requête ne peut utiliser un index de similarité vectorielle que si la fonction de distance de la requête SELECT est la même que celle définie dans l&#39;index.

Les utilisateurs avancés peuvent fournir une valeur personnalisée pour le paramètre [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) (également connu sous le nom d&#39;hyperparamètre HNSW &quot;ef&#95;search&quot;) afin d&#39;ajuster la taille de la liste de candidats pendant la recherche (par ex.  `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
La valeur par défaut de ce paramètre, 256, fonctionne bien dans la majorité des cas d&#39;usage.
Des valeurs de paramètre plus élevées offrent une meilleure précision au prix de performances moindres.

Si la requête peut utiliser un index de similarité vectorielle, ClickHouse vérifie que la valeur LIMIT `<N>` fournie dans les requêtes SELECT reste dans des limites raisonnables.
Plus précisément, une erreur est renvoyée si `<N>` est supérieur à la valeur du paramètre [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries), dont la valeur par défaut est 100.
Des valeurs LIMIT trop élevées peuvent ralentir les recherches et indiquent généralement une erreur d&#39;utilisation.

Pour vérifier si une requête SELECT utilise un index de similarité vectorielle, vous pouvez faire précéder la requête de `EXPLAIN indexes = 1`.

Par exemple, la requête

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

peut renvoyer

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Dans cet exemple, 1 million de vecteurs du [jeu de données dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M), chacun de dimension 1536, sont stockés dans 575 granules, soit 1,7k lignes par granule.
La requête demande 10 voisins, et l’index de similarité vectorielle trouve ces 10 voisins dans 10 granules distincts.
Ces 10 granules seront lus pendant l’exécution de la requête.

Les index de similarité vectorielle sont utilisés si le résultat contient `Skip` ainsi que le nom et le type de l’index vectoriel (dans l’exemple, `idx` et `vector_similarity`).
Dans ce cas, l’index de similarité vectorielle a permis d’ignorer deux granules sur quatre, soit 50 % des données.
Plus il est possible d’ignorer de granules, plus l’utilisation de l’index est efficace.

:::tip
Pour forcer l’utilisation de l’index, vous pouvez exécuter la requête SELECT avec le paramètre [force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) (indiquez le nom de l’index comme valeur du paramètre).
:::

**Post-filtrage et pré-filtrage**

Les utilisateurs peuvent éventuellement spécifier une clause `WHERE` avec des conditions de filtre supplémentaires pour la requête SELECT.
ClickHouse évaluera ces conditions de filtre en utilisant une stratégie de post-filtrage ou de pré-filtrage.
En bref, ces deux stratégies déterminent l’ordre dans lequel les filtres sont évalués :

* Le post-filtrage signifie que l’index de similarité vectorielle est évalué en premier, puis ClickHouse évalue le ou les filtres supplémentaires spécifiés dans la clause `WHERE`.
* Le pré-filtrage signifie que l’ordre d’évaluation des filtres est inversé.

Ces stratégies présentent différents compromis :

* Le post-filtrage présente un problème général : il peut renvoyer moins de lignes que le nombre demandé dans la clause `LIMIT <N>`. Cette situation se produit lorsqu’une ou plusieurs lignes de résultat renvoyées par l’index de similarité vectorielle ne satisfont pas aux filtres supplémentaires.
* Le pré-filtrage reste généralement un problème non résolu. Certaines bases de données vectorielles spécialisées proposent des algorithmes de pré-filtrage, mais la plupart des bases de données relationnelles (y compris ClickHouse) se rabattent sur une recherche exacte des plus proches voisins, c’est-à-dire un balayage exhaustif sans index.

La stratégie utilisée dépend de la condition de filtrage.

*Les filtres supplémentaires font partie de la clé de partition*

Si la condition de filtrage supplémentaire fait partie de la clé de partition, ClickHouse appliquera alors l’élagage des partitions.
Par exemple, une table est partitionnée par plage sur la colonne `year`, et la requête suivante est exécutée :

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse ignorera toutes les partitions sauf celle de 2025.

*Les filtres supplémentaires ne peuvent pas être évalués à l’aide des index*

Si des conditions de filtre supplémentaires ne peuvent pas être évaluées à l’aide des index (index de clé primaire, index de saut), ClickHouse appliquera un post-filtrage.

*Les filtres supplémentaires peuvent être évalués à l’aide de l’index de clé primaire*

Si des conditions de filtre supplémentaires peuvent être évaluées à l’aide de la [clé primaire](mergetree.md#primary-key) (c’est-à-dire qu’elles forment un préfixe de la clé primaire) et

* la condition de filtre élimine au moins une ligne dans une part, ClickHouse basculera vers le pré-filtrage pour les plages « survivantes » au sein de la part,
* la condition de filtre n’élimine aucune ligne dans une part, ClickHouse effectuera un post-filtrage pour la part.

En pratique, ce dernier cas est plutôt peu probable.

*Les filtres supplémentaires peuvent être évalués à l’aide d’un index de saut*

Si des conditions de filtre supplémentaires peuvent être évaluées à l’aide des [index de saut](mergetree.md#table_engine-mergetree-data_skipping-indexes) (index minmax, index set, etc.), ClickHouse effectue un post-filtrage.
Dans ce cas, l’index de similarité vectorielle est évalué en premier, car il est censé éliminer davantage de lignes que les autres index de saut.

Pour mieux contrôler le post-filtrage et le pré-filtrage, deux paramètres peuvent être utilisés :

Le paramètre [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy) (par défaut : `auto`, qui implémente les heuristiques ci-dessus) peut être défini sur `prefilter`.
Cela est utile pour forcer le pré-filtrage lorsque les conditions de filtre supplémentaires sont extrêmement sélectives.
Par exemple, la requête suivante peut bénéficier du pré-filtrage :

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

En supposant que seul un très petit nombre de livres coûtent moins de 2 dollars, le post-filtrage peut ne renvoyer aucune ligne, car les 10 meilleures correspondances renvoyées par l’index vectoriel peuvent toutes avoir un prix supérieur à 2 dollars.
En forçant le pré-filtrage (ajoutez `SETTINGS vector_search_filter_strategy = 'prefilter'` à la requête), ClickHouse commence par trouver tous les livres dont le prix est inférieur à 2 dollars, puis exécute une recherche vectorielle en brute-force sur les livres trouvés.

Comme autre approche pour résoudre le problème ci-dessus, le paramètre [vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (par défaut : `1.0`, maximum : `1000.0`) peut être configuré avec une valeur &gt; `1.0` (par exemple, `2.0`).
Le nombre de plus proches voisins récupérés depuis l’index vectoriel est multiplié par la valeur du paramètre, puis le filtre supplémentaire est appliqué à ces lignes afin de renvoyer le nombre de lignes indiqué par LIMIT.
À titre d’exemple, nous pouvons relancer la requête, mais avec le multiplicateur `3.0` :

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse récupérera 3.0 x 10 = 30 plus proches voisins depuis l’index vectoriel dans chaque partie de données, puis appliquera les filtres supplémentaires.
Seuls les dix voisins les plus proches seront renvoyés.
Notez que le paramètre `vector_search_index_fetch_multiplier` peut atténuer le problème, mais dans les cas extrêmes (condition WHERE très sélective), il est toujours possible que moins de N lignes demandées soient renvoyées.

**Recalcul du score**

Les index de saut dans ClickHouse filtrent généralement au niveau de la granule, c’est-à-dire qu’une recherche dans un index de saut renvoie (en interne) une liste de granules potentiellement correspondantes, ce qui réduit la quantité de données lues lors du balayage suivant.
Cela fonctionne bien pour les index de saut en général, mais dans le cas des index de similarité vectorielle, cela crée un &quot;décalage de granularité&quot;.
Plus précisément, l’index de similarité vectorielle détermine les numéros de ligne des N vecteurs les plus similaires pour un vecteur de référence donné, mais il doit ensuite extrapoler ces numéros de ligne en numéros de granule.
ClickHouse charge alors ces granules depuis le disque et recalcule la distance pour tous les vecteurs qu’elles contiennent.
Cette étape s’appelle le rescoring et, bien qu’elle puisse théoriquement améliorer la précision — rappelez-vous que l’index de similarité vectorielle ne renvoie qu’un résultat *approximatif* — elle n’est manifestement pas optimale en termes de performances.

ClickHouse propose donc une optimisation qui désactive le rescoring et renvoie directement depuis l’index les vecteurs les plus similaires ainsi que leurs distances.
L’optimisation est activée par défaut, voir le paramètre [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring).
Dans les grandes lignes, son fonctionnement est le suivant : ClickHouse met à disposition les vecteurs les plus similaires et leurs distances sous la forme d’une colonne virtuelle `_distances`.
Pour le voir, exécutez une requête de recherche vectorielle avec `EXPLAIN header = 1` :

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
Une requête exécutée sans rescoring (`vector_search_with_rescoring = 0`) et avec les répliques parallèles activées peut se rabattre sur le rescoring.
:::

<div id="performance-tuning">
  #### Optimisation des performances
</div>

**Réglage de la compression**

Dans la quasi-totalité des cas d’usage, les vecteurs de la colonne sous-jacente sont denses et se compressent mal.
Par conséquent, la [compression](/fr/sql-reference/statements/create/table.md#column_compression_codec) ralentit les insertions et les lectures dans la colonne vectorielle.
Nous recommandons donc de désactiver la compression.
Pour cela, spécifiez `CODEC(NONE)` pour la colonne vectorielle comme suit :

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**Ajuster la création des index**

Le cycle de vie des index de similarité vectorielle est lié à celui des parts.
Autrement dit, chaque fois qu&#39;une nouvelle part avec un index de similarité vectorielle défini est créée, l&#39;index l&#39;est aussi.
Cela se produit généralement lorsque des données sont [insérées](https://clickhouse.com/docs/guides/inserting-data) ou lors des [fusions](https://clickhouse.com/docs/merges).
Malheureusement, HNSW est connu pour ses temps de création d&#39;index élevés, qui peuvent ralentir considérablement les insertions et les fusions.
Les index de similarité vectorielle ne devraient idéalement être utilisés que si les données sont immuables ou rarement modifiées.

Pour accélérer la création des index, les techniques suivantes peuvent être utilisées :

Premièrement, la création des index peut être parallélisée.
Le nombre maximal de threads de création d&#39;index peut être configuré à l&#39;aide du paramètre serveur [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size).
Pour des performances optimales, la valeur de ce paramètre doit être définie sur le nombre de cœurs de CPU.

Deuxièmement, pour accélérer les instructions INSERT, les utilisateurs peuvent désactiver la création des index de saut sur les parts nouvellement insérées à l&#39;aide du paramètre de session [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert).
Les requêtes SELECT sur ces parts se rabattront sur une recherche exacte.
Comme les parts insérées ont tendance à être petites par rapport à la taille totale de la table, l&#39;impact sur les performances devrait être négligeable.

Troisièmement, pour accélérer les fusions, les utilisateurs peuvent désactiver la création des index de saut sur les parts fusionnées à l&#39;aide du paramètre de session [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge).
Cela, conjointement avec l&#39;instruction [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), permet un contrôle explicite du cycle de vie des index de similarité vectorielle.
Par exemple, la création des index peut être différée jusqu&#39;à ce que toutes les données aient été ingérées ou jusqu&#39;à une période de faible charge système, comme le week-end.

**Ajuster l&#39;utilisation des index**

Les requêtes SELECT doivent charger les index de similarité vectorielle en mémoire vive pour pouvoir les utiliser.
Pour éviter qu&#39;un même index de similarité vectorielle soit chargé à répétition en mémoire vive, ClickHouse fournit un cache en mémoire vive dédié à ces index.
Plus ce cache est grand, moins il y aura de chargements inutiles.
La taille maximale du cache peut être configurée à l&#39;aide du paramètre serveur [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size).
Par défaut, le cache peut atteindre 5 Go.

Les messages de journal suivants (`system.text_log`) indiquent que l&#39;index de similarité vectorielle est en cours de chargement.
Si de tels messages apparaissent de manière répétée pour différentes requêtes de recherche vectorielle, cela indique que la taille du cache est trop faible.

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
Le cache de l’index de similarité vectorielle stocke les granules de l’index vectoriel.
Si certains granules de l’index vectoriel sont plus volumineux que le cache, ils ne seront pas mis en cache.
Veillez donc à calculer la taille de l’index vectoriel (à l’aide de la formule figurant dans &quot;Estimating storage and memory consumption&quot; ou [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)) et à dimensionner le cache en conséquence.
:::

*Nous rappelons que la vérification du cache de l’index vectoriel et, si nécessaire, son augmentation doivent constituer la première étape de l’analyse de requêtes de recherche vectorielle lentes.*

La taille actuelle du cache de l’index de similarité vectorielle est indiquée dans [system.metrics](../../../operations/system-tables/metrics.md) :

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

Les succès et les échecs du cache pour une requête ayant un certain identifiant de requête peuvent être consultés dans [system.query&#95;log](../../../operations/system-tables/query_log.md) :

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

Pour les cas d’usage en production, nous recommandons de dimensionner le cache de sorte que tous les index vectoriels restent en mémoire en permanence.

**Réglage de la quantification**

La [quantification](https://huggingface.co/blog/embedding-quantization) est une technique qui permet de réduire l’empreinte mémoire des vecteurs et les coûts de calcul liés à la construction et au parcours des index vectoriels.
Les index vectoriels de ClickHouse prennent en charge les options de quantification suivantes :

| Quantization      | Nom                          | Stockage par dimension |
| ----------------- | ---------------------------- | ---------------------- |
| f32               | Précision simple             | 4 octets               |
| f16               | Demi-précision               | 2 octets               |
| bf16 (par défaut) | Demi-précision (brain float) | 2 octets               |
| i8                | Quart de précision           | 1 octet                |
| b1                | Binaire                      | 1 bit                  |

La quantification réduit la précision des recherches vectorielles par rapport à une recherche sur les valeurs d’origine en virgule flottante pleine précision (`f32`).
Cependant, sur la plupart des jeux de données, la quantification brain float en demi-précision (`bf16`) entraîne une perte de précision négligeable ; c’est pourquoi les index de similarité vectorielle utilisent cette technique de quantification par défaut.
La quantification en quart de précision (`i8`) et la quantification binaire (`b1`) entraînent une perte de précision sensible dans les recherches vectorielles.
Nous recommandons ces deux types de quantification uniquement si la taille de l’index de similarité vectorielle est nettement supérieure à la mémoire DRAM disponible.
Dans ce cas, nous suggérons également d’activer la réévaluation ([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier), [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) pour améliorer la précision.
La quantification binaire est recommandée uniquement 1) pour les embeddings normalisés (c.-à-d. longueur du vecteur = 1, les modèles OpenAI sont généralement normalisés), et 2) si la distance cosinus est utilisée comme fonction de distance.
La quantification binaire utilise en interne la distance de Hamming pour construire et interroger le graphe de proximité.
L’étape de réévaluation utilise les vecteurs d’origine en pleine précision stockés dans la table pour identifier les plus proches voisins via la distance cosinus.

**Réglage du transfert de données**

Le vecteur de référence dans une requête de recherche vectorielle est fourni par l’utilisateur et généralement obtenu via un appel à un grand modèle de langage (LLM).
Un code Python typique qui exécute une recherche vectorielle dans ClickHouse peut ressembler à ceci

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

Les vecteurs d&#39;embedding (`search_v` dans l&#39;extrait ci-dessus) peuvent avoir un très grand nombre de dimensions.
Par exemple, OpenAI propose des modèles qui génèrent des vecteurs d&#39;embedding de 1 536, voire 3 072 dimensions.
Dans le code ci-dessus, le driver Python de ClickHouse remplace le vecteur d&#39;embedding par une chaîne lisible par l&#39;humain, puis envoie la requête SELECT entièrement sous forme de chaîne.
En supposant que le vecteur d&#39;embedding se compose de 1 536 valeurs en virgule flottante simple précision, la chaîne envoyée atteint une longueur de 20 kB.
Cela entraîne une forte utilisation du CPU pour la tokenisation, l&#39;analyse syntaxique et l&#39;exécution de milliers de conversions de chaînes en nombres à virgule flottante.
De plus, cela nécessite un espace important dans le fichier journal du serveur ClickHouse, ce qui gonfle également `system.query_log`.

Notez que la plupart des modèles LLM renvoient un vecteur d&#39;embedding sous la forme d&#39;une liste ou d&#39;un tableau NumPy de flottants natifs.
Nous recommandons donc aux applications Python de lier le paramètre du vecteur de référence sous forme binaire en utilisant le style suivant :

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

Dans l’exemple, le vecteur de référence est envoyé tel quel sous forme binaire, puis réinterprété comme un tableau de flottants sur le serveur.
Cela permet d’économiser du temps CPU côté serveur et évite d’alourdir les logs du serveur ainsi que `system.query_log`.

<div id="administration">
  #### Administration et supervision
</div>

La taille des index de similarité vectorielle sur disque peut être obtenue à partir de [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices) :

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

Exemple de sortie :

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### Différences par rapport aux index de saut ordinaires
</div>

Comme tous les [index de saut](/fr/optimize/skipping-indexes) ordinaires, les index de similarité vectorielle sont construits sur des granules, et chaque bloc indexé se compose de `GRANULARITY = [N]` granules (`[N]` = 1 par défaut pour les index de saut ordinaires).
Par exemple, si l’index granularity primaire de la table est de 8192 (paramètre `index_granularity = 8192`) et que `GRANULARITY = 2`, alors chaque bloc indexé contiendra 16384 lignes.
Cependant, les structures de données et les algorithmes de recherche approximative de voisins sont intrinsèquement orientés lignes.
Ils stockent une représentation compacte d’un ensemble de lignes et renvoient également des lignes pour les requêtes de recherche vectorielle.
Cela entraîne des différences parfois peu intuitives dans la façon dont les index de similarité vectorielle se comportent par rapport aux index de saut ordinaires.

Lorsqu’un utilisateur définit un index de similarité vectorielle sur une colonne, ClickHouse crée en interne un « sous-index » de similarité vectorielle pour chaque bloc d’index.
Le sous-index est « local » au sens où il ne connaît que les lignes du bloc d’index qui le contient.
Dans l’exemple précédent, en supposant qu’une colonne comporte 65536 lignes, on obtient quatre blocs d’index (couvrant huit granules) et un sous-index de similarité vectorielle pour chaque bloc d’index.
En théorie, un sous-index peut renvoyer directement les lignes contenant les N points les plus proches dans son bloc d’index.
Cependant, comme ClickHouse charge les données du disque en mémoire à la granularité des granules, les sous-index extrapolent les lignes correspondantes à la granularité des granules.
Cela diffère des index de saut ordinaires, qui ignorent les données à la granularité des blocs d’index.

Le paramètre `GRANULARITY` détermine combien de sous-index de similarité vectorielle sont créés.
Des valeurs plus élevées de `GRANULARITY` signifient des sous-index de similarité vectorielle moins nombreux mais plus grands, jusqu’au point où une colonne (ou une data part de colonne) ne possède plus qu’un seul sous-index.
Dans ce cas, le sous-index a une vue « globale » de toutes les lignes de la colonne et peut renvoyer directement tous les granules de la colonne (part) contenant des lignes pertinentes (il y a au plus `LIMIT [N]` granules de ce type).
Dans un second temps, ClickHouse chargera ces granules et identifiera les meilleures lignes réelles en effectuant un calcul de distance en brute-force sur toutes les lignes des granules.
Avec une petite valeur de `GRANULARITY`, chacun des sous-index renvoie jusqu’à `LIMIT N` granules.
Par conséquent, davantage de granules doivent être chargés puis post-filtrés.
Notez que la précision de recherche est tout aussi bonne dans les deux cas ; seule la performance de traitement diffère.
Il est généralement recommandé d’utiliser une grande valeur de `GRANULARITY` pour les index de similarité vectorielle et de revenir à des valeurs plus petites de `GRANULARITY` uniquement en cas de problèmes, comme une consommation mémoire excessive des structures de similarité vectorielle.
Si aucun `GRANULARITY` n’a été spécifié pour les index de similarité vectorielle, la valeur par défaut est de 100 millions.

<div id="approximate-nearest-neighbor-search-example">
  #### Exemple
</div>

Requêtes :

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

Autres jeux de données d’exemple utilisant la recherche vectorielle approximative :

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

Une approche courante pour accélérer la recherche vectorielle exacte consiste à utiliser un [type de données float](../../../sql-reference/data-types/float.md) de plus faible précision.
Par exemple, si les vecteurs sont stockés sous la forme `Array(BFloat16)` au lieu de `Array(Float32)`, la taille des données est réduite de moitié, et le temps d&#39;exécution des requêtes devrait diminuer proportionnellement.
Cette méthode est appelée quantification. Bien qu&#39;elle accélère les calculs, elle peut réduire la précision des résultats malgré un balayage exhaustif de tous les vecteurs.

Avec la quantification traditionnelle, on perd en précision à la fois pendant la recherche et lors du stockage des données. Dans l&#39;exemple ci-dessus, nous stockerions `BFloat16` au lieu de `Float32`, ce qui signifie qu&#39;il serait ensuite impossible d&#39;effectuer une recherche plus précise, même si on le souhaitait. Une autre approche consiste à stocker deux copies des données : une quantifiée et une en pleine précision. Bien que cela fonctionne, cela nécessite un stockage redondant. Prenons un scénario dans lequel `Float64` est utilisé comme donnée d&#39;origine et où l&#39;on souhaite exécuter des recherches avec différents niveaux de précision (16 bits, 32 bits ou 64 bits complets). Il faudrait alors stocker trois copies distinctes des données.

ClickHouse propose le type de données Quantized Bit (`QBit`), qui répond à ces limites en :

1. Stockant les données d&#39;origine en pleine précision.
2. Permettant de spécifier la précision de quantification au moment de la requête.

Cela est rendu possible en stockant les données dans un format regroupé par bit (c&#39;est-à-dire que tous les i-ièmes bits de tous les vecteurs sont stockés ensemble), ce qui permet d&#39;effectuer des lectures uniquement au niveau de précision demandé. Vous bénéficiez ainsi des gains de vitesse apportés par la réduction des E/S et des calculs grâce à la quantification, tout en conservant toutes les données d&#39;origine disponibles lorsque nécessaire. Lorsque la précision maximale est sélectionnée, la recherche devient exacte.

Pour déclarer une colonne de type `QBit`, utilisez la syntaxe suivante :

```sql
column_name QBit(element_type, dimension[, stride])
```

Où :

* `element_type` – le type de chaque élément du vecteur. Les types pris en charge sont `Int8`, `BFloat16`, `Float32` et `Float64`
* `dimension` – le nombre d’éléments de chaque vecteur
* `stride` – facultatif. Un diviseur de `dimension` qui répartit les dimensions en `dimension / stride` groupes contigus stockés dans des flux distincts, de sorte qu’une recherche portant uniquement sur les dimensions initiales lit moins de flux (utile pour les embeddings Matryoshka). La valeur par défaut est `dimension` ; dans ce cas, le type est identique octet pour octet à un `QBit` sans stride. Voir la [page du type de données `QBit`](/fr/sql-reference/data-types/qbit) pour plus de détails.

<div id="qbit-create">
  #### Création d’une table `QBit` et ajout de données
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### Recherche vectorielle avec `QBit`
</div>

Trouvons les plus proches voisins d’un vecteur représentant le mot &#39;lemon&#39; à l’aide de la distance L2. Le troisième paramètre de la fonction de distance spécifie la précision en bits : des valeurs plus élevées offrent une meilleure précision, mais nécessitent davantage de calculs.

Vous trouverez [ici](../../../sql-reference/data-types/qbit.md#vector-search-functions) toutes les fonctions de distance disponibles pour `QBit`.

**Recherche en précision totale (64 bits) :**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**Recherche en précision réduite :**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

Notez qu’avec une quantification sur 12 bits, nous obtenons une bonne approximation des distances tout en accélérant l’exécution des requêtes. L’ordre relatif reste globalement cohérent, &#39;apple&#39; demeurant toujours la correspondance la plus proche.

<div id="qbit-performance">
  #### Considérations relatives aux performances
</div>

Le gain de performances de `QBit` vient de la réduction des opérations d’E/S, car moins de données doivent être lues depuis le stockage lorsque l’on utilise une précision plus faible. De plus, lorsque `QBit` contient des données `Float32`, si le paramètre de précision est inférieur ou égal à 16, la réduction des calculs apporte des gains supplémentaires. Le paramètre de précision contrôle directement le compromis entre précision et vitesse :

* **Précision plus élevée** (plus proche de la largeur des données d’origine) : résultats plus précis, requêtes plus lentes
* **Précision plus faible** : requêtes plus rapides avec des résultats approximatifs, utilisation mémoire réduite

<div id="references">
  ### Références
</div>

Articles de blog :

* [Recherche vectorielle avec ClickHouse - Partie 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [Recherche vectorielle avec ClickHouse - Partie 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [Nous avons créé un moteur de recherche vectorielle qui vous permet de choisir la précision au moment de l’exécution de la requête](https://clickhouse.com/blog/qbit-vector-search)