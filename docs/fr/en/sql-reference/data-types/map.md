---
description: 'Documentation du type de données Map dans ClickHouse'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'reference'
---

Le type de données `Map(K, V)` stocke des paires clé-valeur.

Contrairement à d&#39;autres bases de données, les maps ne sont pas uniques dans ClickHouse, c&#39;est-à-dire qu&#39;une map peut contenir deux éléments ayant la même clé.
(Cela s&#39;explique par le fait que les maps sont implémentées en interne sous la forme de `Array(Tuple(K, V))`.)

Vous pouvez utiliser la syntaxe `m[k]` pour obtenir la valeur associée à la clé `k` dans la map `m`.
De plus, `m[k]` parcourt la map, c&#39;est-à-dire que le temps d&#39;exécution de l&#39;opération est linéaire par rapport à la taille de la map.

**Paramètres**

* `K` — Le type des clés de la Map. N&#39;importe quel type, à l&#39;exception de [Nullable](../../sql-reference/data-types/nullable.md) et de [LowCardinality](../../sql-reference/data-types/lowcardinality.md) lorsqu&#39;il est imbriqué avec des types [Nullable](../../sql-reference/data-types/nullable.md).
* `V` — Le type des valeurs de la Map. N&#39;importe quel type.

**Exemples**

Créez une table avec une colonne de type map :

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

Pour sélectionner les valeurs de `key2` :

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

Si la clé demandée `k` n’est pas présente dans la map, `m[k]` renvoie la valeur par défaut du type de valeur, par exemple `0` pour les types entiers et `''` pour les types String.
Pour vérifier si une clé existe dans une map, vous pouvez utiliser la fonction [mapContains](/fr/sql-reference/functions/tuple-map-functions#mapContainsKey).

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

<div id="converting-tuple-to-map">
  ## Conversion de Tuple en Map
</div>

Les valeurs de type `Tuple()` peuvent être converties en valeurs de type `Map()` à l’aide de la fonction [CAST](/fr/sql-reference/functions/type-conversion-functions#CAST) :

**Exemple**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

<div id="reading-subcolumns-of-map">
  ## Lecture des sous-colonnes de type Map
</div>

Pour éviter de lire l’intégralité du Map, vous pouvez, dans certains cas, utiliser les sous-colonnes `keys` et `values`.

**Exemple**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

<div id="bucketed-map-serialization">
  ## Sérialisation des Map par buckets dans MergeTree
</div>

Par défaut, une colonne `Map` dans MergeTree est stockée sous la forme d’un seul flux `Array(Tuple(K, V))`.
La lecture d’une seule clé avec `m['key']` impose de parcourir toute la colonne — chaque paire clé-valeur de chaque ligne — même si une seule clé est demandée.
Pour les maps contenant un grand nombre de clés distinctes, cela devient un goulot d’étranglement.

La sérialisation par buckets (`with_buckets`) répartit les paires clé-valeur dans plusieurs sous-flux indépendants (buckets) à l’aide d’un hachage de la clé.
Lorsqu’une requête accède à `m['key']`, seul le bucket contenant cette clé est lu sur le disque, tous les autres buckets étant ignorés.

<div id="enabling-bucketed-serialization">
  ### Activer la sérialisation par bucket
</div>

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

Pour éviter de ralentir les `inserts`, vous pouvez conserver la sérialisation `basic` pour les `parts` de niveau zéro (créées lors de `INSERT`) et n’utiliser `with_buckets` que pour les `parts` fusionnées :

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

<div id="how-it-works">
  ### Fonctionnement
</div>

Lorsqu&#39;une part de données est écrite avec la sérialisation `with_buckets` :

1. Le nombre moyen de clés par ligne est calculé à partir des statistiques du bloc.
2. Le nombre de buckets est déterminé par la stratégie configurée (voir [Paramètres](#bucketed-map-settings)).
3. Chaque paire clé-valeur est affectée à un bucket en hachant la clé : `bucket = hash(key) % num_buckets`.
4. Chaque bucket est stocké comme un sous-flux indépendant avec ses propres clés, valeurs et offsets.
5. Un flux de métadonnées `buckets_info` enregistre le nombre de buckets et les statistiques.

Lorsqu&#39;une requête lit une clé spécifique (`m['key']`), l&#39;optimiseur réécrit l&#39;expression en sous-colonne de clé (`m.key_<serialized_key>`).
La couche de sérialisation calcule à quel bucket appartient la clé demandée et ne lit que ce bucket sur le disque.

Lorsque la Map complète est lue (par ex., `SELECT m`), tous les buckets sont lus puis réassemblés dans la Map d&#39;origine. C&#39;est plus lent que la sérialisation `basic` en raison du surcoût lié à la lecture et à la fusion de plusieurs sous-flux.

:::note
L&#39;ordre des clés dans une valeur de type Map peut différer de l&#39;ordre d&#39;insertion d&#39;origine lors de l&#39;utilisation de la sérialisation `with_buckets`. Les clés sont réparties entre les buckets par hash, puis réassemblées dans l&#39;ordre des buckets, et non dans l&#39;ordre d&#39;insertion. Avec la sérialisation `basic`, l&#39;ordre des clés des Maps insérées est préservé.
:::

Le nombre de buckets peut varier d&#39;une part à l&#39;autre. Lorsque des parts avec des nombres de buckets différents sont fusionnées, le nombre de buckets de la nouvelle part est recalculé à partir des statistiques fusionnées. Les parts utilisant les sérialisations `basic` et `with_buckets` peuvent coexister dans la même table et sont fusionnées de manière transparente.

<div id="bucketed-map-settings">
  ### Paramètres
</div>

| Paramètre                                        | Par défaut | Description                                                                                                                                                                                                                                                                                              |
| ------------------------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `map_serialization_version`                      | `basic`    | Format de sérialisation des colonnes `Map`. `basic` stocke les données sous la forme d’un flux de tableau unique. `with_buckets` répartit les clés dans des buckets pour accélérer la lecture d’une clé unique.                                                                                          |
| `map_serialization_version_for_zero_level_parts` | `basic`    | Format de sérialisation pour les parts de niveau zéro (créées par `INSERT`). Permet de conserver `basic` pour les insertions afin d’éviter la surcharge d’écriture, tandis que les parts fusionnées utilisent `with_buckets`.                                                                            |
| `max_buckets_in_map`                             | `32`       | Limite supérieure du nombre de buckets. Le nombre réel dépend de `map_buckets_strategy`. La valeur maximale autorisée est 256.                                                                                                                                                                           |
| `map_buckets_strategy`                           | `sqrt`     | Stratégie de calcul du nombre de buckets à partir de la taille moyenne de la map : `constant` — utilise toujours `max_buckets_in_map` ; `sqrt` — utilise `round(coefficient * sqrt(avg_size))` ; `linear` — utilise `round(coefficient * avg_size)`. Le résultat est limité à `[1, max_buckets_in_map]`. |
| `map_buckets_coefficient`                        | `1.0`      | Multiplicateur pour les stratégies `sqrt` et `linear`. Ignoré lorsque la stratégie est `constant`.                                                                                                                                                                                                       |
| `map_buckets_min_avg_size`                       | `32`       | Nombre moyen minimal de clés par ligne pour activer la répartition en buckets. Si la moyenne est inférieure à ce seuil, un seul bucket est utilisé quels que soient les autres paramètres. Définissez `0` pour désactiver ce seuil.                                                                      |

<div id="performance-trade-offs">
  ### Compromis de performances
</div>

Le tableau suivant résume l&#39;impact sur les performances de `with_buckets` par rapport à la sérialisation `basic` pour différentes tailles de Map (de 10 à 10 000 clés par ligne). Le nombre de buckets a été déterminé selon la stratégie `sqrt`, avec un plafond de 32. Les chiffres exacts dépendent des types de clés/valeurs, de la distribution des données et du matériel.

| Opération                                      | 10 clés              | 100 clés             | 1 000 clés           | 10 000 clés          | Remarques                                                                                                                                                                                                                                    |
| ---------------------------------------------- | -------------------- | -------------------- | -------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Recherche d&#39;une seule clé** (`m['key']`) | 1.6–3.2x plus rapide | 4.5–7.7x plus rapide | 16–39x plus rapide   | 21–49x plus rapide   | Lit un seul bucket au lieu de la colonne entière.                                                                                                                                                                                            |
| **5 recherches de clés**                       | ~1x                  | 1.5–3.1x plus rapide | 2.9–8.3x plus rapide | 4.5–6.7x plus rapide | Chaque clé lit son propre bucket ; certains buckets peuvent se recouper.                                                                                                                                                                     |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1.5–3.0x plus rapide | 2.9–7.3x plus rapide | 5.3–31x plus rapide  | 20–45x plus rapide   | Le filtre PREWHERE ne lit qu&#39;un seul bucket ; la Map complète n&#39;est lue que pour les lignes correspondantes. Le gain dépend de la sélectivité — moins il y a de granules correspondants, moins il y a d&#39;E/S sur la Map complète. |
| **Parcours complet de la Map** (`SELECT m`)    | ~2x plus lent        | ~2x plus lent        | ~2x plus lent        | ~2x plus lent        | Il faut lire et réassembler tous les buckets.                                                                                                                                                                                                |
| **INSERT**                                     | 1.5–2.5x plus lent   | 1.5–2.5x plus lent   | 1.5–2.5x plus lent   | 1.5–2.5x plus lent   | Surcoût lié au hachage des clés et à l&#39;écriture dans plusieurs sous-flux.                                                                                                                                                                |

<div id="recommendations">
  ### Recommandations
</div>

* **Petites maps (&lt; 32 clés en moyenne) :** Conservez la sérialisation `basic`. Le surcoût du découpage en buckets ne se justifie pas pour les petites maps. La valeur par défaut `map_buckets_min_avg_size = 32` gère cela automatiquement.
* **Maps de taille moyenne (32–100 clés) :** Utilisez `with_buckets` avec la stratégie `sqrt` si les requêtes accèdent fréquemment à des clés individuelles. Le gain de vitesse est de 4 à 8x pour les recherches sur une seule clé.
* **Grandes maps (100+ clés) :** Utilisez `with_buckets`. Les recherches sur une seule clé sont 16 à 49x plus rapides. Envisagez `map_serialization_version_for_zero_level_parts = 'basic'` pour conserver une vitesse d&#39;insertion proche de la référence.
* **Les parcours complets de maps dominent la charge de travail :** Conservez `basic`. La sérialisation par buckets ajoute un surcoût d&#39;environ 2x pour les parcours complets.
* **Charge de travail mixte (certaines recherches de clés, certains parcours complets) :** Utilisez `with_buckets` avec les parts de niveau zéro définies sur `basic`. L&#39;optimisation `PREWHERE` ne lit que le bucket pertinent pour le filtre, puis ne lit la map complète que pour les lignes correspondantes, ce qui apporte un gain net de performance significatif.

<div id="map-alternatives">
  ### Approches alternatives
</div>

Si la sérialisation `Map` compartimentée ne convient pas à votre cas d’utilisation, il existe deux autres approches pour améliorer les performances d’accès au niveau des clés :

<div id="using-the-json-data-type">
  #### Utilisation du type de données JSON
</div>

Le type de données [JSON](/fr/sql-reference/data-types/newjson) stocke chaque chemin fréquent dans une sous-colonne dynamique distincte. Les chemins qui dépassent la limite `max_dynamic_paths` sont stockés dans une [structure de données partagées](/fr/sql-reference/data-types/newjson#shared-data-structure), qui peut utiliser la sérialisation `advanced` pour optimiser la lecture d’un chemin unique. Consultez l’[article de blog](https://clickhouse.com/blog/json-data-type-gets-even-better) pour une présentation détaillée de la sérialisation `advanced`.

| Aspect                            | `Map` avec buckets                                                                                          | `JSON`                                                                                                                                                                                                                                |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lecture d’une seule clé           | Lit un bucket (qui peut contenir d’autres clés). Toutes les paires clé-valeur du bucket sont désérialisées. | Les chemins fréquents sont lus directement à partir des sous-colonnes dynamiques. Les chemins peu fréquents sont stockés dans les données partagées ; avec la sérialisation `advanced`, seules les données du chemin exact sont lues. |
| Types de valeurs                  | Toutes les valeurs partagent le même type `V`                                                               | Chaque chemin peut avoir son propre type. Les chemins sans indication de type utilisent `Dynamic`.                                                                                                                                    |
| Prise en charge des index de saut | Fonctionne avec certains types d’index créés sur `mapKeys`/`mapValues`                                      | Les index de saut ne peuvent être créés que sur des sous-colonnes de chemin spécifiques, pas sur tous les chemins/toutes les valeurs en une seule fois.                                                                               |
| Lecture de la colonne complète    | ~2x plus lente que `basic` en raison du réassemblage des buckets                                            | Surcoût lié à l’encodage du type `Dynamic` et à la reconstruction des chemins.                                                                                                                                                        |
| Surcoût de stockage               | Métadonnées supplémentaires minimales                                                                       | Plus élevé en raison de l’encodage du type `Dynamic`, du stockage des noms de chemin et des métadonnées supplémentaires dans la sérialisation `advanced`.                                                                             |
| Flexibilité du schéma             | Types de clé et de valeur fixés lors de la création de la table                                             | Entièrement dynamique — les clés et les types de valeur peuvent varier d’une ligne à l’autre. Des indications de type peuvent être déclarées pour les chemins connus.                                                                 |

Utilisez `JSON` lorsque différentes clés nécessitent différents types de valeur, lorsque l’ensemble des clés varie fortement d’une ligne à l’autre, ou lorsque les clés fréquemment consultées sont connues à l’avance et peuvent être déclarées comme chemins typés pour un accès direct aux sous-colonnes.

<div id="manual-sharding-into-multiple-map-columns">
  #### Partitionnement manuel en plusieurs colonnes `Map`
</div>

Vous pouvez répartir manuellement un seul `Map` dans plusieurs colonnes en fonction du hachage de la clé, au niveau de l’application :

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

Lors de l’insertion, acheminez chaque paire clé-valeur vers la colonne `m{hash(key) % 4}`. Lors des requêtes, lisez dans la colonne correspondante : `m{hash('target_key') % 4}['target_key']`.

| Aspect                  | `Map` avec buckets                                                      | Partitionnement manuel                                                                          |
| ----------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Facilité d’utilisation  | Transparent — géré par le moteur de stockage                            | Nécessite une logique de routage au niveau de l’application pour les insertions et les sélections |
| Vertical merge          | Non pris en charge — tous les buckets appartiennent à une seule colonne | Pris en charge — chaque colonne `Map` est indépendante et peut être fusionnée verticalement       |
| Modifications du schéma | Le nombre de buckets s’adapte automatiquement par part                  | Modifier le nombre de shards nécessite de réécrire les données ou d’ajouter de nouvelles colonnes |
| Syntaxe de requête      | `m['key']` fonctionne directement                                       | Il faut calculer la bonne colonne : `m0['key']`, `m1['key']`, etc.                                |
| Granularité des buckets | Par part, s’adapte aux statistiques des données                         | Fixée lors de la création de la table                                                             |

Le partitionnement manuel est utile lorsque les vertical merges sont importants pour réduire l’utilisation de la mémoire lors des merges de tables comportant de nombreuses colonnes, ou lorsque le nombre de shards doit être fixe et contrôlé explicitement. Pour la plupart des cas d’usage, la sérialisation automatique par buckets est plus simple et suffisante.

**Voir aussi**

* fonction [map()](/fr/sql-reference/functions/tuple-map-functions#map)
* fonction [CAST()](/fr/sql-reference/functions/type-conversion-functions#CAST)
* [combinateur -Map pour le type de données Map](../aggregate-functions/combinators.md#-map)

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Créer une solution d’observabilité avec ClickHouse - Partie 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)