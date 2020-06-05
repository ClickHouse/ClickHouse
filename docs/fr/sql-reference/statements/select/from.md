---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# De la Clause {#select-from}

Le `FROM` clause spécifie la source à partir de laquelle lire les données:

-   [Table](../../../engines/table-engines/index.md)
-   [Sous-requête](index.md) {##TODO: meilleur lien ##}
-   [Fonction de Table](../../table-functions/index.md#table-functions)

[JOIN](join.md) et [ARRAY JOIN](array-join.md) les clauses peuvent également être utilisées pour étendre la fonctionnalité de la `FROM` clause.

Subquery est un autre `SELECT` requête qui peut être spécifié entre parenthèses à l'intérieur `FROM` clause.

`FROM` la clause peut contenir plusieurs sources de données, séparées par des virgules, ce qui équivaut à effectuer [CROSS JOIN](join.md) sur eux.

## Modificateur FINAL {#select-from-final}

Lorsque `FINAL` est spécifié, ClickHouse fusionne complètement les données avant de renvoyer le résultat et effectue ainsi toutes les transformations de données qui se produisent lors des fusions pour le moteur de table donné.

Il est applicable lors de la sélection de données à partir de tables qui utilisent [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-la famille de moteurs (à l'exception de `GraphiteMergeTree`). Également pris en charge pour:

-   [Répliqué](../../../engines/table-engines/mergetree-family/replication.md) les versions de `MergeTree` moteur.
-   [Vue](../../../engines/table-engines/special/view.md), [Tampon](../../../engines/table-engines/special/buffer.md), [Distribué](../../../engines/table-engines/special/distributed.md), et [MaterializedView](../../../engines/table-engines/special/materializedview.md) moteurs qui fonctionnent sur d'autres moteurs, à condition qu'ils aient été créés sur `MergeTree`-tables de moteur.

### Inconvénient {#drawbacks}

Requêtes qui utilisent `FINAL` sont exécutés pas aussi vite que les requêtes similaires qui ne le font pas, car:

-   La requête est exécutée dans un seul thread et les données sont fusionnées lors de l'exécution de la requête.
-   Les requêtes avec `FINAL` lire les colonnes de clé primaire en plus des colonnes spécifiées dans la requête.

**Dans la plupart des cas, évitez d'utiliser `FINAL`.** L'approche commune consiste à utiliser différentes requêtes qui supposent les processus d'arrière-plan du `MergeTree` le moteur n'est pas encore arrivé et y faire face en appliquant l'agrégation (par exemple, pour éliminer les doublons). {##TODO: exemples ##}

## Détails De Mise En Œuvre {#implementation-details}

Si l' `FROM` la clause est omise, les données seront lues à partir `system.one` table.
Le `system.one` table contient exactement une ligne (cette table remplit le même but que la table double trouvée dans d'autres SGBD).

Pour exécuter une requête, toutes les colonnes mentionnées dans la requête sont extraites de la table appropriée. Toutes les colonnes non nécessaires pour la requête externe sont rejetées des sous-requêtes.
Si une requête ne répertorie aucune colonne (par exemple, `SELECT count() FROM t`), une colonne est extraite de la table de toute façon (la plus petite est préférée), afin de calculer le nombre de lignes.
