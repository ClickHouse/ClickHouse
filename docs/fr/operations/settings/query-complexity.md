---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "Restrictions sur la complexit\xE9 des requ\xEAtes"
---

# Restrictions sur la complexité des requêtes {#restrictions-on-query-complexity}

Les Restrictions sur la complexité des requêtes font partie des paramètres.
Ils sont utilisés pour fournir une exécution plus sûre à partir de l'interface utilisateur.
Presque toutes les restrictions ne s'appliquent qu'à `SELECT`. Pour le traitement des requêtes distribuées, des restrictions sont appliquées sur chaque serveur séparément.

ClickHouse vérifie les restrictions pour les parties de données, pas pour chaque ligne. Cela signifie que vous pouvez dépasser la valeur de restriction de la taille de la partie données.

Restrictions sur l' “maximum amount of something” peut prendre la valeur 0, ce qui signifie “unrestricted”.
La plupart des restrictions ont également un ‘overflow\_mode’ paramètre signification que faire lorsque la limite est dépassée.
Il peut prendre deux valeurs: `throw` ou `break`. Les Restrictions sur l'agrégation (group\_by\_overflow\_mode) ont également la valeur `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don't add new keys to the set.

## max\_memory\_usage {#settings_max_memory_usage}

La quantité maximale de RAM à utiliser pour exécuter une requête sur un seul serveur.

Dans le fichier de configuration par défaut, le maximum est de 10 Go.

Le réglage ne tient pas compte du volume de mémoire disponible ou du volume total de mémoire sur la machine.
La restriction s'applique à une seule requête au sein d'un seul serveur.
Vous pouvez utiliser `SHOW PROCESSLIST` pour vérifier la consommation de mémoire pour chaque requête.
En outre, la consommation de mémoire maximale est suivie pour chaque requête et écrite dans le journal.

L'utilisation de la mémoire n'est pas surveillée pour les membres de certaines fonctions d'agrégation.

L'utilisation de la mémoire n'est pas totalement suivies pour les états des fonctions d'agrégation `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` de `String` et `Array` argument.

La consommation de mémoire est également limitée par les paramètres `max_memory_usage_for_user` et `max_memory_usage_for_all_queries`.

## max\_memory\_usage\_for\_user {#max-memory-usage-for-user}

Quantité maximale de RAM à utiliser pour exécuter les requêtes d'un utilisateur sur un seul serveur.

Les valeurs par défaut sont définies dans [Paramètre.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288). Par défaut, le montant n'est pas limité (`max_memory_usage_for_user = 0`).

Voir aussi la description de [max\_memory\_usage](#settings_max_memory_usage).

## max\_memory\_usage\_for\_all\_queries {#max-memory-usage-for-all-queries}

La quantité maximale de RAM à utiliser pour exécuter toutes les requêtes sur un seul serveur.

Les valeurs par défaut sont définies dans [Paramètre.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L289). Par défaut, le montant n'est pas limité (`max_memory_usage_for_all_queries = 0`).

Voir aussi la description de [max\_memory\_usage](#settings_max_memory_usage).

## max\_rows\_to\_read {#max-rows-to-read}

Les restrictions suivantes peut être vérifiée sur chaque bloc (au lieu de sur chaque ligne). Autrement dit, les restrictions peuvent être brisées un peu.
Lors de l'exécution d'une requête dans plusieurs threads, les restrictions suivantes s'appliquent à chaque thread séparément.

Un nombre maximum de lignes pouvant être lues à partir d'un tableau lors de l'exécution d'une requête.

## max\_bytes\_to\_read {#max-bytes-to-read}

Nombre maximal d'octets (données non compressées) pouvant être lus à partir d'une table lors de l'exécution d'une requête.

## read\_overflow\_mode {#read-overflow-mode}

Que faire lorsque le volume de lecture de données dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

## max\_rows\_to\_group\_by {#settings-max-rows-to-group-by}

Un nombre maximum de clés uniques reçues de l'agrégation. Ce paramètre permet de limiter la consommation de mémoire lors de l'agrégation.

## group\_by\_overflow\_mode {#group-by-overflow-mode}

Que faire lorsque le nombre de clés uniques pour l'agrégation dépasse la limite: ‘throw’, ‘break’, ou ‘any’. Par défaut, les jeter.
À l'aide de la ‘any’ valeur vous permet d'exécuter une approximation de GROUP BY. La qualité de cette approximation dépend de la nature statistique des données.

## max\_bytes\_before\_external\_group\_by {#settings-max_bytes_before_external_group_by}

Active ou désactive l'exécution de `GROUP BY` clauses dans la mémoire externe. Voir [Groupe par dans la mémoire externe](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory).

Valeurs possibles:

-   Volume maximal de RAM (en octets) pouvant être utilisé par le [GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause) opération.
-   0 — `GROUP BY` dans la mémoire externe désactivé.

Valeur par défaut: 0.

## max\_rows\_to\_sort {#max-rows-to-sort}

Un nombre maximum de lignes avant le tri. Cela vous permet de limiter la consommation de mémoire lors du tri.

## max\_bytes\_to\_sort {#max-bytes-to-sort}

Un nombre maximal d'octets avant le tri.

## sort\_overflow\_mode {#sort-overflow-mode}

Que faire si le nombre de lignes reçues avant le tri dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

## max\_result\_rows {#setting-max_result_rows}

Limite sur le nombre de lignes dans le résultat. Également vérifié pour les sous-requêtes, et sur des serveurs distants lors de l'exécution de parties d'une requête distribuée.

## max\_result\_bytes {#max-result-bytes}

Limite sur le nombre d'octets dans le résultat. Le même que le réglage précédent.

## result\_overflow\_mode {#result-overflow-mode}

Que faire si le volume du résultat dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

Utiliser ‘break’ est similaire à L'utilisation de LIMIT. `Break` interrompt l'exécution seulement au niveau du bloc. Cela signifie que la quantité de lignes renvoyées est supérieure à [max\_result\_rows](#setting-max_result_rows) multiples de [max\_block\_size](settings.md#setting-max_block_size) et dépend de l' [max\_threads](settings.md#settings-max_threads).

Exemple:

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

Résultat:

``` text
6666 rows in set. ...
```

## max\_execution\_time {#max-execution-time}

Durée maximale d'exécution de la requête en secondes.
Pour le moment, il n'est pas vérifié pour l'une des étapes de tri, ni lors de la fusion et de la finalisation des fonctions d'agrégat.

## timeout\_overflow\_mode {#timeout-overflow-mode}

Que faire si la requête est exécutée plus de ‘max\_execution\_time’: ‘throw’ ou ‘break’. Par défaut, les jeter.

## min\_execution\_speed {#min-execution-speed}

Vitesse d'exécution minimale en lignes par seconde. Vérifié sur chaque bloc de données quand ‘timeout\_before\_checking\_execution\_speed’ expirer. Si la vitesse d'exécution est inférieure, une exception est levée.

## min\_execution\_speed\_bytes {#min-execution-speed-bytes}

Un nombre minimum d'exécution d'octets par seconde. Vérifié sur chaque bloc de données quand ‘timeout\_before\_checking\_execution\_speed’ expirer. Si la vitesse d'exécution est inférieure, une exception est levée.

## max\_execution\_speed {#max-execution-speed}

Un nombre maximal d'exécution de lignes par seconde. Vérifié sur chaque bloc de données quand ‘timeout\_before\_checking\_execution\_speed’ expirer. Si la vitesse d'exécution est élevée, la vitesse d'exécution sera réduit.

## max\_execution\_speed\_bytes {#max-execution-speed-bytes}

Un nombre maximal d'exécution d'octets par seconde. Vérifié sur chaque bloc de données quand ‘timeout\_before\_checking\_execution\_speed’ expirer. Si la vitesse d'exécution est élevée, la vitesse d'exécution sera réduit.

## timeout\_before\_checking\_execution\_speed {#timeout-before-checking-execution-speed}

Vérifie que la vitesse d'exécution n'est pas trop lent (pas moins de ‘min\_execution\_speed’), après l'expiration du temps spécifié en secondes.

## max\_columns\_to\_read {#max-columns-to-read}

Nombre maximal de colonnes pouvant être lues à partir d'une table dans une seule requête. Si une requête nécessite la lecture d'un plus grand nombre de colonnes, il lève une exception.

## max\_temporary\_columns {#max-temporary-columns}

Nombre maximal de colonnes temporaires qui doivent être conservées en RAM en même temps lors de l'exécution d'une requête, y compris les colonnes constantes. S'il y a plus de colonnes temporaires que cela, il lève une exception.

## max\_temporary\_non\_const\_columns {#max-temporary-non-const-columns}

La même chose que ‘max\_temporary\_columns’ mais sans compter constante colonnes.
Notez que les colonnes constantes sont formées assez souvent lors de l'exécution d'une requête, mais elles nécessitent environ zéro ressource informatique.

## max\_subquery\_depth {#max-subquery-depth}

Profondeur maximale de sous-requêtes. Si les sous-requêtes sont plus profondes, une exception est levée. Par défaut, 100.

## max\_pipeline\_depth {#max-pipeline-depth}

Profondeur maximale du pipeline. Correspond au nombre de transformations que chaque bloc de données lors du traitement des requêtes. Compté dans les limites d'un seul serveur. Si la profondeur du pipeline est supérieure, une exception est levée. Par défaut, 1000.

## max\_ast\_depth {#max-ast-depth}

Profondeur maximale d'une requête arbre syntaxique. En cas de dépassement, une exception est levée.
À ce moment, il n'est pas vérifié pendant l'analyse, mais seulement après l'analyse de la requête. Autrement dit, un arbre syntaxique trop profond peut être créé pendant l'analyse, mais la requête échouera. Par défaut, 1000.

## max\_ast\_elements {#max-ast-elements}

Un nombre maximal d'éléments dans une requête arbre syntaxique. En cas de dépassement, une exception est levée.
De la même manière que le paramètre précédent, il est vérifié qu'après l'analyse de la requête. Par défaut, 50 000.

## max\_rows\_in\_set {#max-rows-in-set}

Nombre maximal de lignes pour un ensemble de données dans la clause in créée à partir d'une sous-requête.

## max\_bytes\_in\_set {#max-bytes-in-set}

Nombre maximal d'octets (données non compressées) utilisés par un ensemble de la clause in créé à partir d'une sous-requête.

## set\_overflow\_mode {#set-overflow-mode}

Que faire lorsque la quantité de données dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

## max\_rows\_in\_distinct {#max-rows-in-distinct}

Un nombre maximum de lignes différentes lors de L'utilisation de DISTINCT.

## max\_bytes\_in\_distinct {#max-bytes-in-distinct}

Nombre maximal d'octets utilisés par une table de hachage lors de L'utilisation de DISTINCT.

## distinct\_overflow\_mode {#distinct-overflow-mode}

Que faire lorsque la quantité de données dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

## max\_rows\_to\_transfer {#max-rows-to-transfer}

Nombre maximal de lignes pouvant être transmises à un serveur distant ou enregistrées dans une table temporaire lors de L'utilisation de GLOBAL IN.

## max\_bytes\_to\_transfer {#max-bytes-to-transfer}

Nombre maximal d'octets (données non compressées) pouvant être transmis à un serveur distant ou enregistrés dans une table temporaire lors de L'utilisation de GLOBAL IN.

## transfer\_overflow\_mode {#transfer-overflow-mode}

Que faire lorsque la quantité de données dépasse l'une des limites: ‘throw’ ou ‘break’. Par défaut, les jeter.

## max\_rows\_in\_join {#settings-max_rows_in_join}

Limite le nombre de lignes dans la table de hachage utilisée lors de la jonction de tables.

Ce réglage s'applique à [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) les opérations et les [Rejoindre](../../engines/table-engines/special/join.md) tableau moteur.

Si une requête contient plusieurs jointures, ClickHouse vérifie ce paramètre pour chaque résultat intermédiaire.

ClickHouse peut procéder à différentes actions lorsque la limite est atteinte. L'utilisation de la [join\_overflow\_mode](#settings-join_overflow_mode) réglage pour choisir l'action.

Valeurs possibles:

-   Entier positif.
-   0 — Unlimited number of rows.

Valeur par défaut: 0.

## max\_bytes\_in\_join {#settings-max_bytes_in_join}

Limite la taille en octets de la table de hachage utilisée lors de l'assemblage de tables.

Ce réglage s'applique à [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) les opérations et les [Rejoindre le moteur de table](../../engines/table-engines/special/join.md).

Si la requête contient des jointures, ClickHouse vérifie ce paramètre pour chaque résultat intermédiaire.

ClickHouse peut procéder à différentes actions lorsque la limite est atteinte. Utiliser [join\_overflow\_mode](#settings-join_overflow_mode) paramètres pour choisir l'action.

Valeurs possibles:

-   Entier positif.
-   0 — Memory control is disabled.

Valeur par défaut: 0.

## join\_overflow\_mode {#settings-join_overflow_mode}

Définit l'action que ClickHouse effectue lorsque l'une des limites de jointure suivantes est atteinte:

-   [max\_bytes\_in\_join](#settings-max_bytes_in_join)
-   [max\_rows\_in\_join](#settings-max_rows_in_join)

Valeurs possibles:

-   `THROW` — ClickHouse throws an exception and breaks operation.
-   `BREAK` — ClickHouse breaks operation and doesn't throw an exception.

Valeur par défaut: `THROW`.

**Voir Aussi**

-   [Clause de JOINTURE](../../sql-reference/statements/select/join.md#select-join)
-   [Rejoindre le moteur de table](../../engines/table-engines/special/join.md)

## max\_partitions\_per\_insert\_block {#max-partitions-per-insert-block}

Limite le nombre maximal de partitions dans un seul bloc inséré.

-   Entier positif.
-   0 — Unlimited number of partitions.

Valeur par défaut: 100.

**Détail**

Lors de l'insertion de données, ClickHouse calcule le nombre de partitions dans le bloc inséré. Si le nombre de partitions est plus que `max_partitions_per_insert_block`, ClickHouse lève une exception avec le texte suivant:

> “Too many partitions for single INSERT block (more than” + toString (max\_parts) + “). The limit is controlled by ‘max\_partitions\_per\_insert\_block’ setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).”

[Article Original](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->
