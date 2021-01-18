---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Paramètre {#settings}

## distributed\_product\_mode {#distributed-product-mode}

Modifie le comportement de [distribués sous-requêtes](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restriction:

-   Uniquement appliqué pour les sous-requêtes IN et JOIN.
-   Uniquement si la section FROM utilise une table distribuée contenant plus d'un fragment.
-   Si la sous-requête concerne un distribué tableau contenant plus d'un fragment.
-   Pas utilisé pour une table [distant](../../sql-reference/table-functions/remote.md) fonction.

Valeurs possibles:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` requête avec `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable\_optimize\_predicate\_expression {#enable-optimize-predicate-expression}

Active la poussée du prédicat `SELECT` requête.

Prédicat pushdown peut réduire considérablement le trafic réseau pour les requêtes distribuées.

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 1.

Utilisation

Considérez les requêtes suivantes:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

Si `enable_optimize_predicate_expression = 1`, alors le temps d'exécution de ces requêtes est égal car ClickHouse s'applique `WHERE` à la sous-requête lors du traitement.

Si `enable_optimize_predicate_expression = 0` puis le temps d'exécution de la deuxième requête est beaucoup plus long, parce que le `WHERE` la clause s'applique à toutes les données après la sous-requête des finitions.

## fallback\_to\_stale\_replicas\_for\_distributed\_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Force une requête à un réplica obsolète si les données mises à jour ne sont pas disponibles. Voir [Réplication](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse sélectionne le plus pertinent parmi les répliques obsolètes de la table.

Utilisé lors de l'exécution `SELECT` à partir d'une table distribuée qui pointe vers des tables répliquées.

Par défaut, 1 (activé).

## force\_index\_by\_date {#settings-force_index_by_date}

Désactive l'exécution de la requête si l'index ne peut pas être utilisé par jour.

Fonctionne avec les tables de la famille MergeTree.

Si `force_index_by_date=1`, Clickhouse vérifie si la requête a une condition de clé de date qui peut être utilisée pour restreindre les plages de données. S'il n'y a pas de condition appropriée, il lève une exception. Cependant, il ne vérifie pas si la condition réduit la quantité de données à lire. Par exemple, la condition `Date != ' 2000-01-01 '` est acceptable même lorsqu'il correspond à toutes les données de la table (c'est-à-dire que l'exécution de la requête nécessite une analyse complète). Pour plus d'informations sur les plages de données dans les tables MergeTree, voir [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force\_primary\_key {#force-primary-key}

Désactive l'exécution de la requête si l'indexation par la clé primaire n'est pas possible.

Fonctionne avec les tables de la famille MergeTree.

Si `force_primary_key=1`, Clickhouse vérifie si la requête a une condition de clé primaire qui peut être utilisée pour restreindre les plages de données. S'il n'y a pas de condition appropriée, il lève une exception. Cependant, il ne vérifie pas si la condition réduit la quantité de données à lire. Pour plus d'informations sur les plages de données dans les tables MergeTree, voir [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## format\_schema {#format-schema}

Ce paramètre est utile lorsque vous utilisez des formats nécessitant une définition de schéma, tels que [Cap'n Proto](https://capnproto.org/) ou [Protobuf](https://developers.google.com/protocol-buffers/). La valeur dépend du format.

## fsync\_metadata {#fsync-metadata}

Active ou désactive [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) lors de l'écriture `.sql` fichier. Activé par défaut.

Il est logique de le désactiver si le serveur a des millions de tables minuscules qui sont constamment créées et détruites.

## enable\_http\_compression {#settings-enable_http_compression}

Active ou désactive la compression de données dans la réponse à une requête HTTP.

Pour plus d'informations, lire l' [Description de L'interface HTTP](../../interfaces/http.md).

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

## http\_zlib\_compression\_level {#settings-http_zlib_compression_level}

Définit le niveau de compression des données dans la réponse à une requête HTTP si [enable\_http\_compression = 1](#settings-enable_http_compression).

Valeurs possibles: nombres de 1 à 9.

Valeur par défaut: 3.

## http\_native\_compression\_disable\_checksumming\_on\_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

Active ou désactive la vérification de la somme de contrôle lors de la décompression des données HTTP POST du client. Utilisé uniquement pour le format de compression natif ClickHouse (non utilisé avec `gzip` ou `deflate`).

Pour plus d'informations, lire l' [Description de L'interface HTTP](../../interfaces/http.md).

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

## send\_progress\_in\_http\_headers {#settings-send_progress_in_http_headers}

Active ou désactive `X-ClickHouse-Progress` - Têtes de réponse HTTP dans `clickhouse-server` réponse.

Pour plus d'informations, lire l' [Description de L'interface HTTP](../../interfaces/http.md).

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

## max\_http\_get\_redirects {#setting-max_http_get_redirects}

Limite le nombre maximal de sauts de redirection HTTP GET pour [URL](../../engines/table-engines/special/url.md)-tables de moteur. Le paramètre s'applique aux deux types de tables: celles créées par [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) requête et par la [URL](../../sql-reference/table-functions/url.md) table de fonction.

Valeurs possibles:

-   Tout nombre entier positif de houblon.
-   0 — No hops allowed.

Valeur par défaut: 0.

## input\_format\_allow\_errors\_num {#settings-input_format_allow_errors_num}

Définit le nombre maximal d'erreurs acceptables lors de la lecture à partir de formats de texte (CSV, TSV, etc.).

La valeur par défaut est de 0.

Toujours le coupler avec `input_format_allow_errors_ratio`.

Si une erreur s'est produite lors de la lecture de lignes mais que le compteur d'erreurs est toujours inférieur à `input_format_allow_errors_num`, ClickHouse ignore la ligne et passe à la suivante.

Si les deux `input_format_allow_errors_num` et `input_format_allow_errors_ratio` sont dépassés, ClickHouse lève une exception.

## input\_format\_allow\_errors\_ratio {#settings-input_format_allow_errors_ratio}

Définit le pourcentage maximal d'erreurs autorisées lors de la lecture à partir de formats de texte (CSV, TSV, etc.).
Le pourcentage d'erreurs est défini comme un nombre à virgule flottante compris entre 0 et 1.

La valeur par défaut est de 0.

Toujours le coupler avec `input_format_allow_errors_num`.

Si une erreur s'est produite lors de la lecture de lignes mais que le compteur d'erreurs est toujours inférieur à `input_format_allow_errors_ratio`, ClickHouse ignore la ligne et passe à la suivante.

Si les deux `input_format_allow_errors_num` et `input_format_allow_errors_ratio` sont dépassés, ClickHouse lève une exception.

## input\_format\_values\_interpret\_expressions {#settings-input_format_values_interpret_expressions}

Active ou désactive L'analyseur SQL complet si l'analyseur de flux rapide ne peut pas analyser les données. Ce paramètre est utilisé uniquement pour la [Valeur](../../interfaces/formats.md#data-format-values) format lors de l'insertion des données. Pour plus d'informations sur l'analyse syntaxique, consultez [Syntaxe](../../sql-reference/syntax.md) section.

Valeurs possibles:

-   0 — Disabled.

    Dans ce cas, vous devez fournir des données formatées. Voir la [Format](../../interfaces/formats.md) section.

-   1 — Enabled.

    Dans ce cas, vous pouvez utiliser une expression SQL en tant que valeur, mais l'insertion de données est beaucoup plus lente de cette façon. Si vous insérez uniquement des données formatées, ClickHouse se comporte comme si la valeur de réglage était 0.

Valeur par défaut: 1.

Exemple D'utilisation

Insérez le [DateTime](../../sql-reference/data-types/datetime.md) tapez valeur avec les différents paramètres.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

La dernière requête est équivalente à la suivante:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input\_format\_values\_deduce\_templates\_of\_expressions {#settings-input_format_values_deduce_templates_of_expressions}

Active ou désactive la déduction de modèle pour les expressions SQL dans [Valeur](../../interfaces/formats.md#data-format-values) format. Il permet d'analyser et d'interpréter des expressions dans `Values` beaucoup plus rapide si les expressions dans des lignes consécutives ont la même structure. ClickHouse tente de déduire le modèle d'une expression, d'analyser les lignes suivantes à l'aide de ce modèle et d'évaluer l'expression sur un lot de lignes analysées avec succès.

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 1.

Pour la requête suivante:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   Si `input_format_values_interpret_expressions=1` et `format_values_deduce_templates_of_expressions=0`, les expressions sont interprétées séparément pour chaque ligne (c'est très lent pour un grand nombre de lignes).
-   Si `input_format_values_interpret_expressions=0` et `format_values_deduce_templates_of_expressions=1`, les expressions des première, deuxième et troisième lignes sont analysées à l'aide de template `lower(String)` et interprété ensemble, l'expression dans la quatrième ligne est analysée avec un autre modèle (`upper(String)`).
-   Si `input_format_values_interpret_expressions=1` et `format_values_deduce_templates_of_expressions=1`, le même que dans le cas précédent, mais permet également d'interpréter les expressions séparément s'il n'est pas possible de déduire le modèle.

## input\_format\_values\_accurate\_types\_of\_literals {#settings-input-format-values-accurate-types-of-literals}

Ce paramètre est utilisé uniquement lorsque `input_format_values_deduce_templates_of_expressions = 1`. Il peut arriver que les expressions pour une colonne aient la même structure, mais contiennent des littéraux numériques de types différents, par exemple

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Valeurs possibles:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` ou `Int64` plutôt `UInt64` pour `42`), mais cela peut causer des problèmes de débordement et de précision.

-   1 — Enabled.

    Dans ce cas, ClickHouse vérifie le type réel de littéral et utilise un modèle d'expression du type correspondant. Dans certains cas, cela peut considérablement ralentir l'évaluation de l'expression dans `Values`.

Valeur par défaut: 1.

## input\_format\_defaults\_for\_omitted\_fields {#session_settings-input_format_defaults_for_omitted_fields}

Lors de l'exécution de `INSERT` requêtes, remplacez les valeurs de colonne d'entrée omises par les valeurs par défaut des colonnes respectives. Cette option s'applique uniquement aux [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) et [TabSeparated](../../interfaces/formats.md#tabseparated) format.

!!! note "Note"
    Lorsque cette option est activée, les métadonnées de table étendues sont envoyées du serveur au client. Il consomme des ressources informatiques supplémentaires sur le serveur et peut réduire les performances.

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 1.

## input\_format\_tsv\_empty\_as\_default {#settings-input-format-tsv-empty-as-default}

Lorsque cette option est activée, remplacez les champs de saisie vides dans TSV par des valeurs par défaut. Pour les expressions par défaut complexes `input_format_defaults_for_omitted_fields` doit être activé en trop.

Désactivé par défaut.

## input\_format\_null\_as\_default {#settings-input-format-null-as-default}

Active ou désactive l'utilisation des valeurs par défaut si les données `NULL` mais le type de données de la colonne correspondante dans pas `Nullable(T)` (pour les formats de saisie de texte).

## input\_format\_skip\_unknown\_fields {#settings-input-format-skip-unknown-fields}

Active ou désactive le saut d'insertion de données supplémentaires.

Lors de l'écriture de données, ClickHouse lève une exception si les données d'entrée contiennent des colonnes qui n'existent pas dans la table cible. Si le saut est activé, ClickHouse n'insère pas de données supplémentaires et ne lance pas d'exception.

Formats pris en charge:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

## input\_format\_import\_nested\_json {#settings-input_format_import_nested_json}

Active ou désactive l'insertion de données JSON avec des objets imbriqués.

Formats pris en charge:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

Voir aussi:

-   [Utilisation de Structures imbriquées](../../interfaces/formats.md#jsoneachrow-nested) avec l' `JSONEachRow` format.

## input\_format\_with\_names\_use\_header {#settings-input-format-with-names-use-header}

Active ou désactive la vérification de l'ordre des colonnes lors de l'insertion de données.

Pour améliorer les performances d'insertion, nous vous recommandons de désactiver cette vérification si vous êtes sûr que l'ordre des colonnes des données d'entrée est le même que dans la table cible.

Formats pris en charge:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 1.

## date\_time\_input\_format {#settings-date_time_input_format}

Permet de choisir un analyseur de la représentation textuelle de la date et de l'heure.

Le réglage ne s'applique pas à [fonctions date et heure](../../sql-reference/functions/date-time-functions.md).

Valeurs possibles:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse peut analyser la base `YYYY-MM-DD HH:MM:SS` format et tous [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) formats de date et heure. Exemple, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse ne peut analyser que la base `YYYY-MM-DD HH:MM:SS` format. Exemple, `'2019-08-20 10:18:56'`.

Valeur par défaut: `'basic'`.

Voir aussi:

-   [Type de données DateTime.](../../sql-reference/data-types/datetime.md)
-   [Fonctions pour travailler avec des dates et des heures.](../../sql-reference/functions/date-time-functions.md)

## join\_default\_strictness {#settings-join_default_strictness}

Définit la rigueur par défaut pour [JOIN clauses](../../sql-reference/statements/select/join.md#select-join).

Valeurs possibles:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Produit cartésien](https://en.wikipedia.org/wiki/Cartesian_product) à partir des lignes correspondantes. C'est normal `JOIN` comportement de SQL standard.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` et `ALL` sont les mêmes.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` ou `ANY` n'est pas spécifié dans la requête, ClickHouse lève une exception.

Valeur par défaut: `ALL`.

## join\_any\_take\_last\_row {#settings-join_any_take_last_row}

Modifie le comportement des opérations de jointure avec `ANY` rigueur.

!!! warning "Attention"
    Ce paramètre s'applique uniquement pour `JOIN` opérations avec [Rejoindre](../../engines/table-engines/special/join.md) le moteur de tables.

Valeurs possibles:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

Valeur par défaut: 0.

Voir aussi:

-   [Clause de JOINTURE](../../sql-reference/statements/select/join.md#select-join)
-   [Rejoindre le moteur de table](../../engines/table-engines/special/join.md)
-   [join\_default\_strictness](#settings-join_default_strictness)

## join\_use\_nulls {#join_use_nulls}

Définit le type de [JOIN](../../sql-reference/statements/select/join.md) comportement. Lors de la fusion de tables, des cellules vides peuvent apparaître. ClickHouse les remplit différemment en fonction de ce paramètre.

Valeurs possibles:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` se comporte de la même manière que dans SQL standard. Le type du champ correspondant est converti en [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) et les cellules vides sont remplis avec [NULL](../../sql-reference/syntax.md).

Valeur par défaut: 0.

## max\_block\_size {#setting-max_block_size}

Dans ClickHouse, les données sont traitées par Blocs (Ensembles de parties de colonne). Les cycles de traitement internes pour un seul bloc sont assez efficaces, mais il y a des dépenses notables sur chaque bloc. Le `max_block_size` le paramètre est une recommandation pour la taille du bloc (dans un nombre de lignes) à charger à partir des tables. La taille du bloc ne doit pas être trop petite, de sorte que les dépenses sur chaque bloc sont toujours perceptibles, mais pas trop grande pour que la requête avec limite qui est terminée après le premier bloc soit traitée rapidement. L'objectif est d'éviter de consommer trop de mémoire lors de l'extraction d'un grand nombre de colonnes dans plusieurs threads et de préserver au moins certains localité de cache.

Valeur par défaut: de 65 536.

Les blocs de la taille de `max_block_size` ne sont pas toujours chargées de la table. Si il est évident que moins de données doivent être récupérées, un bloc plus petit est traitée.

## preferred\_block\_size\_bytes {#preferred-block-size-bytes}

Utilisé dans le même but que `max_block_size`, mais il définit la taille de bloc recommandée en octets en l'adaptant au nombre de lignes dans le bloc.
Cependant, la taille du bloc ne peut pas être supérieure à `max_block_size` rangée.
Par défaut: 1 000 000. Cela ne fonctionne que lors de la lecture des moteurs MergeTree.

## merge\_tree\_min\_rows\_for\_concurrent\_read {#setting-merge-tree-min-rows-for-concurrent-read}

Si le nombre de lignes à lire à partir d'un fichier d'un [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table dépasse `merge_tree_min_rows_for_concurrent_read` ensuite, ClickHouse essaie d'effectuer une lecture simultanée de ce fichier sur plusieurs threads.

Valeurs possibles:

-   Tout nombre entier positif.

Valeur par défaut: 163840.

## merge\_tree\_min\_bytes\_for\_concurrent\_read {#setting-merge-tree-min-bytes-for-concurrent-read}

Si le nombre d'octets à lire à partir d'un fichier d'un [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)- table de moteur dépasse `merge_tree_min_bytes_for_concurrent_read` puis ClickHouse essaie de lire simultanément à partir de ce fichier dans plusieurs threads.

Valeur Possible:

-   Tout nombre entier positif.

Valeur par défaut: 251658240.

## merge\_tree\_min\_rows\_for\_seek {#setting-merge-tree-min-rows-for-seek}

Si la distance entre deux blocs de données à lire dans un fichier est inférieure à `merge_tree_min_rows_for_seek` lignes, puis ClickHouse ne cherche pas à travers le fichier mais lit les données séquentiellement.

Valeurs possibles:

-   Tout nombre entier positif.

Valeur par défaut: 0.

## merge\_tree\_min\_bytes\_for\_seek {#setting-merge-tree-min-bytes-for-seek}

Si la distance entre deux blocs de données à lire dans un fichier est inférieure à `merge_tree_min_bytes_for_seek` octets, puis ClickHouse lit séquentiellement une plage de fichier qui contient les deux blocs, évitant ainsi la recherche supplémentaire.

Valeurs possibles:

-   Tout nombre entier positif.

Valeur par défaut: 0.

## merge\_tree\_coarse\_index\_granularité {#setting-merge-tree-coarse-index-granularity}

Lors de la recherche de données, ClickHouse vérifie les marques de données dans le fichier d'index. Si ClickHouse trouve que les clés requises sont dans une certaine plage, il divise cette plage en `merge_tree_coarse_index_granularity` subranges et recherche les clés requises récursivement.

Valeurs possibles:

-   Tout entier pair positif.

Valeur par défaut: 8.

## merge\_tree\_max\_rows\_to\_use\_cache {#setting-merge-tree-max-rows-to-use-cache}

Si ClickHouse devrait lire plus de `merge_tree_max_rows_to_use_cache` lignes dans une requête, il n'utilise pas le cache des blocs non compressés.

Le cache des blocs non compressés stocke les données extraites pour les requêtes. ClickHouse utilise ce cache pour accélérer les réponses aux petites requêtes répétées. Ce paramètre protège le cache contre le saccage par les requêtes qui lisent une grande quantité de données. Le [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) le paramètre serveur définit la taille du cache des blocs non compressés.

Valeurs possibles:

-   Tout nombre entier positif.

Default value: 128 ✕ 8192.

## merge\_tree\_max\_bytes\_to\_use\_cache {#setting-merge-tree-max-bytes-to-use-cache}

Si ClickHouse devrait lire plus de `merge_tree_max_bytes_to_use_cache` octets dans une requête, il n'utilise pas le cache de non compressé blocs.

Le cache des blocs non compressés stocke les données extraites pour les requêtes. ClickHouse utilise ce cache pour accélérer les réponses aux petites requêtes répétées. Ce paramètre protège le cache contre le saccage par les requêtes qui lisent une grande quantité de données. Le [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) le paramètre serveur définit la taille du cache des blocs non compressés.

Valeur Possible:

-   Tout nombre entier positif.

Valeur par défaut: 2013265920.

## min\_bytes\_to\_use\_direct\_io {#settings-min-bytes-to-use-direct-io}

Volume de données minimum requis pour utiliser l'accès direct aux E/S sur le disque de stockage.

ClickHouse utilise ce paramètre lors de la lecture de données à partir de tables. Si le volume total de stockage de toutes les données à lire dépasse `min_bytes_to_use_direct_io` octets, puis ClickHouse lit les données du disque de stockage avec le `O_DIRECT` option.

Valeurs possibles:

-   0 — Direct I/O is disabled.
-   Entier positif.

Valeur par défaut: 0.

## log\_queries {#settings-log-queries}

Configuration de la journalisation des requêtes.

Les requêtes envoyées à ClickHouse avec cette configuration sont enregistrées selon les règles du [query\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) paramètre de configuration du serveur.

Exemple:

``` text
log_queries=1
```

## log\_queries\_min\_type {#settings-log-queries-min-type}

`query_log` type minimal à enregistrer.

Valeurs possibles:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Valeur par défaut: `QUERY_START`.

Peut être utilisé pour limiter le nombre de entiries va va `query_log`, dites que vous êtes intéressant que dans les erreurs, alors vous pouvez utiliser `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log\_query\_threads {#settings-log-query-threads}

Configuration de la journalisation des threads de requête.

Les threads de requêtes exécutés par ClickHouse avec cette configuration sont journalisés selon les règles du [query\_thread\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) paramètre de configuration du serveur.

Exemple:

``` text
log_query_threads=1
```

## max\_insert\_block\_size {#settings-max_insert_block_size}

La taille des blocs à former pour l'insertion dans une table.
Ce paramètre s'applique uniquement dans les cas où le serveur formes les blocs.
Par exemple, pour une insertion via L'interface HTTP, le serveur analyse le format de données et forme des blocs de la taille spécifiée.
Mais lors de l'utilisation de clickhouse-client, le client analyse les données ‘max\_insert\_block\_size’ le réglage sur le serveur n'affecte pas la taille des blocs insérés.
Le paramètre n'a pas non plus de but lors de L'utilisation D'INSERT SELECT, car les données sont insérées à l'aide des mêmes blocs qui sont formés après SELECT.

Valeur par défaut: 1 048 576 octets.

La valeur par défaut est légèrement supérieure à `max_block_size`. La raison en est que certains moteurs de table (`*MergeTree`) former une partie de données sur le disque pour chaque bloc inséré, qui est une entité assez grande. Pareillement, `*MergeTree` les tables trient les données lors de l'insertion et une taille de bloc suffisamment grande permet de trier plus de données dans la RAM.

## min\_insert\_block\_size\_rows {#min-insert-block-size-rows}

Définit le nombre minimum de lignes dans le bloc qui peut être inséré dans un tableau par un `INSERT` requête. Les blocs de plus petite taille sont écrasés en plus gros.

Valeurs possibles:

-   Entier positif.
-   0 — Squashing disabled.

Valeur par défaut: 1048576.

## min\_insert\_block\_size\_bytes {#min-insert-block-size-bytes}

Définit le nombre minimal d'octets dans le bloc qui peut être inséré dans un tableau par un `INSERT` requête. Les blocs de plus petite taille sont écrasés en plus gros.

Valeurs possibles:

-   Entier positif.
-   0 — Squashing disabled.

Valeur par défaut: 268435456.

## max\_replica\_delay\_for\_distributed\_queries {#settings-max_replica_delay_for_distributed_queries}

Désactive les répliques en retard pour les requêtes distribuées. Voir [Réplication](../../engines/table-engines/mergetree-family/replication.md).

Définit le temps en secondes. Si une réplique accuse plus de retard que la valeur définie, cette réplique n'est pas utilisée.

Valeur par défaut: 300.

Utilisé lors de l'exécution `SELECT` à partir d'une table distribuée qui pointe vers des tables répliquées.

## max\_threads {#settings-max_threads}

Nombre maximal de threads de traitement des requêtes, à l'exclusion des threads de récupération de données à partir de serveurs distants (voir ‘max\_distributed\_connections’ paramètre).

Ce paramètre s'applique aux threads qui effectuent les mêmes étapes du pipeline de traitement des requêtes en parallèle.
Par exemple, lors de la lecture d'une table, s'il est possible d'évaluer des expressions avec des fonctions, filtrer avec WHERE et pré-agréger pour GROUP BY en parallèle en utilisant au moins ‘max\_threads’ nombre de threads, puis ‘max\_threads’ sont utilisés.

Valeur par défaut: nombre de cœurs de processeur physiques.

Si moins d'une requête SELECT est normalement exécutée sur un serveur à la fois, définissez ce paramètre sur une valeur légèrement inférieure au nombre réel de cœurs de processeur.

Pour les requêtes qui sont terminées rapidement en raison d'une limite, vous pouvez définir une valeur inférieure ‘max\_threads’. Par exemple, si le nombre d'entrées se trouvent dans chaque bloc et max\_threads = 8, 8 blocs sont récupérées, même s'il aurait été suffisante pour lire un seul.

Le plus petit de la `max_threads` valeur, moins la mémoire est consommée.

## max\_insert\_threads {#settings-max-insert-threads}

Nombre maximal de threads à exécuter `INSERT SELECT` requête.

Valeurs possibles:

-   0 (or 1) — `INSERT SELECT` pas d'exécution parallèle.
-   Entier positif. Plus grand que 1.

Valeur par défaut: 0.

Parallèle `INSERT SELECT` n'a d'effet que si l' `SELECT` une partie est exécutée en parallèle, voir [max\_threads](#settings-max_threads) paramètre.
Des valeurs plus élevées conduiront à une utilisation de la mémoire plus élevée.

## max\_compress\_block\_size {#max-compress-block-size}

La taille maximale des blocs de données non compressées avant la compression pour l'écriture dans une table. Par défaut, 1 048 576 (1 MiB). Si la taille est réduite, le taux de compression est considérablement réduit, la vitesse de compression et de décompression augmente légèrement en raison de la localisation du cache, et la consommation de mémoire est réduite. Il n'y aucune raison de modifier ce paramètre.

Ne confondez pas les blocs pour la compression (un morceau de mémoire constitué d'octets) avec des blocs pour le traitement des requêtes (Un ensemble de lignes d'une table).

## min\_compress\_block\_size {#min-compress-block-size}

Pour [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)" table. Afin de réduire la latence lors du traitement des requêtes, un bloc est compressé lors de l'écriture de la marque suivante si sa taille est au moins ‘min\_compress\_block\_size’. Par défaut, 65 536.

La taille réelle du bloc, si les données non compressées sont inférieures à ‘max\_compress\_block\_size’ pas moins de cette valeur et pas moins que le volume de données pour une marque.

Regardons un exemple. Supposons que ‘index\_granularity’ a 8192 lors de la création de la table.

Nous écrivons une colonne de type UInt32 (4 octets par valeur). Lors de l'écriture de 8192 lignes, le total sera de 32 KO de données. Puisque min\_compress\_block\_size = 65 536, un bloc compressé sera formé pour toutes les deux marques.

Nous écrivons une colonne URL avec le type de chaîne (taille moyenne de 60 octets par valeur). Lors de l'écriture de 8192 lignes, la moyenne sera légèrement inférieure à 500 Ko de données. Comme il s'agit de plus de 65 536, un bloc compressé sera formé pour chaque marque. Dans ce cas, lors de la lecture de données du disque dans la plage d'une seule marque, les données supplémentaires ne seront pas décompressées.

Il n'y aucune raison de modifier ce paramètre.

## max\_query\_size {#settings-max_query_size}

La partie maximale d'une requête qui peut être prise en RAM pour l'analyse avec L'analyseur SQL.
La requête INSERT contient également des données pour INSERT qui sont traitées par un analyseur de flux séparé (qui consomme O (1) RAM), qui n'est pas inclus dans cette restriction.

Valeur par défaut: 256 Ko.

## interactive\_delay {#interactive-delay}

Intervalle en microsecondes pour vérifier si l'exécution de la requête a été annulée et envoyer la progression.

Valeur par défaut: 100 000 (vérifie l'Annulation et envoie la progression dix fois par seconde).

## connect\_timeout, receive\_timeout, send\_timeout {#connect-timeout-receive-timeout-send-timeout}

Délais d'attente en secondes sur le socket utilisé pour communiquer avec le client.

Valeur par défaut: 10, 300, 300.

## cancel\_http\_readonly\_queries\_on\_client\_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Valeur par défaut: 0

## poll\_interval {#poll-interval}

Verrouillez une boucle d'attente pendant le nombre de secondes spécifié.

Valeur par défaut: 10.

## max\_distributed\_connections {#max-distributed-connections}

Nombre maximal de connexions simultanées avec des serveurs distants pour le traitement distribué d'une seule requête vers une seule table distribuée. Nous vous recommandons de définir une valeur au moins égale au nombre de serveurs dans le cluster.

Valeur par défaut: 1024.

Les paramètres suivants ne sont utilisés que lors de la création de tables distribuées (et lors du lancement d'un serveur), il n'y a donc aucune raison de les modifier lors de l'exécution.

## distributed\_connections\_pool\_size {#distributed-connections-pool-size}

Nombre maximal de connexions simultanées avec des serveurs distants pour le traitement distribué de toutes les requêtes vers une seule table distribuée. Nous vous recommandons de définir une valeur au moins égale au nombre de serveurs dans le cluster.

Valeur par défaut: 1024.

## connect\_timeout\_with\_failover\_ms {#connect-timeout-with-failover-ms}

Délai d'attente en millisecondes pour la connexion à un serveur distant pour un moteur de table distribué, si ‘shard’ et ‘replica’ les sections sont utilisées dans la définition du cluster.
En cas d'échec, plusieurs tentatives sont faites pour se connecter à diverses répliques.

Valeur par défaut: 50.

## connections\_with\_failover\_max\_tries {#connections-with-failover-max-tries}

Nombre maximal de tentatives de connexion avec chaque réplique pour le moteur de table distribué.

Valeur par défaut: 3.

## extrême {#extremes}

Indique s'il faut compter les valeurs extrêmes (les minimums et les maximums dans les colonnes d'un résultat de requête). Accepte 0 ou 1. Par défaut, 0 (désactivé).
Pour plus d'informations, consultez la section “Extreme values”.

## use\_uncompressed\_cache {#setting-use_uncompressed_cache}

Indique s'il faut utiliser un cache de blocs non compressés. Accepte 0 ou 1. Par défaut, 0 (désactivé).
L'utilisation du cache non compressé (uniquement pour les tables de la famille MergeTree) peut réduire considérablement la latence et augmenter le débit lorsque vous travaillez avec un grand nombre de requêtes courtes. Activez ce paramètre pour les utilisateurs qui envoient des requêtes courtes fréquentes. Faites également attention à la [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

Pour les requêtes qui lisent au moins un volume de données assez important (un million de lignes ou plus), le cache non compressé est désactivé automatiquement pour économiser de l'espace pour les requêtes vraiment petites. Cela signifie que vous pouvez garder la ‘use\_uncompressed\_cache’ toujours la valeur 1.

## replace\_running\_query {#replace-running-query}

Lors de l'utilisation de L'interface HTTP, le ‘query\_id’ le paramètre peut être passé. C'est n'importe quelle chaîne qui sert d'Identificateur de requête.
Si une requête d'un utilisateur avec le même ‘query\_id’ il existe déjà à ce moment, le comportement dépend de la ‘replace\_running\_query’ paramètre.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query\_id’ est déjà en cours d'exécution).

`1` – Cancel the old query and start running the new one.

Yandex.Metrica utilise ce paramètre défini sur 1 pour implémenter des suggestions de conditions de segmentation. Après avoir entré le caractère suivant, si l'ancienne requête n'est pas encore terminée, elle doit être annulée.

## stream\_flush\_interval\_ms {#stream-flush-interval-ms}

Fonctionne pour les tables avec des flux dans le cas d'une expiration, ou lorsqu'un thread génère [max\_insert\_block\_size](#settings-max_insert_block_size) rangée.

La valeur par défaut est 7500.

Plus la valeur est petite, plus les données sont vidées dans la table. Régler la valeur trop faible entraîne de mauvaises performances.

## équilibrage {#settings-load_balancing}

Spécifie l'algorithme de sélection des réplicas utilisé pour le traitement des requêtes distribuées.

ClickHouse prend en charge les algorithmes suivants de choix des répliques:

-   [Aléatoire](#load_balancing-random) (par défaut)
-   [Nom d'hôte le plus proche](#load_balancing-nearest_hostname)
-   [Afin](#load_balancing-in_order)
-   [Premier ou aléatoire](#load_balancing-first_or_random)

### Aléatoire (par défaut) {#load_balancing-random}

``` sql
load_balancing = random
```

Le nombre d'erreurs est compté pour chaque réplique. La requête est envoyée au réplica avec le moins d'erreurs, et s'il y en a plusieurs, à n'importe qui d'entre eux.
Inconvénients: la proximité du serveur n'est pas prise en compte; si les répliques ont des données différentes, vous obtiendrez également des données différentes.

### Nom D'Hôte Le Plus Proche {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

Par exemple, exemple01-01-1 et example01-01-2.yandex.ru sont différents dans une position, tandis que l'exemple01-01-1 et l'exemple01-02-2 diffèrent dans deux endroits.
Cette méthode peut sembler primitive, mais elle ne nécessite pas de données externes sur la topologie du réseau, et elle ne compare pas les adresses IP, ce qui serait compliqué pour nos adresses IPv6.

Ainsi, s'il existe des répliques équivalentes, la plus proche par son nom est préférée.
Nous pouvons également supposer que lors de l'envoi d'une requête au même serveur, en l'absence d'Échecs, une requête distribuée ira également aux mêmes serveurs. Ainsi, même si des données différentes sont placées sur les répliques, la requête retournera principalement les mêmes résultats.

### Afin {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Répliques avec le même nombre d'erreurs sont accessibles dans le même ordre qu'ils sont définis dans la configuration.
Cette méthode est appropriée lorsque vous savez exactement quelle réplique est préférable.

### Premier ou aléatoire {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

Cet algorithme choisit la première réplique de l'ensemble ou une réplique aléatoire si la première n'est pas disponible. Il est efficace dans les configurations de topologie de réplication croisée, mais inutile dans d'autres configurations.

Le `first_or_random` algorithme résout le problème de la `in_order` algorithme. Avec `in_order`, si une réplique tombe en panne, la suivante obtient une double charge tandis que les répliques restantes gèrent la quantité habituelle de trafic. Lors de l'utilisation de la `first_or_random` algorithme, la charge est répartie uniformément entre les répliques qui sont encore disponibles.

## prefer\_localhost\_replica {#settings-prefer-localhost-replica}

Active / désactive préférable d'utiliser le réplica localhost lors du traitement des requêtes distribuées.

Valeurs possibles:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [équilibrage](#settings-load_balancing) paramètre.

Valeur par défaut: 1.

!!! warning "Avertissement"
    Désactivez ce paramètre si vous utilisez [max\_parallel\_replicas](#settings-max_parallel_replicas).

## totals\_mode {#totals-mode}

Comment calculer les totaux lorsque HAVING est présent, ainsi que lorsque max\_rows\_to\_group\_by et group\_by\_overflow\_mode = ‘any’ sont présents.
Voir la section “WITH TOTALS modifier”.

## totals\_auto\_threshold {#totals-auto-threshold}

Le seuil de `totals_mode = 'auto'`.
Voir la section “WITH TOTALS modifier”.

## max\_parallel\_replicas {#settings-max_parallel_replicas}

Nombre maximal de répliques pour chaque fragment lors de l'exécution d'une requête.
Par souci de cohérence (pour obtenir différentes parties du même partage de données), Cette option ne fonctionne que lorsque la clé d'échantillonnage est définie.
Le retard de réplique n'est pas contrôlé.

## compiler {#compile}

Activer la compilation des requêtes. Par défaut, 0 (désactivé).

La compilation n'est utilisée que pour une partie du pipeline de traitement des requêtes: pour la première étape de l'agrégation (GROUP BY).
Si cette partie du pipeline a été compilée, la requête peut s'exécuter plus rapidement en raison du déploiement de cycles courts et des appels de fonction d'agrégation intégrés. L'amélioration maximale des performances (jusqu'à quatre fois plus rapide dans de rares cas) est observée pour les requêtes avec plusieurs fonctions d'agrégat simples. Typiquement, le gain de performance est insignifiant. Dans de très rares cas, il peut ralentir l'exécution de la requête.

## min\_count\_to\_compile {#min-count-to-compile}

Combien de fois utiliser potentiellement un morceau de code compilé avant d'exécuter la compilation. Par défaut, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
Si la valeur est 1 ou plus, la compilation se produit de manière asynchrone dans un thread séparé. Le résultat sera utilisé dès qu'il sera prêt, y compris les requêtes en cours d'exécution.

Le code compilé est requis pour chaque combinaison différente de fonctions d'agrégat utilisées dans la requête et le type de clés dans la clause GROUP BY.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output\_format\_json\_quote\_64bit\_integers {#session_settings-output_format_json_quote_64bit_integers}

Si la valeur est true, les entiers apparaissent entre guillemets lors de l'utilisation des formats JSON\* Int64 et UInt64 (pour la compatibilité avec la plupart des implémentations JavaScript); sinon, les entiers sont sortis sans les guillemets.

## format\_csv\_delimiter {#settings-format_csv_delimiter}

Caractère interprété comme un délimiteur dans les données CSV. Par défaut, le délimiteur est `,`.

## input\_format\_csv\_unquoted\_null\_literal\_as\_null {#settings-input_format_csv_unquoted_null_literal_as_null}

Pour le format D'entrée CSV active ou désactive l'analyse des `NULL` comme littéral (synonyme de `\N`).

## output\_format\_csv\_crlf\_end\_of\_line {#settings-output-format-csv-crlf-end-of-line}

Utilisez le séparateur de ligne de style DOS/Windows (CRLF) en CSV au lieu du style Unix (LF).

## output\_format\_tsv\_crlf\_end\_of\_line {#settings-output-format-tsv-crlf-end-of-line}

Utilisez le séparateur de ligne de style DOC/Windows (CRLF) dans TSV au lieu du style Unix (LF).

## insert\_quorum {#settings-insert_quorum}

Active les Écritures de quorum.

-   Si `insert_quorum < 2`, les Écritures de quorum sont désactivées.
-   Si `insert_quorum >= 2`, les Écritures de quorum sont activées.

Valeur par défaut: 0.

Quorum écrit

`INSERT` ne réussit que lorsque ClickHouse parvient à écrire correctement les données `insert_quorum` des répliques au cours de la `insert_quorum_timeout`. Si, pour une raison quelconque, le nombre de répliques avec succès écrit n'atteint pas le `insert_quorum`, l'écriture est considérée comme ayant échoué et ClickHouse supprimera le bloc inséré de toutes les répliques où les données ont déjà été écrites.

Toutes les répliques du quorum sont cohérentes, c'est-à-dire qu'elles contiennent des données de toutes les `INSERT` requête. Le `INSERT` la séquence est linéarisé.

Lors de la lecture des données écrites à partir du `insert_quorum`, vous pouvez utiliser le [select\_sequential\_consistency](#settings-select_sequential_consistency) option.

Clickhouse génère une exception

-   Si le nombre de répliques au moment de la requête est inférieure à la `insert_quorum`.
-   Lors d'une tentative d'écriture de données lorsque le bloc précédent n'a pas encore été inséré dans le `insert_quorum` des répliques. Cette situation peut se produire si l'utilisateur tente d'effectuer une `INSERT` avant le précédent avec le `insert_quorum` est terminé.

Voir aussi:

-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## insert\_quorum\_timeout {#settings-insert_quorum_timeout}

Ecrire dans quorum timeout en secondes. Si le délai d'attente est passé et qu'aucune écriture n'a encore eu lieu, ClickHouse génère une exception et le client doit répéter la requête pour écrire le même bloc dans le même réplica ou tout autre réplica.

Valeur par défaut: 60 secondes.

Voir aussi:

-   [insert\_quorum](#settings-insert_quorum)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## select\_sequential\_consistency {#settings-select_sequential_consistency}

Active ou désactive la cohérence séquentielle pour `SELECT` requête:

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

Utilisation

Lorsque la cohérence séquentielle est activée, ClickHouse permet au client d'exécuter `SELECT` requête uniquement pour les répliques qui contiennent des données de toutes les `INSERT` requêtes exécutées avec `insert_quorum`. Si le client fait référence à une réplique partielle, ClickHouse génère une exception. La requête SELECT n'inclut pas les données qui n'ont pas encore été écrites dans le quorum des répliques.

Voir aussi:

-   [insert\_quorum](#settings-insert_quorum)
-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)

## insert\_deduplicate {#settings-insert-deduplicate}

Active ou désactive la déduplication des blocs `INSERT` (Répliqués\* les tableaux).

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 1.

Par défaut, les blocs insérés dans les tables répliquées `INSERT` déclaration sont dédupliquées (voir [Réplication Des Données](../../engines/table-engines/mergetree-family/replication.md)).

## déduplicate\_blocks\_in\_dependent\_materialized\_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Active ou désactive la vérification de déduplication des vues matérialisées qui reçoivent des données à partir de tables\* répliquées.

Valeurs possibles:

      0 — Disabled.
      1 — Enabled.

Valeur par défaut: 0.

Utilisation

Par défaut, la déduplication n'est pas effectuée pour les vues matérialisées mais en amont, dans la table source.
Si un bloc inséré est ignoré en raison de la déduplication dans la table source, il n'y aura pas d'insertion dans les vues matérialisées attachées. Ce comportement existe pour permettre l'insertion de données hautement agrégées dans des vues matérialisées, dans les cas où les blocs insérés sont les mêmes après l'agrégation de vues matérialisées mais dérivés de différentes insertions dans la table source.
Dans le même temps, ce comportement “breaks” `INSERT` idempotence. Si un `INSERT` dans la table principale a été un succès et `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` permet de changer ce comportement. Lors d'une nouvelle tentative, une vue matérialisée recevra l'insertion répétée et effectuera une vérification de déduplication par elle-même,
ignorant le résultat de la vérification pour la table source, et insérera des lignes perdues en raison de la première défaillance.

## max\_network\_bytes {#settings-max-network-bytes}

Limite le volume de données (en octets) qui est reçu ou transmis sur le réseau lors de l'exécution d'une requête. Ce paramètre s'applique à chaque individu requête.

Valeurs possibles:

-   Entier positif.
-   0 — Data volume control is disabled.

Valeur par défaut: 0.

## max\_network\_bandwidth {#settings-max-network-bandwidth}

Limite la vitesse de l'échange de données sur le réseau en octets par seconde. Ce paramètre s'applique à toutes les requêtes.

Valeurs possibles:

-   Entier positif.
-   0 — Bandwidth control is disabled.

Valeur par défaut: 0.

## max\_network\_bandwidth\_for\_user {#settings-max-network-bandwidth-for-user}

Limite la vitesse de l'échange de données sur le réseau en octets par seconde. Ce paramètre s'applique à toutes les requêtes exécutées simultanément par un seul utilisateur.

Valeurs possibles:

-   Entier positif.
-   0 — Control of the data speed is disabled.

Valeur par défaut: 0.

## max\_network\_bandwidth\_for\_all\_users {#settings-max-network-bandwidth-for-all-users}

Limite la vitesse à laquelle les données sont échangées sur le réseau en octets par seconde. Ce paramètre s'applique à toutes les requêtes exécutées simultanément sur le serveur.

Valeurs possibles:

-   Entier positif.
-   0 — Control of the data speed is disabled.

Valeur par défaut: 0.

## count\_distinct\_implementation {#settings-count_distinct_implementation}

Spécifie de l' `uniq*` les fonctions doivent être utilisées pour [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) construction.

Valeurs possibles:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqcombiné](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

Valeur par défaut: `uniqExact`.

## skip\_unavailable\_shards {#settings-skip_unavailable_shards}

Active ou désactive le saut silencieux des fragments indisponibles.

Tesson est considéré comme indisponible si toutes ses répliques ne sont pas disponibles. Une réplique n'est pas disponible dans les cas suivants:

-   ClickHouse ne peut pas se connecter à la réplique pour une raison quelconque.

    Lors de la connexion à une réplique, ClickHouse effectue plusieurs tentatives. Si toutes ces tentatives échouent, la réplique est considéré comme indisponible.

-   La réplique ne peut pas être résolue via le DNS.

    Si le nom d'hôte du réplica ne peut pas être résolu via DNS, il peut indiquer les situations suivantes:

    -   L'hôte de la réplique n'a pas d'enregistrement DNS. Il peut se produire dans les systèmes avec DNS dynamique, par exemple, [Kubernetes](https://kubernetes.io), où les nœuds peuvent être insolubles pendant les temps d'arrêt, et ce n'est pas une erreur.

    -   Erreur de Configuration. Le fichier de configuration ClickHouse contient un mauvais nom d'hôte.

Valeurs possibles:

-   1 — skipping enabled.

    Si un fragment n'est pas disponible, ClickHouse renvoie un résultat basé sur des données partielles et ne signale pas les problèmes de disponibilité des nœuds.

-   0 — skipping disabled.

    Si un fragment n'est pas disponible, ClickHouse lève une exception.

Valeur par défaut: 0.

## optimize\_skip\_unused\_shards {#settings-optimize_skip_unused_shards}

Active ou désactive le saut des fragments inutilisés pour les requêtes SELECT qui ont la condition de clé de sharding dans PREWHERE/WHERE (suppose que les données sont distribuées par la clé de sharding, sinon ne rien faire).

Valeur par défaut: 0

## force\_optimize\_skip\_unused\_shards {#settings-force_optimize_skip_unused_shards}

Active ou désactive l'exécution de la requête si [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) activé et sauter des fragments inutilisés n'est pas possible. Si le saut n'est pas possible et le paramètre est activé, une exception sera levée.

Valeurs possibles:

-   0 - Désactivé (ne jette)
-   1-Désactiver l'exécution de la requête uniquement si la table a une clé de sharding
-   2-Désactiver l'exécution de la requête quelle que soit la clé de sharding est définie pour la table

Valeur par défaut: 0

## optimize\_throw\_if\_noop {#setting-optimize_throw_if_noop}

Active ou désactive le lancement d'une exception si [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) la requête n'a pas effectué de fusion.

Par défaut, `OPTIMIZE` retourne avec succès même s'il n'a rien fait. Ce paramètre vous permet de différencier ces situations et d'obtenir la raison dans un message d'exception.

Valeurs possibles:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

Valeur par défaut: 0.

## distributed\_replica\_error\_half\_life {#settings-distributed_replica_error_half_life}

-   Type: secondes
-   Valeur par défaut: 60 secondes

Contrôle la vitesse à laquelle les erreurs dans les tables distribuées sont mises à zéro. Si une réplique est indisponible pendant un certain temps, accumule 5 erreurs et distributed\_replica\_error\_half\_life est défini sur 1 seconde, la réplique est considérée comme normale 3 secondes après la dernière erreur.

Voir aussi:

-   [Tableau moteur Distribués](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap](#settings-distributed_replica_error_cap)

## distributed\_replica\_error\_cap {#settings-distributed_replica_error_cap}

-   Type: unsigned int
-   Valeur par défaut: 1000

Le nombre d'erreurs de chaque réplique est plafonné à cette valeur, empêchant une seule réplique d'accumuler trop d'erreurs.

Voir aussi:

-   [Tableau moteur Distribués](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_half\_life](#settings-distributed_replica_error_half_life)

## distributed\_directory\_monitor\_sleep\_time\_ms {#distributed_directory_monitor_sleep_time_ms}

Intervalle de Base pour le [Distribué](../../engines/table-engines/special/distributed.md) tableau moteur à envoyer des données. L'intervalle réel augmente de façon exponentielle en cas d'erreurs.

Valeurs possibles:

-   Un nombre entier positif de millisecondes.

Valeur par défaut: 100 millisecondes.

## distributed\_directory\_monitor\_max\_sleep\_time\_ms {#distributed_directory_monitor_max_sleep_time_ms}

Intervalle maximal pour le [Distribué](../../engines/table-engines/special/distributed.md) tableau moteur à envoyer des données. Limite la croissance exponentielle de l'intervalle défini dans [distributed\_directory\_monitor\_sleep\_time\_ms](#distributed_directory_monitor_sleep_time_ms) paramètre.

Valeurs possibles:

-   Un nombre entier positif de millisecondes.

Valeur par défaut: 30000 millisecondes (30 secondes).

## distributed\_directory\_monitor\_batch\_inserts {#distributed_directory_monitor_batch_inserts}

Active / désactive l'envoi des données insérées par lots.

Lorsque l'envoi par lots est activé, le [Distribué](../../engines/table-engines/special/distributed.md) tableau moteur essaie d'envoyer plusieurs fichiers de données insérées dans une seule opération au lieu de les envoyer séparément. L'envoi par lots améliore les performances du cluster en utilisant mieux les ressources du serveur et du réseau.

Valeurs possibles:

-   1 — Enabled.
-   0 — Disabled.

Valeur par défaut: 0.

## os\_thread\_priority {#setting-os-thread-priority}

Définit la priorité ([beau](https://en.wikipedia.org/wiki/Nice_(Unix))) pour les threads qui exécutent des requêtes. Le planificateur du système d'exploitation considère cette priorité lors du choix du prochain thread à exécuter sur chaque noyau CPU disponible.

!!! warning "Avertissement"
    Pour utiliser ce paramètre, vous devez définir l' `CAP_SYS_NICE` capacité. Le `clickhouse-server` paquet configure lors de l'installation. Certains environnements virtuels ne vous permettent pas de définir `CAP_SYS_NICE` capacité. Dans ce cas, `clickhouse-server` affiche un message à ce sujet au début.

Valeurs possibles:

-   Vous pouvez définir des valeurs dans la gamme `[-20, 19]`.

Des valeurs plus faibles signifient une priorité plus élevée. Les discussions avec des bas `nice` les valeurs de priorité sont effectués plus fréquemment que les discussions avec des valeurs élevées. Les valeurs élevées sont préférables pour les requêtes non interactives de longue durée, car elles leur permettent d'abandonner rapidement des ressources au profit de requêtes interactives courtes lorsqu'elles arrivent.

Valeur par défaut: 0.

## query\_profiler\_real\_time\_period\_ns {#query_profiler_real_time_period_ns}

Définit la période pour une horloge réelle de la [requête profiler](../../operations/optimizing-performance/sampling-query-profiler.md). La vraie minuterie d'horloge compte le temps d'horloge murale.

Valeurs possibles:

-   Nombre entier positif, en nanosecondes.

    Valeurs recommandées:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 pour éteindre la minuterie.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Valeur par défaut: 1000000000 nanosecondes (une fois par seconde).

Voir aussi:

-   Système de table [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## query\_profiler\_cpu\_time\_period\_ns {#query_profiler_cpu_time_period_ns}

Définit la période pour une minuterie D'horloge CPU du [requête profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Cette minuterie ne compte que le temps CPU.

Valeurs possibles:

-   Un nombre entier positif de nanosecondes.

    Valeurs recommandées:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 pour éteindre la minuterie.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Valeur par défaut: 1000000000 nanosecondes.

Voir aussi:

-   Système de table [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## allow\_introspection\_functions {#settings-allow_introspection_functions}

Active des désactive [obscures fonctions](../../sql-reference/functions/introspection.md) pour le profilage de requête.

Valeurs possibles:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

Valeur par défaut: 0.

**Voir Aussi**

-   [Échantillonnage Du Profileur De Requête](../optimizing-performance/sampling-query-profiler.md)
-   Système de table [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## input\_format\_parallel\_parsing {#input-format-parallel-parsing}

-   Type: bool
-   Valeur par défaut: True

Activer l'analyse parallèle des formats de données en préservant l'ordre. Pris en charge uniquement pour les formats TSV, TKSV, CSV et jsoneachrow.

## min\_chunk\_bytes\_for\_parallel\_parsing {#min-chunk-bytes-for-parallel-parsing}

-   Type: unsigned int
-   Valeur par défaut: 1 MiB

La taille minimale du bloc en octets, que chaque thread analysera en parallèle.

## output\_format\_avro\_codec {#settings-output_format_avro_codec}

Définit le codec de compression utilisé pour le fichier Avro de sortie.

Type: string

Valeurs possibles:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [Hargneux](https://google.github.io/snappy/)

Valeur par défaut: `snappy` (si disponible) ou `deflate`.

## output\_format\_avro\_sync\_interval {#settings-output_format_avro_sync_interval}

Définit la taille minimale des données (en octets) entre les marqueurs de synchronisation pour le fichier Avro de sortie.

Type: unsigned int

Valeurs possibles: 32 (32 octets) - 1073741824 (1 GiB)

Valeur par défaut: 32768 (32 Ko)

## format\_avro\_schema\_registry\_url {#settings-format_avro_schema_registry_url}

Définit L'URL de Registre de schéma Confluent à utiliser avec [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) format

Type: URL

Valeur par défaut: vide

## background\_pool\_size {#background_pool_size}

Définit le nombre de threads effectuant des opérations d'arrière-plan dans les moteurs de table (par exemple, fusionne dans [Moteur MergeTree](../../engines/table-engines/mergetree-family/index.md) table). Ce paramètre est appliqué au démarrage du serveur ClickHouse et ne peut pas être modifié dans une session utilisateur. En ajustant ce paramètre, vous gérez la charge du processeur et du disque. Une taille de pool plus petite utilise moins de ressources CPU et disque, mais les processus d'arrière-plan avancent plus lentement, ce qui pourrait éventuellement avoir un impact sur les performances des requêtes.

Valeurs possibles:

-   Tout nombre entier positif.

Valeur par défaut: 16.

[Article Original](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
