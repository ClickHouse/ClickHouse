---
description: 'Ce moteur assure l’intégration avec l’écosystème Amazon S3 et permet
  les importations en continu. Semblable aux moteurs Kafka et RabbitMQ, il offre
  toutefois des fonctionnalités spécifiques à S3.'
sidebar_label: 'S3Queue'
sidebar_position: 181
slug: /engines/table-engines/integrations/s3queue
title: 'Moteur de table S3Queue'
doc_type: 'reference'
---

import ScalePlanFeatureBadge from '@theme/badges/ScalePlanFeatureBadge'

<div id="s3queue-table-engine">
  # Moteur de table S3Queue
</div>

Ce moteur s’intègre à l’écosystème [Amazon S3](https://aws.amazon.com/s3/) et permet l’import en continu. Il est similaire aux moteurs [Kafka](../../../engines/table-engines/integrations/kafka.md) et [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md), mais offre des fonctionnalités spécifiques à S3.

Il est important de bien comprendre cette remarque de la [PR d’origine pour l’implémentation de S3Queue](https://github.com/ClickHouse/ClickHouse/pull/49086/files#diff-e1106769c9c8fbe48dd84f18310ef1a250f2c248800fde97586b3104e9cd6af8R183) : lorsqu’une `MATERIALIZED VIEW` est associée au moteur, le moteur de table S3Queue commence à collecter les données en arrière-plan.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression], [headers], [extra_credentials])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 10,]
    [processing_threads_num = 16,]
    [parallel_inserts = false,]
    [enable_logging_to_queue_log = true,]
    [last_processed_path = "",]
    [tracked_files_limit = 1000,]
    [tracked_file_ttl_sec = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 600000,]
    [polling_backoff_ms = 30000,]
    [cleanup_interval_min_ms = 60000,]
    [cleanup_interval_max_ms = 60000,]
    [buckets = 0,]
    [list_objects_batch_size = 1000,]
    [enable_hash_ring_filtering = 0,]
    [max_processed_files_before_commit = 100,]
    [max_processed_rows_before_commit = 0,]
    [max_processed_bytes_before_commit = 0,]
    [max_processing_time_sec_before_commit = 0,]
```

:::warning
Avant `24.7`, il faut utiliser le préfixe `s3queue_` pour tous les paramètres, sauf `mode`, `after_processing` et `keeper_path`.
:::

**Paramètres du moteur**

Les paramètres de `S3Queue` sont les mêmes que ceux du moteur de table `S3`. Voir la section des paramètres [ici](../../../engines/table-engines/integrations/s3.md#parameters).

**Exemple**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

Utilisation des named collections :

```xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

<div id="settings">
  ## Paramètres
</div>

Pour obtenir la liste des paramètres configurés pour la table, utilisez la table `system.s3_queue_settings`. Disponible à partir de la version `24.10`.

:::note Noms des paramètres (24.7+)
À partir de la version 24.7, les paramètres de S3Queue peuvent être spécifiés avec ou sans le préfixe `s3queue_` :

* **Syntaxe moderne** (24.7+) : `processing_threads_num`, `tracked_file_ttl_sec`, etc.
* **Syntaxe legacy** (toutes les versions) : `s3queue_processing_threads_num`, `s3queue_tracked_file_ttl_sec`, etc.

Les deux formes sont prises en charge à partir de la version 24.7. Les exemples de cette page utilisent la syntaxe moderne sans préfixe.
:::

<div id="mode">
  ### Mode
</div>

Valeurs possibles :

* unordered — En mode unordered, l’ensemble des fichiers déjà traités est suivi à l’aide de nœuds persistants dans ZooKeeper.
* ordered — En mode ordered, les fichiers sont traités dans l’ordre lexicographique. Cela signifie que si un fichier nommé &#39;BBB&#39; a déjà été traité et qu’un fichier nommé &#39;AA&#39; est ensuite ajouté au bucket, il sera ignoré. Seuls le nom maximal (au sens lexicographique) du fichier consommé avec succès, ainsi que les noms des fichiers qui seront retentés après une tentative de chargement infructueuse, sont stockés dans ZooKeeper.

Valeur par défaut : `ordered` dans les versions antérieures à 24.6. À partir de la version 24.6, il n’y a plus de valeur par défaut : ce paramètre doit être spécifié manuellement. Pour les tables créées avec des versions antérieures, la valeur par défaut restera `Ordered` pour des raisons de compatibilité.

<div id="after_processing">
  ### `after_processing`
</div>

Comment gérer le fichier après un traitement réussi.

Valeurs possibles :

* keep.
* delete.
* move.
* tag.

Valeur par défaut : `keep`.

`move` nécessite des paramètres supplémentaires. En cas de déplacement au sein du même bucket, un nouveau préfixe de chemin doit être fourni via `after_processing_move_prefix`.

Le déplacement vers un autre bucket S3 nécessite l’URI du bucket cible via `after_processing_move_uri`, ainsi que les identifiants S3 `after_processing_move_access_key_id` et `after_processing_move_secret_access_key`.

Exemple :

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_retries = 20,
    after_processing_move_prefix = 'dst_prefix',
    after_processing_move_uri = 'https://clickhouse-public-datasets.s3.amazonaws.com/dst-bucket',
    after_processing_move_access_key_id = 'test',
    after_processing_move_secret_access_key = 'test';
```

Le déplacement d’un conteneur Azure vers un autre nécessite la chaîne de connexion Blob Storage `after_processing_move_connection_string` ainsi que le nom du conteneur `after_processing_move_container`. Voir [les paramètres d’AzureQueue](../../../engines/table-engines/integrations/azure-queue.md#settings).

L’ajout de tags nécessite de fournir la clé et la valeur du tag via `after_processing_tag_key` et `after_processing_tag_value`.

<div id="after_processing_retries">
  ### `after_processing_retries`
</div>

Nombre de tentatives pour l’action de post-traitement demandée, avant d’abandonner.

Valeurs possibles :

* Entier non négatif.

Valeur par défaut : `10`.

<div id="after_processing_move_access_key_id">
  ### `after_processing_move_access_key_id`
</div>

ID de la clé d’accès du bucket S3 vers lequel déplacer les fichiers traités avec succès, si la destination est un autre bucket S3.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="after_processing_move_prefix">
  ### `after_processing_move_prefix`
</div>

Préfixe de chemin vers lequel déplacer les fichiers traités avec succès. Ce paramètre s’applique aussi bien à un déplacement au sein du même bucket que vers un autre bucket.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="after_processing_move_preserve_path">
  ### `after_processing_move_preserve_path`
</div>

Si `true`, le chemin complet de l’objet source est ajouté à `after_processing_move_prefix` lors du déplacement d’un fichier traité avec succès, afin de préserver à la destination la structure du répertoire source sous le bucket. Si `false`, seul le nom du fichier est utilisé et la structure du répertoire source est aplatie.

Valeurs possibles :

* `true` / `false`.

Valeur par défaut : `false`.

<div id="after_processing_move_secret_access_key">
  ### `after_processing_move_secret_access_key`
</div>

Clé d’accès secrète du bucket S3 de destination, utilisée pour y déplacer les fichiers traités avec succès si la destination est un autre bucket S3.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="after_processing_move_uri">
  ### `after_processing_move_uri`
</div>

URI du bucket S3 vers lequel déplacer les fichiers traités avec succès, si la destination est un autre bucket S3.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="after_processing_tag_key">
  ### `after_processing_tag_key`
</div>

Clé du tag à appliquer aux fichiers traités avec succès, si `after_processing='tag'`.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="after_processing_tag_value">
  ### `after_processing_tag_value`
</div>

Valeur de tag à attribuer aux fichiers traités avec succès, si `after_processing='tag'`.

Valeurs possibles :

* String.

Valeur par défaut : chaîne vide.

<div id="keeper_path">
  ### `keeper_path`
</div>

Chemin vers les métadonnées de la file d’attente dans ZooKeeper. S’il n’est pas explicitement spécifié, ClickHouse construit le chemin à partir de `s3queue_default_zookeeper_path`, de l’UUID de la base de données et de l’UUID de la table. Les valeurs absolues (commençant par `/`) sont utilisées telles quelles, tandis que les valeurs relatives sont ajoutées au préfixe configuré. Les macros telles que `{database}` ou `{uuid}` sont développées avant que le moteur ne se connecte à ZooKeeper.

Pour cibler un cluster ZooKeeper auxiliaire, faites précéder la valeur du nom configuré, par exemple `analytics_keeper:/clickhouse/queue/orders`. Le nom doit exister dans `<auxiliary_zookeepers>` ; sinon, le moteur signale `Unknown auxiliary ZooKeeper name ...`. La chaîne complète (y compris le préfixe) est conservée dans `SHOW CREATE TABLE` afin que l’instruction puisse être répliquée telle quelle.

Valeurs possibles :

* String.

Valeur par défaut : `/`.

<div id="loading_retries">
  ### `loading_retries`
</div>

Réessaie le chargement du fichier jusqu’au nombre de fois spécifié.
Valeurs possibles :

* Entier non négatif.

Valeur par défaut : `10`.

<div id="processing_threads_num">
  ### `processing_threads_num`
</div>

Nombre de threads utilisés pour le traitement. S’applique uniquement au mode `Unordered`.

Valeur par défaut : nombre de CPU ou 16.

<div id="parallel_inserts">
  ### `parallel_inserts`
</div>

Par défaut, `processing_threads_num` n&#39;exécute qu&#39;un seul `INSERT` : il se contente donc de télécharger les fichiers et de les analyser dans plusieurs threads.
Mais cela limite le parallélisme. Pour un meilleur débit, utilisez `parallel_inserts=true` ; cela permet d&#39;insérer les données en parallèle (gardez toutefois à l&#39;esprit que cela générera un plus grand nombre de parties de données pour la famille MergeTree).

:::note
Les `INSERT` seront lancés en fonction des paramètres `max_process*_before_commit`.
:::

Valeur par défaut : `false`.

<div id="enable_logging_to_queue_log">
  ### `enable_logging_to_queue_log`
</div>

Active la journalisation dans `system.s3queue_log`.

Valeur par défaut : `1`.

<div id="polling_min_timeout_ms">
  ### `polling_min_timeout_ms`
</div>

Spécifie la durée minimale, en millisecondes, pendant laquelle ClickHouse attend avant d&#39;effectuer une nouvelle tentative de polling.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `1000`.

<div id="polling_max_timeout_ms">
  ### `polling_max_timeout_ms`
</div>

Définit la durée maximale, en millisecondes, pendant laquelle ClickHouse attend avant d’effectuer la tentative de polling suivante.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `600000`.

<div id="polling_backoff_ms">
  ### `polling_backoff_ms`
</div>

Détermine le délai d’attente supplémentaire ajouté à l’intervalle de polling précédent lorsqu’aucun nouveau fichier n’est trouvé. Le polling suivant a lieu après la somme de l’intervalle précédent et de cette valeur de backoff, ou de l’intervalle maximal, selon la plus petite des deux valeurs.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `30000`.

<div id="tracked_files_limit">
  ### `tracked_files_limit`
</div>

Permet de limiter le nombre de nœuds ZooKeeper lorsque le mode &#39;unordered&#39; est utilisé ; n&#39;a aucun effet en mode &#39;ordered&#39;.
Si la limite est atteinte, les fichiers traités les plus anciens seront supprimés du nœud ZooKeeper puis traités à nouveau.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `1000`.

<div id="tracked_file_ttl_sec">
  ### `tracked_file_ttl_sec`
</div>

Nombre maximal de secondes pendant lesquelles conserver les fichiers traités dans le nœud ZooKeeper (conservation indéfinie par défaut) pour le mode &#39;unordered&#39; ; n’a aucun effet pour le mode &#39;ordered&#39;.
Une fois ce délai écoulé, le fichier sera réimporté.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `0`.

<div id="cleanup_interval_min_ms">
  ### `cleanup_interval_min_ms`
</div>

Pour le mode « Ordered ». Définit la valeur minimale de l’intervalle de replanification d’une tâche en arrière-plan chargée de gérer le TTL des fichiers suivis et l’ensemble maximal de fichiers suivis.

Valeur par défaut : `60000`.

<div id="cleanup_interval_max_ms">
  ### `cleanup_interval_max_ms`
</div>

Pour le mode « Ordered ». Définit la limite supérieure de l’intervalle de replanification d’une tâche d’arrière-plan, chargée de gérer le TTL des fichiers suivis ainsi que la taille maximale de l’ensemble des fichiers suivis.

Valeur par défaut : `60000`.

<div id="buckets">
  ### `buckets`
</div>

Pour le mode « Ordered ». Disponible depuis la version `24.6`. S&#39;il existe plusieurs répliques de la table S3Queue, utilisant toutes le même répertoire de métadonnées dans Keeper, la valeur de `buckets` doit être au moins égale au nombre de répliques. Si le paramètre `processing_threads` est également utilisé, il peut être judicieux d&#39;augmenter davantage la valeur du paramètre `buckets`, car elle détermine le niveau réel de parallélisme du traitement de `S3Queue`.

<div id="use_persistent_processing_nodes">
  ### `use_persistent_processing_nodes`
</div>

Par défaut, la table S3Queue a toujours utilisé des nœuds de traitement éphémères, ce qui pouvait entraîner des doublons dans les données si la session ZooKeeper expirait avant que S3Queue n’enregistre les fichiers traités dans ZooKeeper, mais après le début de leur traitement. Ce paramètre force le server à éliminer tout risque de doublons en cas d’expiration de la session Keeper.

<div id="persistent_processing_node_ttl_seconds">
  ### `persistent_processing_node_ttl_seconds`
</div>

En cas d’arrêt brutal du serveur, il est possible que, si `use_persistent_processing_nodes` est activé, certains nœuds de traitement n’aient pas été supprimés. Ce paramètre définit la durée pendant laquelle ces nœuds de traitement peuvent être supprimés en toute sécurité. Le même TTL est également utilisé pour le verrou du bucket en mode `Ordered`, qui peut être conservé plus longtemps qu’un seul nœud de traitement ; la valeur doit donc aussi en tenir compte.

Valeur par défaut : `21600` (6 heures).

<div id="s3-settings">
  ## Paramètres relatifs à S3
</div>

Le moteur prend en charge tous les paramètres relatifs à S3. Pour plus d&#39;informations sur les paramètres S3, consultez [cette page](../../../engines/table-engines/integrations/s3.md).

<div id="s3-role-based-access">
  ## Accès S3 basé sur les rôles
</div>

<ScalePlanFeatureBadge feature="S3 Role-Based Access" />

Le moteur de table S3Queue prend en charge l&#39;accès basé sur les rôles.
Consultez la documentation [ici](/fr/cloud/data-sources/secure-s3) pour connaître les étapes à suivre afin de configurer un rôle permettant d&#39;accéder à votre bucket.

Une fois le rôle configuré, un `roleARN` peut être transmis via le paramètre `extra_credentials`, comme indiqué ci-dessous :

```sql
CREATE TABLE s3_table
(
    ts DateTime,
    value UInt64
)
ENGINE = S3Queue(
                'https://<your_bucket>/*.csv',
                extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/<your_role>')
                ,'CSV')
SETTINGS
    ...
```

<div id="ordered-mode">
  ## Mode ordonné de S3Queue
</div>

Le mode de traitement `S3Queue` permet de stocker moins de métadonnées dans ZooKeeper, mais il présente une limite : les fichiers ajoutés ultérieurement doivent avoir des noms alphanumériquement supérieurs.

Le mode `ordered` de `S3Queue`, tout comme `unordered`, prend en charge le paramètre `(s3queue_)processing_threads_num` (le préfixe `s3queue_` est facultatif), qui permet de contrôler le nombre de threads chargés de traiter localement les fichiers `S3` sur le server.

Pour le mode `ordered` sans partitionnement, ClickHouse peut reprendre l’énumération S3 à partir de la dernière key traitée afin d’éviter de relister tout l’historique du préfixe. En mode ordonné avec buckets, le point de reprise est choisi de manière prudente comme la plus petite key traitée parmi tous les buckets afin d’éviter de sauter des fichiers non traités.
Cette optimisation de reprise de l’énumération n’est utilisée que pour les queues basées sur S3 en mode ordonné sans partitionnement (pas pour AzureQueue ni lorsque `partitioning_mode` est défini).
En outre, le mode `ordered` introduit également un autre paramètre appelé `(s3queue_)buckets`, qui correspond à des « threads logiques ». Cela signifie que, dans un scénario distribué, lorsqu’il y a plusieurs servers avec des répliques de table `S3Queue`, ce paramètre définit le nombre d’unités de traitement. Par exemple, chaque thread de traitement sur chaque réplique `S3Queue` tentera de verrouiller un certain `bucket` pour le traitement, chaque `bucket` étant attribué à certains fichiers via le hash du nom du fichier. Par conséquent, dans un scénario distribué, il est fortement recommandé que le paramètre `(s3queue_)buckets` soit au moins égal au nombre de répliques, voire supérieur. Il n’y a aucun inconvénient à avoir un nombre de buckets supérieur au nombre de répliques. Le scénario optimal est que le paramètre `(s3queue_)buckets` soit égal au produit de `number_of_replicas` et de `(s3queue_)processing_threads_num`.
L’utilisation du paramètre `(s3queue_)processing_threads_num` n’est pas recommandée avant la version `24.6`.
Le paramètre `(s3queue_)buckets` est disponible à partir de la version `24.6`.

<div id="select">
  ## SELECT sur le moteur de table S3Queue
</div>

Les requêtes SELECT sont interdites par défaut sur les tables S3Queue. Cela suit le modèle classique des files d’attente, où les données sont lues une seule fois puis supprimées de la file d’attente. SELECT est interdit pour éviter toute perte accidentelle de données.
Cependant, cela peut parfois s’avérer utile. Pour cela, vous devez définir le paramètre `stream_like_engine_allow_direct_select` sur `True`.
Le moteur S3Queue dispose d’un paramètre spécial pour les requêtes SELECT : `commit_on_select`. Définissez-le sur `False` pour conserver les données dans la file d’attente après leur lecture, ou sur `True` pour les supprimer.

<div id="description">
  ## Description
</div>

`SELECT` n&#39;est pas particulièrement utile pour l&#39;importation en continu (sauf pour le débogage), car chaque fichier ne peut être importé qu&#39;une seule fois. Il est plus pratique de mettre en place des traitements en temps réel à l&#39;aide de [vues matérialisées](../../../sql-reference/statements/create/view.md). Pour ce faire :

1. Utilisez le moteur pour créer une table qui consomme les données à partir du chemin spécifié dans S3, et considérez-la comme un flux de données.
2. Créez une table avec la structure souhaitée.
3. Créez une vue matérialisée qui convertit les données du moteur et les insère dans une table créée précédemment.

Lorsque la `MATERIALIZED VIEW` est associée au moteur, elle commence à collecter les données en arrière-plan.

Exemple :

```sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier.
* `_file` — Nom du fichier.
* `_size` — Taille du fichier.
* `_time` — Date de création du fichier.

Pour plus d&#39;informations sur les colonnes virtuelles, voir [ici](../../../engines/table-engines/index.md#table_engines-virtual_columns).

<div id="wildcards-in-path">
  ## Caractères génériques dans le chemin
</div>

L’argument `path` peut spécifier plusieurs fichiers à l’aide de caractères génériques de type bash. Pour être traité, un fichier doit exister et correspondre à l’intégralité du motif de chemin. La liste des fichiers est déterminée lors du `SELECT` (et non au moment du `CREATE`).

* `*` — Remplace n’importe quel nombre de caractères, à l’exception de `/`, y compris une chaîne vide.
* `**` — Remplace n’importe quel nombre de caractères, y compris `/`, y compris une chaîne vide.
* `?` — Remplace un seul caractère.
* `{some_string,another_string,yet_another_one}` — Remplace l’une des chaînes `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Remplace n’importe quel nombre dans l’intervalle de N à M, bornes incluses. N et M peuvent comporter des zéros non significatifs, par exemple `000..078`.

Les constructions avec `{}` sont similaires à la fonction de table [remote](../../../sql-reference/table-functions/remote.md).

<div id="limitations">
  ## Limitations
</div>

1. Des lignes dupliquées peuvent résulter de :

* une exception se produit lors de l’analyse syntaxique au milieu du traitement du fichier et les tentatives sont activées via `s3queue_loading_retries` ;

* `S3Queue` est configuré sur plusieurs serveurs pointant vers le même chemin dans ZooKeeper, et la session Keeper expire avant qu’un serveur n’ait pu valider le fichier traité, ce qui peut amener un autre serveur à prendre en charge le traitement du fichier, alors que celui-ci a pu être partiellement ou entièrement traité par le premier serveur ; toutefois, ce n’est plus le cas depuis la version 25.8 si `use_persistent_processing_nodes = 1`.

* arrêt anormal du serveur.

2. Si `S3Queue` est configuré sur plusieurs serveurs pointant vers le même chemin dans ZooKeeper et que le mode `Ordered` est utilisé, alors `s3queue_loading_retries` ne fonctionnera pas. Cela sera bientôt corrigé.

<div id="introspection">
  ## Introspection
</div>

Pour l’introspection, utilisez la table sans état `system.s3queue_metadata_cache` et la table persistante `system.s3queue_log`.

1. `system.s3queue_metadata_cache`. Cette table n’est pas persistante et affiche l’état en mémoire de `S3Queue` : quels fichiers sont actuellement en cours de traitement, et quels fichiers ont été traités ou ont échoué.

```sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_metadata_cache
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Exemple :

```sql

SELECT *
FROM system.s3queue_metadata_cache

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. `system.s3queue_log`. Table persistante. Contient les mêmes informations que `system.s3queue_metadata_cache`, mais pour les fichiers `processed` et `failed`.

La table a la structure suivante :

```sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Pour utiliser `system.s3queue_log`, définissez sa configuration dans le fichier de configuration du serveur :

```xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

Exemple :

```sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```