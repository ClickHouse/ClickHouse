---
title: Paramètres du serveur hors de la source
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

Activé par défaut dans les déploiements ClickHouse Cloud.

Si ce paramètre n’est pas activé par défaut dans votre environnement, selon la façon dont ClickHouse a été installé, vous pouvez suivre les instructions ci-dessous pour l’activer ou le désactiver.

**Activation**

Pour activer manuellement la collecte de l’historique des métriques asynchrones [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), créez `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` avec le contenu suivant :

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**Désactivation**

Pour désactiver le paramètre `asynchronous_metric_log`, créez le fichier suivant `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` avec le contenu ci-dessous :

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

Utilisez l&#39;adresse d&#39;origine pour l&#39;authentification des clients connectés via un proxy.

:::note
Ce paramètre doit être utilisé avec une extrême prudence, car les adresses transmises peuvent être facilement usurpées ; les serveurs acceptant ce type d&#39;authentification ne doivent pas être accessibles directement, mais uniquement via un proxy de confiance.
:::

<div id="backups">
  ## sauvegardes
</div>

Paramètres des sauvegardes, utilisés lors de l’exécution des instructions [`BACKUP` et `RESTORE`](/fr/operations/backup/overview).

Les paramètres suivants peuvent être configurés à l’aide de sous-balises :

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Détermine si plusieurs opérations de sauvegarde peuvent s’exécuter simultanément sur le même hôte.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Détermine si plusieurs opérations de restauration peuvent s’exécuter simultanément sur le même hôte.', 'true'),
    ('allowed_disk', 'String', 'Disque de destination de la sauvegarde lors de l’utilisation de `File()`. Ce paramètre doit être défini pour pouvoir utiliser `File`.', ''),
    ('allowed_path', 'String', 'Chemin de destination de la sauvegarde lors de l’utilisation de `File()`. Ce paramètre doit être défini pour pouvoir utiliser `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Nombre de tentatives de collecte des métadonnées avant une attente en cas d’incohérence après comparaison des métadonnées collectées.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Délai d’expiration, en millisecondes, pour la collecte des métadonnées pendant la sauvegarde.', '600000'),
    ('compare_collected_metadata', 'Bool', 'Si true, compare les métadonnées collectées aux métadonnées existantes afin de s’assurer qu’elles n’ont pas été modifiées pendant la sauvegarde.', 'true'),
    ('create_table_timeout', 'UInt64', 'Délai d’expiration, en millisecondes, pour la création des tables pendant la restauration.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Nombre maximal de tentatives après une erreur de version lors d’une nouvelle tentative de sauvegarde/restauration coordonnée.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Temps d’attente maximal, en millisecondes, avant la prochaine tentative de collecte des métadonnées.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Temps d’attente minimal, en millisecondes, avant la prochaine tentative de collecte des métadonnées.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'Si la commande `BACKUP` échoue, ClickHouse tentera de supprimer les fichiers déjà copiés dans la sauvegarde avant l’échec ; sinon, il laissera les fichiers copiés tels quels.', 'true'),
    ('sync_period_ms', 'UInt64', 'Période de synchronisation, en millisecondes, pour la sauvegarde/restauration coordonnée.', '5000'),
    ('test_inject_sleep', 'Bool', 'Attente liée aux tests', 'false'),
    ('test_randomize_order', 'Bool', 'Si true, randomise l’ordre de certaines opérations à des fins de test.', 'false'),
    ('zookeeper_path', 'String', 'Chemin dans ZooKeeper où sont stockées les métadonnées de sauvegarde et de restauration lors de l’utilisation de la clause `ON CLUSTER`.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
  */ }

| Paramètre                                           | Type   | Description                                                                                                                                                                     | Par défaut            |
| :-------------------------------------------------- | :----- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | Détermine si plusieurs opérations de sauvegarde peuvent s’exécuter simultanément sur le même hôte.                                                                              | `true`                |
| `allow_concurrent_restores`                         | Bool   | Détermine si plusieurs opérations de restauration peuvent s’exécuter simultanément sur le même hôte.                                                                            | `true`                |
| `allowed_disk`                                      | String | Disque sur lequel effectuer la sauvegarde lors de l’utilisation de `File()`. Ce paramètre doit être défini pour pouvoir utiliser `File`.                                        | &#96;&#96;            |
| `allowed_path`                                      | String | Chemin vers lequel effectuer la sauvegarde lors de l’utilisation de `File()`. Ce paramètre doit être défini pour pouvoir utiliser `File`.                                       | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | Nombre de tentatives de collecte des métadonnées avant une attente en cas d’incohérence après comparaison des métadonnées collectées.                                           | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | Délai d’expiration, en millisecondes, pour la collecte des métadonnées pendant la sauvegarde.                                                                                      | `600000`              |
| `compare_collected_metadata`                        | Bool   | Si la valeur est `true`, compare les métadonnées collectées aux métadonnées existantes afin de s’assurer qu’elles ne changent pas pendant la sauvegarde.                        | `true`                |
| `create_table_timeout`                              | UInt64 | Délai d’expiration, en millisecondes, pour la création de tables pendant la restauration.                                                                                          | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | Nombre maximal de tentatives de nouvelle exécution après une erreur de version invalide pendant une sauvegarde/restauration coordonnée.                                         | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Temps d’attente maximal, en millisecondes, avant la prochaine tentative de collecte des métadonnées.                                                                            | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Temps d’attente minimal, en millisecondes, avant la prochaine tentative de collecte des métadonnées.                                                                            | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | Si la commande `BACKUP` échoue, ClickHouse essaiera de supprimer les fichiers déjà copiés dans la sauvegarde avant l’échec ; sinon, il laissera les fichiers copiés tels quels. | `true`                |
| `sync_period_ms`                                    | UInt64 | Période de synchronisation, en millisecondes, pour la sauvegarde/restauration coordonnée.                                                                                       | `5000`                |
| `test_inject_sleep`                                 | Bool   | Attente utilisée pour les tests                                                                                                                                                 | `false`               |
| `test_randomize_order`                              | Bool   | Si la valeur est `true`, randomise l’ordre de certaines opérations à des fins de test.                                                                                          | `false`               |
| `zookeeper_path`                                    | String | Chemin dans ZooKeeper où sont stockées les métadonnées de sauvegarde et de restauration lors de l’utilisation de la clause `ON CLUSTER`.                                        | `/clickhouse/backups` |

Ce paramètre est configuré par défaut comme suit :

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

Contient des informations sur toutes les tâches en arrière-plan exécutées via différents pools en arrière-plan.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

Facteur de coût pour le type d’authentification `bcrypt_password`, qui utilise l’[algorithme Bcrypt](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
Le facteur de coût définit la quantité de calcul et le temps nécessaires pour calculer le hachage et vérifier le mot de passe.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
Pour les applications nécessitant des authentifications fréquentes,
envisagez d&#39;autres méthodes d&#39;authentification en raison du
coût de calcul de bcrypt lorsque les facteurs de coût sont élevés.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

S’il est défini sur true, les utilisateurs doivent disposer d’un grant pour créer une table avec un engine spécifique, par exemple `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
Par défaut, pour des raisons de backward compatibility, la création d’une table avec un table engine spécifique ignore le grant. Vous pouvez toutefois modifier ce comportement en définissant ce paramètre sur true.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

L’intervalle, en secondes, avant le rechargement des dictionnaires intégrés.

ClickHouse recharge les dictionnaires intégrés toutes les x secondes. Cela permet de modifier les dictionnaires « à la volée » sans redémarrer le serveur.

**Exemple**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## compression
</div>

Paramètres de compression des données pour les tables à moteur [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

:::note
Nous vous recommandons de ne pas modifier ce paramètre si vous débutez avec ClickHouse.
:::

**Modèle de configuration**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**Champs de `<case>`**:

* `min_part_size` – Taille minimale d&#39;une partie de données.
* `min_part_size_ratio` – Rapport entre la taille de la partie de données et celle de la table.
* `method` – Méthode de compression. Valeurs acceptées : `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – Niveau de compression. Voir [Codecs](/fr/sql-reference/statements/create/table#general-purpose-codecs).

:::note
Vous pouvez configurer plusieurs sections `<case>`.
:::

**Actions lorsque les conditions sont satisfaites**:

* Si une partie de données correspond à un ensemble de conditions, ClickHouse utilise la méthode de compression spécifiée.
* Si une partie de données correspond à plusieurs ensembles de conditions, ClickHouse utilise le premier ensemble correspondant.

:::note
Si aucune condition n&#39;est satisfaite pour une partie de données, ClickHouse utilise la compression `lz4`.
:::

**Exemple**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## chiffrement
</div>

Configure une commande permettant d&#39;obtenir une clé à utiliser par les [codecs de chiffrement](/fr/sql-reference/statements/create/table#encryption-codecs). La ou les clés doivent être stockées dans des variables d&#39;environnement ou définies dans le fichier de configuration.

Les clés peuvent être en hexadécimal ou sous forme de chaîne d&#39;une longueur de 16 octets.

**Exemple**

Chargement depuis la configuration :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Il est déconseillé de stocker les clés dans le fichier de configuration. Ce n’est pas sûr. Vous pouvez déplacer les clés dans un fichier de configuration distinct sur un disque sécurisé, puis placer un lien symbolique vers ce fichier dans le dossier `config.d/`.
:::

Chargement depuis la configuration, lorsque la clé est en hexadécimal :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Chargement de la clé depuis la variable d’environnement :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Ici, `current_key_id` définit la clé active pour le chiffrement, et toutes les clés spécifiées peuvent être utilisées pour le déchiffrement.

Chacune de ces méthodes peut être appliquée à plusieurs clés :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Ici, `current_key_id` indique la clé actuellement utilisée pour le chiffrement.

Les utilisateurs peuvent également ajouter un nonce, qui doit faire 12 octets de long (par défaut, les processus de chiffrement et de déchiffrement utilisent un nonce composé d’octets nuls) :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Ou il peut aussi être défini en hexadécimal :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Tout ce qui précède peut s’appliquer à `aes_256_gcm_siv` (mais la clé doit faire 32 octets).
:::

<div id="error_log">
  ## error_log
</div>

Il est désactivé par défaut.

**Activation**

Pour activer manuellement la collecte de l’historique des erreurs [`system.error_log`](../../operations/system-tables/error_log.md), créez `/etc/clickhouse-server/config.d/error_log.xml` avec le contenu suivant :

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Désactivation**

Pour désactiver le paramètre `error_log`, créez le fichier suivant `/etc/clickhouse-server/config.d/disable_error_log.xml` avec le contenu ci-dessous :

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

Liste des préfixes utilisés pour les [paramètres personnalisés](/fr/operations/settings/query-level#custom_settings).
S&#39;il y a plusieurs préfixes, séparez-les par des virgules.

**Exemple**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**Voir aussi**

* [Paramètres personnalisés](/fr/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

Configure la limite souple pour la taille des fichiers core dump.

:::note
La limite stricte se configure via les outils système
:::

**Exemple**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

Profil de paramètres par défaut. Les profils de paramètres se trouvent dans le fichier indiqué par le paramètre `user_config`.

**Exemple**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

Chemin d’accès au fichier de configuration des dictionnaires.

Chemin :

* Indiquez le chemin absolu ou le chemin relatif au fichier de configuration du serveur.
* Le chemin peut contenir les caractères génériques * et ?.

Voir aussi :

* &quot;[Dictionaries](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**Exemple**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

Le chemin du fichier de configuration des fonctions exécutables définies par l’utilisateur.

Chemin :

* Spécifiez le chemin absolu ou le chemin relatif au fichier de configuration du serveur.
* Le chemin peut contenir les caractères génériques * et ?.

Voir aussi :

* &quot;[Executable User Defined Functions](/fr/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**Exemple**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

Envoi de données vers [Graphite](https://github.com/graphite-project).

Paramètres :

* `host` – Le serveur Graphite.
* `port` – Le port du serveur Graphite.
* `interval` – L’intervalle d’envoi, en secondes.
* `timeout` – Le délai d’expiration de l’envoi, en secondes.
* `root_path` – Préfixe des clés.
* `metrics` – Envoi des données de la table [system.metrics](/fr/operations/system-tables/metrics).
* `events` – Envoi des deltas accumulés sur la période à partir de la table [system.events](/fr/operations/system-tables/events).
* `events_cumulative` – Envoi des données cumulées de la table [system.events](/fr/operations/system-tables/events).
* `asynchronous_metrics` – Envoi des données de la table [system.asynchronous&#95;metrics](/fr/operations/system-tables/asynchronous_metrics).

Vous pouvez configurer plusieurs clauses `<graphite>`. Par exemple, vous pouvez vous en servir pour envoyer différentes données à des intervalles différents.

**Exemple**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Paramètres pour réduire progressivement le volume des données pour Graphite.

Pour plus de détails, consultez [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Exemple**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

Permet d’utiliser des gestionnaires HTTP personnalisés.
Pour ajouter un nouveau gestionnaire HTTP, ajoutez simplement une nouvelle `<rule>`.
Les règles sont évaluées de haut en bas dans l’ordre défini,
et la première correspondance exécute le gestionnaire.
Une règle sans condition de correspondance (uniquement `handler`) correspond à toutes les requêtes ; comme les règles sont évaluées dans l’ordre,
une telle règle n’est utile qu’en tant que solution de repli, placée en dernier.

Les paramètres suivants peuvent être configurés via des sous-balises (toutes ces sous-balises sont facultatives, sauf `handler`) :

| Sub-tags             | Definition                                                                                                                                                                                                                                                                                                                              |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | Permet de faire correspondre le chemin d’URL de la requête. La query string est ignorée lors de la correspondance                                                                                                                                                                                                                       |
| `url_prefix`         | Permet de faire correspondre le chemin d’URL de la requête à un chemin de base : le chemin lui-même ou tout ce qui se trouve en dessous, sur une limite de segment de chemin (par ex. &#39;/api/v1&#39; correspond à /api/v1, /api/v1/ et /api/v1/write, mais pas à /api/v1beta). La query string est ignorée lors de la correspondance |
| `url_regexp`         | Permet de faire correspondre le chemin d’URL de la requête à une expression régulière. La query string est ignorée lors de la correspondance                                                                                                                                                                                            |
| `full_url`           | Permet de faire correspondre l’URL complète de la requête `scheme://host:port/path`. La query string est ignorée lors de la correspondance, et le host est l’adresse IP de la connexion (et non l’en-tête `Host`)                                                                                                                       |
| `full_url_prefix`    | Permet de faire correspondre l’URL complète de la requête `scheme://host:port/path` à l’base URL `scheme://host:port/base_path`, sur une limite de segment de chemin (voir `url_prefix`). La query string est ignorée lors de la correspondance                                                                                         |
| `full_url_regexp`    | Permet de faire correspondre l’URL complète de la requête `scheme://host:port/path` à une expression régulière. La query string est ignorée lors de la correspondance                                                                                                                                                                   |
| `methods`            | Permet de faire correspondre les méthodes de requête ; vous pouvez utiliser des virgules pour séparer plusieurs méthodes                                                                                                                                                                                                                |
| `headers`            | Permet de faire correspondre les en-têtes de requête ; chaque élément enfant est comparé (le nom de l’élément enfant correspond au nom de l’en-tête)                                                                                                                                                                                    |
| `headers_regexp`     | Comme `headers`, mais la valeur de chaque élément enfant est comparée à une expression régulière                                                                                                                                                                                                                                        |
| `empty_query_string` | Vérifie l’absence de query string dans l’URL                                                                                                                                                                                                                                                                                            |
| `handler`            | Le gestionnaire de requête (obligatoire)                                                                                                                                                                                                                                                                                                |

:::note
Au lieu de `url_regexp`, `full_url_regexp` et `headers_regexp`, vous pouvez aussi écrire une expression régulière dans `url`, `full_url` ou `headers` à l’aide du préfixe `regex:` (par ex. `<url>regex:/api/.*</url>`). Cette syntaxe reste prise en charge pour assurer la backward compatibility, mais elle est Obsolete : privilégiez les sous-balises dédiées `url_regexp`, `full_url_regexp` et `headers_regexp`.
:::

`handler` contient les paramètres suivants, qui peuvent être configurés via des sous-balises :

| Sub-tags           | Definition                                                                                                                                                                                                                          |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | Un emplacement de redirection                                                                                                                                                                                                       |
| `type`             | Types pris en charge : static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                                                                                                            |
| `status`           | À utiliser avec le type static, code d’état de la réponse                                                                                                                                                                           |
| `query_param_name` | À utiliser avec le type dynamic&#95;query&#95;handler ; extrait et exécute la valeur correspondant à la valeur de `<query_param_name>` dans les params de la requête HTTP                                                           |
| `query`            | À utiliser avec le type predefined&#95;query&#95;handler ; exécute la query lorsque le gestionnaire est appelé                                                                                                                      |
| `content_type`     | À utiliser avec le type static, type de contenu de la réponse                                                                                                                                                                       |
| `response_content` | À utiliser avec le type static, contenu de la réponse envoyé au client ; lors de l’utilisation du préfixe &#39;file://&#39; ou &#39;config://&#39;, récupère le contenu depuis le fichier ou la configuration et l’envoie au client |

En plus d’une liste de règles, vous pouvez spécifier `<defaults/>`, ce qui active tous les gestionnaires par défaut.

Exemple :

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

La page affichée par défaut lorsque vous accédez au serveur HTTP(s) de ClickHouse.
La valeur par défaut est &quot;Ok.&quot; (avec un caractère de saut de ligne à la fin)

**Exemple**

Ouvre `https://tabix.io/` lors de l’accès à `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

Permet d’ajouter des en-têtes à la réponse d’une requête HTTP `OPTIONS`.
La méthode `OPTIONS` est utilisée lors des requêtes préliminaires CORS.

Pour plus d’informations, voir [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Exemple :

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

Durée d’expiration de HSTS, en secondes.

:::note
Une valeur de `0` signifie que ClickHouse désactive HSTS. Si vous définissez un nombre positif, HSTS sera activé et `max-age` prendra la valeur définie.
:::

**Exemple**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

Restriction des hôtes autorisés à échanger des données entre les serveurs ClickHouse.
Si Keeper est utilisé, la même restriction s&#39;applique à la communication entre différentes instances de Keeper.

:::note
Par défaut, la valeur est identique au paramètre [`listen_host`](#listen_host).
:::

**Exemple**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Type :

Valeur par défaut :

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

Un nom d&#39;utilisateur et un mot de passe utilisés pour se connecter à d&#39;autres serveurs pendant la [réplication](../../engines/table-engines/mergetree-family/replication.md). De plus, le serveur authentifie les autres répliques à l&#39;aide de ces identifiants.
`interserver_http_credentials` doit donc être identique pour toutes les répliques d&#39;un cluster.

:::note

* Par défaut, si la section `interserver_http_credentials` est omise, l&#39;authentification n&#39;est pas utilisée pendant la réplication.
* Les paramètres `interserver_http_credentials` ne sont pas liés à la configuration des identifiants d&#39;un client ClickHouse [configuration](../../interfaces/client.md#configuration_files).
* Ces identifiants sont communs à la réplication via `HTTP` et `HTTPS`.
  :::

Les paramètres suivants peuvent être configurés via des sous-balises :

* `user` — Nom d&#39;utilisateur.
* `password` — Mot de passe.
* `allow_empty` — Si `true`, les autres répliques sont autorisées à se connecter sans authentification même si des identifiants sont définis. Si `false`, les connexions sans authentification sont refusées. Valeur par défaut : `false`.
* `old` — Contient les anciens `user` et `password` utilisés pendant la rotation des identifiants. Plusieurs sections `old` peuvent être spécifiées.

**Rotation des identifiants**

ClickHouse prend en charge la rotation dynamique des identifiants interserveurs sans arrêter toutes les répliques en même temps pour mettre à jour leur configuration. Les identifiants peuvent être modifiés en plusieurs étapes.

Pour activer l&#39;authentification, définissez `interserver_http_credentials.allow_empty` sur `true` et ajoutez des identifiants. Cela autorise les connexions avec ou sans authentification.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

Après avoir configuré toutes les répliques, définissez `allow_empty` sur `false` ou supprimez ce paramètre. Cela rend obligatoire l’authentification avec de nouveaux identifiants.

Pour modifier les identifiants existants, déplacez le nom d’utilisateur et le mot de passe dans la section `interserver_http_credentials.old`, puis mettez à jour `user` et `password` en leur affectant de nouvelles valeurs. À ce stade, le serveur utilise les nouveaux identifiants pour se connecter aux autres répliques et accepte les connexions avec les nouveaux identifiants comme avec les anciens.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

Une fois que les nouveaux identifiants ont été appliqués à toutes les répliques, les anciens peuvent être supprimés.

<div id="ldap_servers">
  ## ldap_servers
</div>

Répertoriez ici les serveurs LDAP avec leurs paramètres de connexion afin de :

* les utiliser comme mécanismes d’authentification pour des utilisateurs locaux dédiés, pour lesquels le mécanisme d’authentification `ldap` est spécifié à la place de `password`
* les utiliser comme répertoires d’utilisateurs distants.

Les paramètres suivants peuvent être configurés via des sous-balises :

| Paramètre                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | Modèle utilisé pour construire le DN à utiliser pour le bind. Le DN résultant sera construit en remplaçant toutes les sous-chaînes `\{user_name\}` du modèle par le nom d’utilisateur réel à chaque tentative d’authentification.                                                                                                                                                                                                                                                                                                           |
| `enable_tls`                   | Indicateur qui active l’utilisation d’une connexion sécurisée au serveur LDAP. Spécifiez `no` pour le protocol en texte brut (`ldap://`) (non recommandé). Spécifiez `yes` pour le protocol LDAP sur SSL/TLS (`ldaps://`) (recommandé, valeur par défaut). Spécifiez `starttls` pour le protocol StartTLS legacy (protocol en texte brut (`ldap://`), ensuite mis à niveau vers TLS).                                                                                                                                                     |
| `host`                         | Nom d’hôte ou IP du serveur LDAP ; ce paramètre est obligatoire et ne peut pas être vide.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `port`                         | Port du serveur LDAP ; la valeur par défaut est 636 si `enable_tls` est défini sur true, `389` sinon.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `tls_ca_cert_dir`              | path vers le répertoire contenant les certificats de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `tls_ca_cert_file`             | path vers le fichier de certificat de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_cert_file`                | path vers le fichier de certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `tls_cipher_suite`             | suite de chiffrement autorisée (en notation OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `tls_key_file`                 | path vers le fichier de clé du certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `tls_minimum_protocol_version` | Version minimale du protocol SSL/TLS. Les valeurs acceptées sont : `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (valeur par défaut).                                                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_require_cert`             | Comportement de la vérification du certificat pair SSL/TLS. Les valeurs acceptées sont : `never`, `allow`, `try`, `demand` (valeur par défaut).                                                                                                                                                                                                                                                                                                                                                                                           |
| `user_dn_detection`            | Section contenant les paramètres de recherche LDAP permettant de détecter le user DN réel de l’utilisateur lié. Cela est principalement utilisé dans les search filters pour un role mapping ultérieur lorsque le serveur est Active Directory. Le user DN résultant sera utilisé lors du remplacement des sous-chaînes `\{user_dn\}` partout où elles sont autorisées. Par défaut, le user DN est défini comme étant égal au bind DN, mais une fois la recherche effectuée, il sera mis à jour avec la valeur réelle du user DN détecté. |
| `verification_cooldown`        | Période, en secondes, après une tentative de bind réussie, pendant laquelle un utilisateur est considéré comme authentifié avec succès pour toutes les requêtes consécutives sans contacter le serveur LDAP. Spécifiez `0` (valeur par défaut) pour désactiver la mise en cache et forcer le contact avec le serveur LDAP pour chaque requête d’authentification.                                                                                                                                                                           |

Le paramètre `user_dn_detection` peut être configuré avec des sous-balises :

| Paramètre       | Description                                                                                                                                                                                                                                                                                                                                                                |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | modèle utilisé pour construire le base DN de la recherche LDAP. Le DN résultant sera construit en remplaçant toutes les sous-chaînes `\{user_name\}` et `\{bind_dn\}` du modèle par le nom d’utilisateur réel et le bind DN pendant la recherche LDAP.                                                                                                                     |
| `scope`         | portée de la recherche LDAP. Les valeurs acceptées sont : `base`, `one_level`, `children`, `subtree` (valeur par défaut).                                                                                                                                                                                                                                                  |
| `search_filter` | modèle utilisé pour construire le search filter pour la recherche LDAP. Le filtre résultant sera construit en remplaçant toutes les sous-chaînes `\{user_name\}`, `\{bind_dn\}` et `\{base_dn\}` du modèle par le nom d’utilisateur réel, le bind DN et le base DN pendant la recherche LDAP. Notez que les caractères spéciaux doivent être correctement échappés en XML. |

Exemple :

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

Exemple (cas typique d’Active Directory avec détection configurée du DN utilisateur en vue d’un mappage de rôles ultérieur) :

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

<div id="listen_host">
  ## listen_host
</div>

Restriction des hôtes dont les requêtes peuvent provenir. Si vous voulez que le serveur réponde à toutes, indiquez `::`.

Exemples :

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

L’emplacement et le format des messages de log.

**Clés** :

| Clé                          | Description                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | Lorsque `true` (par défaut), la journalisation s’effectue de manière asynchrone (un thread d’arrière-plan par canal de sortie). Sinon, elle s’effectue dans le thread qui appelle LOG                                                                                                                                                                                              |
| `async_queue_max_size`       | Lors de l’utilisation de la journalisation asynchrone, nombre maximal de messages conservés dans la file d’attente avant flush. Les messages supplémentaires seront ignorés                                                                                                                                                                                                        |
| `console`                    | Active la journalisation vers la console. Définissez `1` ou `true` pour l’activer. La valeur par défaut est `1` si ClickHouse ne s’exécute pas en mode démon, `0` sinon.                                                                                                                                                                                                           |
| `console_log_level`          | Niveau de log pour la sortie console. La valeur par défaut est `level`.                                                                                                                                                                                                                                                                                                            |
| `console_shutdown_log_level` | Shutdown level est utilisé pour définir le niveau de log de la console lors de l’arrêt du serveur.                                                                                                                                                                                                                                                                                 |
| `console_startup_log_level`  | Startup level est utilisé pour définir le niveau de log de la console au démarrage du serveur. Après le démarrage, le niveau de log revient au paramètre `console_log_level`                                                                                                                                                                                                       |
| `count`                      | Rotation policy : nombre maximal de fichiers de log historiques conservés par ClickHouse.                                                                                                                                                                                                                                                                                          |
| `errorlog`                   | Chemin du fichier de log des erreurs.                                                                                                                                                                                                                                                                                                                                              |
| `formatting.type`            | Log format pour la sortie console. Actuellement, seul `json` est pris en charge                                                                                                                                                                                                                                                                                                    |
| `level`                      | Niveau de log. Valeurs acceptées : `none` (désactive la journalisation), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                                                                                                                                                                                                                 |
| `log`                        | Chemin du fichier de log.                                                                                                                                                                                                                                                                                                                                                          |
| `rotation`                   | Rotation policy : contrôle à quel moment les fichiers de log sont pivotés. La rotation peut être basée sur la taille, le temps, ou une combinaison des deux. Exemples : 100M, daily, 100M,daily. Une fois que le fichier de log dépasse la taille spécifiée ou que l’intervalle de temps spécifié est atteint, il est renommé et archivé, puis un nouveau fichier de log est créé. |
| `shutdown_level`             | Shutdown level est utilisé pour définir le niveau du root logger lors de l’arrêt du serveur.                                                                                                                                                                                                                                                                                       |
| `size`                       | Rotation policy : taille maximale des fichiers de log en octets. Une fois que la taille du fichier de log dépasse ce seuil, il est renommé et archivé, puis un nouveau fichier de log est créé.                                                                                                                                                                                    |
| `startup_level`              | Startup level est utilisé pour définir le niveau du root logger au démarrage du serveur. Après le démarrage, le niveau de log revient au paramètre `level`                                                                                                                                                                                                                         |
| `stream_compress`            | Compresse les messages de log avec LZ4. Définissez `1` ou `true` pour l’activer.                                                                                                                                                                                                                                                                                                   |
| `syslog_level`               | Niveau de log pour la journalisation vers syslog.                                                                                                                                                                                                                                                                                                                                  |
| `use_syslog`                 | Transmet également la sortie de log à syslog.                                                                                                                                                                                                                                                                                                                                      |

**Spécificateurs de format de log**

Les noms de fichiers dans les chemins `log` et `errorLog` prennent en charge les spécificateurs de format ci-dessous pour le nom de fichier résultant (la partie répertoire ne les prend pas en charge).

La colonne « Example » montre la sortie à `2023-07-06 18:32:07`.

| Spécificateur | Description                                                                                                                                                                                               | Exemple                    |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%`          | Caractère % littéral                                                                                                                                                                                      | `%`                        |
| `%n`          | Caractère de nouvelle ligne                                                                                                                                                                               |                            |
| `%t`          | Caractère de tabulation horizontale                                                                                                                                                                       |                            |
| `%Y`          | Année sous forme de nombre décimal, p. ex. 2017                                                                                                                                                           | `2023`                     |
| `%y`          | 2 derniers chiffres de l’année sous forme de nombre décimal (intervalle [00,99])                                                                                                                          | `23`                       |
| `%C`          | 2 premiers chiffres de l’année sous forme de nombre décimal (intervalle [00,99])                                                                                                                          | `20`                       |
| `%G`          | [Année ISO 8601 basée sur les semaines](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) sur quatre chiffres, c.-à-d. l’année qui contient la semaine indiquée. Généralement utile uniquement avec `%V` | `2023`                     |
| `%g`          | 2 derniers chiffres de l’[année ISO 8601 basée sur les semaines](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), c.-à-d. l’année qui contient la semaine indiquée.                                    | `23`                       |
| `%b`          | Nom du mois abrégé, p. ex. Oct (dépend de la locale)                                                                                                                                                      | `Jul`                      |
| `%h`          | Synonyme de %b                                                                                                                                                                                            | `Jul`                      |
| `%B`          | Nom complet du mois, p. ex. October (dépend de la locale)                                                                                                                                                 | `July`                     |
| `%m`          | Mois sous forme de nombre décimal (intervalle [01,12])                                                                                                                                                    | `07`                       |
| `%U`          | Numéro de la semaine dans l’année sous forme de nombre décimal (dimanche = premier jour de la semaine) (intervalle [00,53])                                                                               | `27`                       |
| `%W`          | Numéro de la semaine dans l’année sous forme de nombre décimal (lundi = premier jour de la semaine) (intervalle [00,53])                                                                                  | `27`                       |
| `%V`          | Numéro de semaine ISO 8601 (intervalle [01,53])                                                                                                                                                           | `27`                       |
| `%j`          | Jour de l’année sous forme de nombre décimal (intervalle [001,366])                                                                                                                                       | `187`                      |
| `%d`          | Jour du mois sous forme de nombre décimal avec ajout d’un zéro initial (intervalle [01,31]). Un seul chiffre est précédé d’un zéro.                                                                       | `06`                       |
| `%e`          | Jour du mois sous forme de nombre décimal avec ajout d’une espace initiale (intervalle [1,31]). Un seul chiffre est précédé d’une espace.                                                                 | `&nbsp; 6`                 |
| `%a`          | Nom abrégé du jour de la semaine, p. ex. Fri (dépend de la locale)                                                                                                                                        | `Thu`                      |
| `%A`          | Nom complet du jour de la semaine, p. ex. Friday (dépend de la locale)                                                                                                                                    | `Thursday`                 |
| `%w`          | Jour de la semaine sous forme d’entier, avec dimanche = 0 (intervalle [0-6])                                                                                                                              | `4`                        |
| `%u`          | Jour de la semaine sous forme de nombre décimal, où lundi = 1 (format ISO 8601) (intervalle [1-7])                                                                                                        | `4`                        |
| `%H`          | Heure sous forme de nombre décimal, au format 24 heures (intervalle [00-23])                                                                                                                              | `18`                       |
| `%I`          | Heure sous forme de nombre décimal, au format 12 heures (intervalle [01,12])                                                                                                                              | `06`                       |
| `%M`          | Minute sous forme de nombre décimal (intervalle [00,59])                                                                                                                                                  | `32`                       |
| `%S`          | Seconde sous forme de nombre décimal (intervalle [00,60])                                                                                                                                                 | `07`                       |
| `%c`          | Chaîne standard de date et d’heure, p. ex. Sun Oct 17 04:41:13 2010 (dépend de la locale)                                                                                                                 | `Thu Jul  6 18:32:07 2023` |
| `%x`          | Représentation localisée de la date (dépend de la locale)                                                                                                                                                 | `07/06/23`                 |
| `%X`          | Représentation localisée de l’heure, p. ex. 18:40:20 ou 6:40:20 PM (dépend de la locale)                                                                                                                  | `18:32:07`                 |
| `%D`          | Date courte au format MM/DD/YY, équivalente à %m/%d/%y                                                                                                                                                    | `07/06/23`                 |
| `%F`          | Date courte au format AAAA-MM-JJ, équivalent à %Y-%m-%d                                                                                                                                                   | `2023-07-06`               |
| `%r`          | Heure localisée au format 12 heures (selon les paramètres régionaux)                                                                                                                                      | `06:32:07 PM`              |
| `%R`          | Équivalent à &quot;%H:%M&quot;                                                                                                                                                                            | `18:32`                    |
| `%T`          | Équivalent à &quot;%H:%M:%S&quot; (format d’heure ISO 8601)                                                                                                                                               | `18:32:07`                 |
| `%p`          | Indicateur localisé a.m. ou p.m. (selon les paramètres régionaux)                                                                                                                                         | `PM`                       |
| `%z`          | Décalage par rapport à UTC au format ISO 8601 (par ex. -0430), ou aucun caractère si les informations de fuseau horaire ne sont pas disponibles                                                           | `+0800`                    |
| `%Z`          | Nom ou abréviation du fuseau horaire selon les paramètres régionaux, ou aucun caractère si les informations de fuseau horaire ne sont pas disponibles                                                     | `Z AWST `                  |

**Exemple**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

Pour afficher uniquement les messages de journalisation dans la console :

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Surcharges par niveau**

Le niveau de journalisation de chaque logger peut être surchargé. Par exemple, pour désactiver tous les messages des loggers &quot;Backup&quot; et &quot;RBAC&quot;.

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

Pour envoyer également les messages de journal à syslog :

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Clés pour `<syslog>` :

| Key        | Description                                                                                                                                                                                                                                                                             |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | L’adresse du syslog, au format `host\[:port\]`. Si elle est omise, le démon local est utilisé.                                                                                                                                                                                          |
| `hostname` | Le nom d’hôte depuis lequel les logs sont envoyés (facultatif).                                                                                                                                                                                                                         |
| `facility` | Le [mot-clé de facility](https://en.wikipedia.org/wiki/Syslog#Facility) du syslog. Il doit être indiqué en majuscules avec le préfixe &quot;LOG&#95;&quot;, par ex. `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, etc. Par défaut : `LOG_USER` si `address` est spécifié, sinon `LOG_DAEMON`. |
| `format`   | Format du message de log. Valeurs possibles : `bsd` et `syslog.`                                                                                                                                                                                                                        |

**Formats de log**

Vous pouvez spécifier le format de log à afficher dans le journal de console. Actuellement, seul le format JSON est pris en charge.

**Exemple**

Voici un exemple de log JSON en sortie :

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

Pour activer la journalisation au format JSON, utilisez l’extrait suivant :

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Renommer des clés dans les logs JSON**

Vous pouvez modifier les noms des clés en changeant les valeurs des balises à l’intérieur de la balise `<names>`. Par exemple, pour remplacer `DATE_TIME` par `MY_DATE_TIME`, vous pouvez utiliser `<date_time>MY_DATE_TIME</date_time>`.

**Omettre des clés dans les logs JSON**

Vous pouvez omettre des propriétés de log en les commentant. Par exemple, si vous ne souhaitez pas que votre log affiche `query_id`, vous pouvez commenter la balise `<query_id>`.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

Paramètres d’envoi des rapports de plantage à l’équipe des développeurs principaux de ClickHouse.

L’activer, en particulier dans les environnements de préproduction, est vivement apprécié.

Clés :

| Key                   | Description                                                                                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`             | Indicateur booléen permettant d’activer la fonctionnalité, `true` par défaut. Définissez `false` pour éviter l’envoi de rapports de plantage.                     |
| `endpoint`            | Vous pouvez redéfinir l’URL du point de terminaison pour l’envoi des rapports de plantage.                                                                        |
| `send_logical_errors` | `LOGICAL_ERROR` est comparable à un `assert` ; il s’agit d’un bug dans ClickHouse. Cet indicateur booléen active l’envoi de ces exceptions (par défaut : `true`). |

**Utilisation recommandée**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

La partie publique de la clé d’hôte sera enregistrée dans le fichier known&#95;hosts
côté client SSH lors de la première connexion.

Les configurations des clés d’hôte sont inactives par défaut.
Décommentez les configurations des clés d’hôte et indiquez le chemin vers la clé SSH correspondante pour les activer :

Exemple :

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

Port du serveur SSH permettant à l’utilisateur de se connecter et d’exécuter des requêtes de manière interactive à l’aide du client intégré via le PTY.

Exemple :

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

Permet de configurer le stockage sur plusieurs disques.

La configuration du stockage suit la structure suivante :

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### Configuration de `disks`
</div>

La configuration de `disks` suit la structure ci-dessous :

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

Les sous-balises ci-dessus définissent les paramètres suivants pour `disks` :

| Paramètre               | Description                                                                                                    |
| ----------------------- | -------------------------------------------------------------------------------------------------------------- |
| `<disk_name_N>`         | Nom du disque, qui doit être unique.                                                                           |
| `path`                  | Chemin où seront stockées les données du serveur (répertoires `data` et `shadow`). Il doit se terminer par `/` |
| `keep_free_space_bytes` | Taille de l’espace disque réservé.                                                                             |

:::note
L’ordre des disques n’a pas d’importance.
:::

<div id="configuration-of-policies">
  ### Configuration des politiques
</div>

Les sous-balises ci-dessus définissent les paramètres suivants pour `policies` :

| Setting                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | Nom de la politique. Les noms de politique doivent être uniques.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `volume_name_N`              | Nom du volume. Les noms de volume doivent être uniques.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `disk`                       | Le disque situé à l’intérieur du volume.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `max_data_part_size_bytes`   | Taille maximale d’un fragment de données pouvant être stocké sur l’un des disques de ce volume. Si une fusion produit un fragment dont la taille prévue dépasse max&#95;data&#95;part&#95;size&#95;bytes, ce fragment sera écrit sur le volume suivant. En pratique, cette fonctionnalité permet de stocker les fragments nouveaux / de petite taille sur un volume rapide (SSD), puis de les déplacer vers un volume lent (HDD) lorsqu’ils deviennent volumineux. N’utilisez pas cette option si la politique ne comporte qu’un seul volume.                                                                               |
| `move_factor`                | La part d’espace libre disponible sur le volume. Si l’espace passe en dessous de ce seuil, les données commenceront à être transférées vers le volume suivant, s’il y en a un. Lors du transfert, les fragments sont triés par taille, du plus grand au plus petit (ordre décroissant), et les fragments dont la taille cumulée suffit à satisfaire la condition `move_factor` sont sélectionnés. Si la taille totale de tous les fragments est insuffisante, tous les fragments seront déplacés.                                                                                                                           |
| `perform_ttl_move_on_insert` | Désactive le déplacement, lors de l’insertion, des données dont le TTL a expiré. Par défaut (si cette option est activée), si l’on insère une portion de données déjà expirée selon la règle de déplacement liée à la durée de vie, elle est immédiatement déplacée vers le volume / disque spécifié dans cette règle. Cela peut considérablement ralentir l’insertion si le volume / disque cible est lent (par exemple S3). Si cette option est désactivée, la partie expirée des données est écrite sur le volume par défaut, puis immédiatement déplacée vers le volume spécifié par la règle applicable au TTL expiré. |
| `load_balancing`             | Politique d’équilibrage des disques, `round_robin` ou `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `least_used_ttl_ms`          | Définit le délai d’expiration (en millisecondes) pour la mise à jour de l’espace disponible sur tous les disques (`0` - toujours mettre à jour, `-1` - ne jamais mettre à jour, la valeur par défaut est `60000`). Remarque : si le disque est utilisé uniquement par ClickHouse et ne fera pas l’objet d’un redimensionnement du système de fichiers à chaud, vous pouvez utiliser la valeur `-1`. Dans tous les autres cas, cela n’est pas recommandé, car cela finira par entraîner une allocation incorrecte de l’espace.                                                                                               |
| `prefer_not_to_merge`        | Désactive la fusion des parties de données sur ce volume. Remarque : ce paramètre peut être nuisible et entraîner un ralentissement. Lorsqu’il est activé (ce que nous déconseillons), la fusion des données sur ce volume est interdite. Cela permet de contrôler la manière dont ClickHouse interagit avec les disques lents. Nous recommandons de ne pas l’utiliser du tout.                                                                                                                                                                                                                                             |
| `volume_priority`            | Définit la priorité (ordre) selon laquelle les volumes sont remplis. Plus la valeur est petite, plus la priorité est élevée. Les valeurs de ce paramètre doivent être des nombres naturels et couvrir l’intervalle de 1 à N (N étant la plus grande valeur spécifiée) sans lacunes.                                                                                                                                                                                                                                                                                                                                         |

Pour `volume_priority` :

* Si tous les volumes ont ce paramètre, ils sont prioritaires dans l’ordre spécifié.
* Si seuls *certains* volumes l’ont, les volumes qui ne l’ont pas ont la priorité la plus faible. Ceux qui l’ont sont priorisés selon la valeur de la balise, tandis que la priorité des autres est déterminée, entre eux, par leur ordre de description dans le fichier de configuration.
* Si *aucun* volume ne reçoit ce paramètre, leur ordre est déterminé par l’ordre de leur description dans le fichier de configuration.
* La priorité des volumes ne peut pas être identique.

<div id="macros">
  ## macros
</div>

Substitutions de paramètres pour les tables répliquées.

Peut être omis si les tables répliquées ne sont pas utilisées.

Pour en savoir plus, voir la section [Création de tables répliquées](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Exemple**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Nom du groupe de répliques pour la base de données Replicated.

Le cluster créé par la base de données Replicated sera composé des répliques du même groupe.
Les requêtes DDL n’attendront que les répliques du même groupe.

Vide par défaut.

**Exemple**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

Délai d’expiration maximal de la session, en secondes.

Exemple :

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

Réglages précis pour les tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Pour plus d’informations, consultez le fichier d’en-tête MergeTreeSettings.h.

**Exemple**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

Il est désactivé par défaut.

**Activation**

Pour activer manuellement la collecte de l’historique des métriques [`system.metric_log`](../../operations/system-tables/metric_log.md), créez `/etc/clickhouse-server/config.d/metric_log.xml` avec le contenu suivant :

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Désactivation**

Pour désactiver le paramètre `metric_log`, créez le fichier suivant `/etc/clickhouse-server/config.d/disable_metric_log.xml` avec le contenu ci-dessous :

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

Paramètres de réglage fin pour les tables [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Ce paramètre est prioritaire.

Pour plus d’informations, consultez le fichier d’en-tête MergeTreeSettings.h.

**Exemple**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

Paramètres de la table système [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md).

<SystemLogParameters />

Exemple :

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

Configuration SSL côté client/serveur.

La prise en charge de SSL est assurée par la bibliothèque `libpoco`. Les options de configuration disponibles sont décrites dans [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Les valeurs par défaut se trouvent dans [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Clés de configuration client/serveur :

| Option                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Valeur par défaut                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | Active ou désactive la mise en cache des sessions. Doit être utilisé avec `sessionIdContext`. Valeurs acceptées : `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                    |
| `caConfig`                    | Chemin d’accès vers le fichier ou le répertoire contenant les certificats d’autorité de certification (CA) de confiance. Si ce chemin désigne un fichier, celui-ci doit être au format PEM et peut contenir plusieurs certificats de CA. Si ce chemin désigne un répertoire, il doit contenir un fichier .pem par certificat de CA. Les noms de fichiers sont recherchés à partir de la valeur de hachage du nom du sujet de la CA. Des détails sont disponibles dans la page de manuel de [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                                                                            |
| `certificateFile`             | Chemin du fichier de certificat client/serveur au format PEM. Vous pouvez l’omettre si `privateKeyFile` contient le certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |                                                                                            |
| `cipherList`                  | Suites de chiffrement OpenSSL prises en charge.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | Protocoles dont l’utilisation est interdite.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |                                                                                            |
| `extendedVerification`        | Si cette option est activée, vérifie que le CN ou le SAN du certificat correspond au nom d’hôte du pair.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `false`                                                                                    |
| `fips`                        | Active le mode FIPS d’OpenSSL. Pris en charge si la version d’OpenSSL de la bibliothèque prend en charge FIPS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `false`                                                                                    |
| `invalidCertificateHandler`   | Classe (sous-classe de CertificateHandler) permettant de vérifier les certificats invalides. Par exemple : `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                                                                                                                                                            | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | Indique si les certificats CA intégrés à OpenSSL doivent être utilisés. ClickHouse suppose que les certificats CA intégrés se trouvent dans le fichier `/etc/ssl/cert.pem` (resp. le répertoire `/etc/ssl/certs`) ou dans le fichier (resp. le répertoire) spécifié par la variable d’environnement `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).                                                                                                                                                                                                                                                                              | `true`                                                                                     |
| `preferServerCiphers`         | Suites de chiffrement du serveur préférées par le client.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `false`                                                                                    |
| `privateKeyFile`              | Chemin vers le fichier contenant la clé privée du certificat PEM. Le fichier peut contenir à la fois la clé et le certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |                                                                                            |
| `privateKeyPassphraseHandler` | Classe (sous-classe de PrivateKeyPassphraseHandler) qui demande la phrase secrète nécessaire pour accéder à la clé privée. Par exemple : `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                                                                                                                                               | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | Exige une connexion TLSv1. Valeurs acceptées : `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `false`                                                                                    |
| `requireTLSv1_1`              | Nécessite une connexion TLSv1.1. Valeurs acceptées : `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `requireTLSv1_2`              | Nécessite une connexion TLSv1.2. Valeurs acceptées : `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `sessionCacheSize`            | Le nombre maximal de sessions que le serveur conserve en cache. Une valeur de `0` signifie un nombre illimité de sessions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | Une suite unique de caractères aléatoires que le serveur ajoute à chaque identifiant généré. La longueur de la chaîne ne doit pas dépasser `SSL_MAX_SSL_SESSION_ID_LENGTH`. Ce paramètre est toujours recommandé, car il permet d’éviter les problèmes, aussi bien lorsque le serveur met la session en cache que lorsque le client a demandé la mise en cache.                                                                                                                                                                                                                                                          | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | Durée de mise en cache de la session sur le serveur, en heures.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `2`                                                                                        |
| `verificationDepth`           | Longueur maximale de la chaîne de vérification. La vérification échoue si la longueur de la chaîne de certificats dépasse la valeur définie.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `9`                                                                                        |
| `verificationMode`            | La méthode utilisée pour vérifier les certificats du nœud. Plus de détails dans la description de la classe [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Valeurs possibles : `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                                                                                                                                                         | `relaxed`                                                                                  |

**Exemple de paramètres :**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

Événements consignés associés à [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Par exemple, l’ajout ou la fusion de données. Vous pouvez utiliser ce journal pour simuler des algorithmes de fusion et comparer leurs caractéristiques. Vous pouvez visualiser le processus de fusion.

Les requêtes sont consignées dans la table [system.part&#95;log](/fr/operations/system-tables/part_log), et non dans un fichier distinct. Vous pouvez configurer le nom de cette table dans le paramètre `table` (voir ci-dessous).

<SystemLogParameters />

**Exemple**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

Paramètres de la table système [`processors_profile_log`](../system-tables/processors_profile_log.md).

<SystemLogParameters />

Les paramètres par défaut sont :

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

Expose les données de métriques pour le scraping par [Prometheus](https://prometheus.io).

Paramètres :

* `endpoint` – endpoint HTTP de scraping des métriques par le serveur Prometheus. Doit commencer par &#39;/&#39;.
* `port` – Port de l’`endpoint`.
* `metrics` – Expose les métriques de la table [system.metrics](/fr/operations/system-tables/metrics).
* `events` – Expose les métriques de la table [system.events](/fr/operations/system-tables/events).
* `asynchronous_metrics` – Expose les valeurs actuelles des métriques de la table [system.asynchronous&#95;metrics](/fr/operations/system-tables/asynchronous_metrics).
* `errors` - Expose le nombre d’erreurs par code d’erreur survenues depuis le dernier redémarrage du serveur. Ces informations peuvent également être obtenues depuis [system.errors](/fr/operations/system-tables/errors).

**Exemple**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

Vérifiez (remplacez `127.0.0.1` par l’adresse IP ou le nom d’hôte de votre serveur ClickHouse) :

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

Paramètre de journalisation des requêtes reçues avec le réglage [log&#95;queries=1](../../operations/settings/settings.md).

Les requêtes sont consignées dans la table [system.query&#95;log](/fr/operations/system-tables/query_log), et non dans un fichier distinct. Vous pouvez modifier le nom de la table via le paramètre `table` (voir ci-dessous).

<SystemLogParameters />

Si la table n&#39;existe pas, ClickHouse la crée. Si la structure du journal des requêtes a changé lors de la mise à jour du serveur ClickHouse, la table utilisant l&#39;ancienne structure est renommée, et une nouvelle table est créée automatiquement.

**Exemple**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

Il est désactivé par défaut.

**Activation**

Pour activer manuellement la collecte de l’historique des métriques [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), créez `/etc/clickhouse-server/config.d/query_metric_log.xml` avec le contenu suivant :

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Désactivation**

Pour désactiver le paramètre `query_metric_log`, créez le fichier suivant `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` avec le contenu ci-dessous :

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

Configuration du [Query cache](../query-cache.md).

Les paramètres suivants sont disponibles :

| Paramètre                 | Description                                                                                                              | Valeur par défaut |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------- |
| `max_entries`             | Le nombre maximal de résultats de requêtes `SELECT` stockés dans le cache.                                               | `1024`            |
| `max_entry_size_in_bytes` | La taille maximale, en octets, des résultats de requêtes `SELECT` pouvant être enregistrés dans le cache.                | `1048576`         |
| `max_entry_size_in_rows`  | Le nombre maximal de lignes que peuvent contenir les résultats de requêtes `SELECT` pour être enregistrés dans le cache. | `30000000`        |
| `max_size_in_bytes`       | La taille maximale du cache, en octets. `0` signifie que le query cache est désactivé.                                   | `1073741824`      |

:::note

* Les paramètres modifiés prennent effet immédiatement.
* Les données du query cache sont allouées en DRAM. Si la mémoire est limitée, veillez à définir une faible valeur pour `max_size_in_bytes` ou à désactiver complètement le query cache.
  :::

**Exemple**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

Paramètre de journalisation des threads des requêtes reçues avec le réglage [log&#95;query&#95;threads=1](/fr/operations/settings/settings#log_query_threads).

Les requêtes sont consignées dans la table [system.query&#95;thread&#95;log](/fr/operations/system-tables/query_thread_log), et non dans un fichier distinct. Vous pouvez modifier le nom de la table via le paramètre `table` (voir ci-dessous).

<SystemLogParameters />

Si la table n&#39;existe pas, ClickHouse la crée. Si la structure du journal des threads de requête a changé lors de la mise à jour du serveur ClickHouse, la table utilisant l&#39;ancienne structure est renommée et une nouvelle table est créée automatiquement.

**Exemple**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

Paramètre de journalisation des vues (directes, matérialisées, etc.) en fonction des requêtes reçues avec le paramètre [log&#95;query&#95;views=1](/fr/operations/settings/settings#log_query_views).

Les requêtes sont consignées dans la table [system.query&#95;views&#95;log](/fr/operations/system-tables/query_views_log), et non dans un fichier distinct. Vous pouvez modifier le nom de la table via le paramètre `table` (voir ci-dessous).

<SystemLogParameters />

Si la table n&#39;existe pas, ClickHouse la créera. Si la structure du journal des vues de requêtes a changé lors de la mise à jour du serveur ClickHouse, la table ayant l&#39;ancienne structure est renommée et une nouvelle table est créée automatiquement.

**Exemple**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

Paramètres de la table système [text&#95;log](/fr/operations/system-tables/text_log) pour la journalisation des messages texte.

<SystemLogParameters />

De plus :

| Paramètre | Description                                                                     | Valeur par défaut |
| --------- | ------------------------------------------------------------------------------- | ----------------- |
| `level`   | Niveau maximal des messages (par défaut `Trace`) qui sera stocké dans la table. | `Trace`           |

**Exemple**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

Paramètres de l’opération de la table système [trace&#95;log](/fr/operations/system-tables/trace_log).

<SystemLogParameters />

Le fichier de configuration du serveur par défaut `config.xml` contient la section de configuration suivante :

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

Paramètres de la [table système asynchronous&#95;insert&#95;log](/fr/operations/system-tables/asynchronous_insert_log), utilisée pour journaliser les insertions asynchrones.

<SystemLogParameters />

**Exemple**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

Paramètres de la table système [crash&#95;log](../../operations/system-tables/crash_log.md).

Les paramètres suivants peuvent être configurés via des sous-balises :

| Setting                            | Description                                                                                                                                                     | Default             | Note                                                                                                                                    |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | Seuil du nombre de lignes. Si ce seuil est atteint, l&#39;écriture des logs sur le disque est lancée en arrière-plan.                                           | `max_size_rows / 2` |                                                                                                                                         |
| `database`                         | Nom de la base de données.                                                                                                                                      |                     |                                                                                                                                         |
| `engine`                           | [Définition du moteur MergeTree](/fr/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) pour une table système.             |                     | Ne peut pas être utilisé si `partition_by` ou `order_by` est défini. Si rien n&#39;est spécifié, `MergeTree` est sélectionné par défaut |
| `flush_interval_milliseconds`      | Intervalle de vidage des données du tampon en mémoire vers la table.                                                                                            | `7500`              |                                                                                                                                         |
| `flush_on_crash`                   | Indique si les logs doivent être écrits sur le disque en cas de crash.                                                                                          | `false`             |                                                                                                                                         |
| `max_size_rows`                    | Taille maximale des logs en nombre de lignes. Lorsque le nombre de logs non vidés atteint `max_size`, les logs sont écrits sur le disque.                       | `1024`              |                                                                                                                                         |
| `order_by`                         | [Clé de tri personnalisée](/fr/engines/table-engines/mergetree-family/mergetree#order_by) pour une table système. Ne peut pas être utilisé si `engine` est défini. |                     | Si `engine` est spécifié pour la table système, le paramètre `order_by` doit être indiqué directement dans &#39;engine&#39;             |
| `partition_by`                     | [Clé de partitionnement personnalisée](/fr/engines/table-engines/mergetree-family/custom-partitioning-key.md) pour une table système.                              |                     | Si `engine` est spécifié pour la table système, le paramètre `partition_by` doit être indiqué directement dans &#39;engine&#39;         |
| `reserved_size_rows`               | Taille de mémoire préallouée pour les logs, en nombre de lignes.                                                                                                | `1024`              |                                                                                                                                         |
| `settings`                         | [Paramètres supplémentaires](/fr/engines/table-engines/mergetree-family/mergetree/#settings) qui contrôlent le comportement de MergeTree (facultatif).             |                     | Si `engine` est spécifié pour la table système, le paramètre `settings` doit être indiqué directement dans &#39;engine&#39;             |
| `storage_policy`                   | Nom de la politique de stockage à utiliser pour la table (facultatif).                                                                                          |                     | Si `engine` est spécifié pour la table système, le paramètre `storage_policy` doit être indiqué directement dans &#39;engine&#39;       |
| `table`                            | Nom de la table système.                                                                                                                                        |                     |                                                                                                                                         |
| `ttl`                              | Spécifie le [TTL](/fr/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) de la table.                                                    |                     | Si `engine` est spécifié pour la table système, le paramètre `ttl` doit être indiqué directement dans &#39;engine&#39;                  |

Le fichier de configuration du serveur par défaut `config.xml` contient la section de paramètres suivante :

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

Ce paramètre spécifie le chemin du cache pour les disques personnalisés avec cache (créés depuis SQL).
`custom_cached_disks_base_directory` a une priorité plus élevée pour les disques personnalisés que `filesystem_caches_path` (situé dans `filesystem_caches_path.xml`),
qui est utilisé si le premier est absent.
Le chemin du paramètre de cache du système de fichiers doit se trouver dans ce répertoire,
sinon une exception sera levée, empêchant la création du disque.

:::note
Cela n&#39;affectera pas les disques créés sur une ancienne version pour lesquels le serveur a été mis à niveau.
Dans ce cas, aucune exception ne sera levée, afin de permettre au serveur de démarrer correctement.
:::

Exemple :

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

Paramètres de la [table système backup&#95;log](../../operations/system-tables/backup_log.md) pour la journalisation des opérations `BACKUP` et `RESTORE`.

<SystemLogParameters />

**Exemple**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

Paramètres de la table système [`blob_storage_log`](../system-tables/blob_storage_log.md).

<SystemLogParameters />

Exemple :

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

Règles basées sur des expressions régulières, appliquées aux requêtes ainsi qu&#39;à tous les messages de log avant leur stockage dans les logs du serveur,
les tables [`system.query_log`](/fr/operations/system-tables/query_log), [`system.text_log`](/fr/operations/system-tables/text_log), [`system.processes`](/fr/operations/system-tables/processes), et dans les logs envoyés au client. Cela permet d&#39;éviter
la fuite de données sensibles issues des requêtes SQL, comme des noms, des e-mails, des identifiants personnels ou des numéros de carte bancaire, dans les logs.

**Exemple**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Champs de configuration**:

| Setting   | Description                                                                                |
| --------- | ------------------------------------------------------------------------------------------ |
| `name`    | nom de la règle (facultatif)                                                               |
| `regexp`  | expression régulière compatible RE2 (obligatoire)                                          |
| `replace` | chaîne de substitution pour les données sensibles (facultatif, six astérisques par défaut) |

Les règles de masquage s’appliquent à l’intégralité de la requête (afin d’éviter les fuites de données sensibles provenant de requêtes malformées / non analysables).

La table [`system.events`](/fr/operations/system-tables/events) comporte le compteur `QueryMaskingRulesMatch`, qui indique le nombre total de correspondances aux règles de masquage des requêtes.

Pour les requêtes distribuées, chaque serveur doit être configuré séparément ; sinon, les sous-requêtes transmises à d’autres
nœuds seront stockées sans masquage.

<div id="remote_servers">
  ## remote_servers
</div>

Configuration des clusters utilisés par le moteur de table [Distributed](../../engines/table-engines/special/distributed.md) ainsi que par la fonction de table `cluster`.

**Exemple**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

Pour la valeur de l’attribut `incl`, reportez-vous à la section &quot;[Fichiers de configuration](/fr/operations/configuration-files)&quot;.

**Voir aussi**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [Cluster Discovery](../../operations/cluster-discovery.md)
* [moteur de base de données Replicated](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

Liste des hôtes autorisés dans les moteurs de stockage liés aux URL et les fonctions de table.

Lors de l&#39;ajout d&#39;un hôte avec la balise XML `\<host\>` :

* il doit être indiqué exactement comme dans l&#39;URL, car le nom est vérifié avant la résolution DNS. Par exemple : `<host>clickhouse.com</host>`
* si le port est explicitement indiqué dans l&#39;URL, alors `host:port` est vérifié dans son ensemble. Par exemple : `<host>clickhouse.com:80</host>`
* si l&#39;hôte est indiqué sans port, alors n&#39;importe quel port de cet hôte est autorisé. Par exemple : si `<host>clickhouse.com</host>` est indiqué, alors `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS), etc. sont autorisés.
* si l&#39;hôte est indiqué sous forme d&#39;adresse IP, alors il est vérifié tel qu&#39;il apparaît dans l&#39;URL. Par exemple : `[2a02:6b8:a::a]`.
* s&#39;il y a des redirections et que leur prise en charge est activée, alors chaque redirection (le champ `location`) est vérifiée.

Par exemple :

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

Le fuseau horaire du serveur.

Il est spécifié sous la forme d’un identifiant IANA du fuseau horaire UTC ou d’un lieu géographique (par exemple, Africa/Abidjan).

Le fuseau horaire est nécessaire pour les conversions entre les formats String et DateTime lorsque des champs DateTime sont affichés au format texte (à l’écran ou dans un fichier), ainsi que lors de la conversion d’une chaîne en DateTime. En outre, il est utilisé par les fonctions qui manipulent la date et l’heure si aucun fuseau horaire ne leur a été fourni dans les paramètres d’entrée.

**Exemple**

```xml
<timezone>Asia/Istanbul</timezone>
```

**Voir aussi**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

Port de communication avec les clients via le protocole TCP.

**Exemple**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

Port TCP pour les communications sécurisées avec les clients. À utiliser avec les paramètres [OpenSSL](#openssl).

**Valeur par défaut**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

Port utilisé pour communiquer avec les clients via le protocole MySQL.

:::note

* Les entiers positifs indiquent le numéro du port sur lequel écouter
* Les valeurs vides servent à désactiver la communication avec les clients via le protocole MySQL.
  :::

**Exemple**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

Port utilisé pour communiquer avec les clients via le protocole PostgreSQL.

:::note

* Les entiers positifs indiquent le numéro du port sur lequel écouter
* Les valeurs vides servent à désactiver la communication avec les clients via le protocole PostgreSQL.
  :::

**Exemple**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

Configuration permettant de convertir des préfixes d’URL abrégés ou symboliques en URL complètes.

Exemple :

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

Le répertoire contenant les fichiers définis par l’utilisateur. Utilisé pour les fonctions définies par l’utilisateur en SQL [Fonctions SQL définies par l’utilisateur](/fr/sql-reference/functions/udf).

**Exemple**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

Chemin d’accès vers le fichier qui contient :

* Les configurations utilisateur.
* Les droits d’accès.
* Les profils de paramètres.
* Les paramètres de quota.

**Exemple**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

Paramètres des améliorations facultatives du système de contrôle d’accès.

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Default |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | Définit si les requêtes `ON CLUSTER` nécessitent le privilège `CLUSTER`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `true`  |
| `role_cache_expiration_time_seconds`            | Définit le nombre de secondes après le dernier accès pendant lesquelles un rôle est conservé dans le cache des rôles.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `600`   |
| `select_from_information_schema_requires_grant` | Définit si `SELECT * FROM information_schema.<table>` nécessite des privilèges ou peut être exécuté par n’importe quel utilisateur. Si cette option est définie sur `true`, cette requête nécessite `GRANT SELECT ON information_schema.<table>`, comme pour les tables ordinaires.                                                                                                                                                                                                                                                                                                       | `true`  |
| `select_from_system_db_requires_grant`          | Définit si `SELECT * FROM system.<table>` nécessite des privilèges ou peut être exécuté par n’importe quel utilisateur. Si cette option est définie sur `true`, cette requête nécessite `GRANT SELECT ON system.<table>`, comme pour les tables non système. Exceptions : quelques tables système (`tables`, `columns`, `databases`, ainsi que certaines tables constantes comme `one` et `contributors`) restent accessibles à tous ; et si un privilège `SHOW` (par exemple `SHOW USERS`) a été accordé, la table système correspondante (c’est-à-dire `system.users`) sera accessible. | `true`  |
| `settings_constraints_replace_previous`         | Définit si une contrainte d’un profil de paramètres pour un paramètre donné annule les effets de la contrainte précédente (définie dans d’autres profils) pour ce paramètre, y compris pour les champs non définis par la nouvelle contrainte. Cela active également le type de contrainte `changeable_in_readonly`.                                                                                                                                                                                                                                                                      | `true`  |
| `table_engines_require_grant`                   | Définit si la création d’une table avec un moteur de table spécifique nécessite un privilège.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `false` |
| `throw_on_unmatched_row_policies`               | Définit si la lecture d’une table doit lever une exception lorsque la table possède des politiques de lignes, mais qu’aucune ne s’applique à l’utilisateur courant                                                                                                                                                                                                                                                                                                                                                                                                                        | `false` |
| `users_without_row_policies_can_read_rows`      | Définit si les utilisateurs sans politiques de lignes permissives peuvent tout de même lire des lignes à l’aide d’une requête `SELECT`. Par exemple, s’il y a deux utilisateurs, A et B, et qu’une politique de lignes est définie uniquement pour A, alors si ce paramètre vaut `true`, l’utilisateur B verra toutes les lignes. Si ce paramètre vaut `false`, l’utilisateur B ne verra aucune ligne.                                                                                                                                                                                    | `true`  |

Exemple :

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

Paramètres de la table système `s3queue_log`.

<SystemLogParameters />

Les paramètres par défaut sont :

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

Paramètre de la table système &#39;dead&#95;letter&#95;queue&#39;.

<SystemLogParameters />

Les paramètres par défaut sont :

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

Contient des paramètres qui permettent à ClickHouse d’interagir avec un cluster [ZooKeeper](http://zookeeper.apache.org/). ClickHouse utilise ZooKeeper pour stocker les métadonnées des répliques lors de l’utilisation de tables répliquées. Si les tables répliquées ne sont pas utilisées, cette section de paramètres peut être omise.

Les paramètres suivants peuvent être configurés via des sous-balises :

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | Point de terminaison ZooKeeper. Vous pouvez définir plusieurs points de terminaison. Par ex. `<node index="1"><host>example_host</host><port>2181</port></node>`. L’attribut `index` spécifie l’ordre des nœuds lors des tentatives de connexion au cluster ZooKeeper.                                                                                                                       |
| `operation_timeout_ms`                          | Délai d’expiration maximal pour une opération, en millisecondes.                                                                                                                                                                                                                                                                                                                             |
| `session_timeout_ms`                            | Délai d’expiration maximal pour la session du client, en millisecondes.                                                                                                                                                                                                                                                                                                                      |
| `root` (optional)                               | Le znode utilisé comme racine pour les znodes utilisés par le ClickHouse server.                                                                                                                                                                                                                                                                                                             |
| `fallback_session_lifetime.min` (optional)      | Limite minimale de durée de vie d’une session ZooKeeper vers le nœud de secours lorsque le nœud primaire n’est pas disponible (équilibrage de charge). Définie en secondes. Par défaut : 3 heures.                                                                                                                                                                                           |
| `fallback_session_lifetime.max` (optional)      | Limite maximale de durée de vie d’une session ZooKeeper vers le nœud de secours lorsque le nœud primaire n’est pas disponible (équilibrage de charge). Définie en secondes. Par défaut : 6 heures.                                                                                                                                                                                           |
| `identity` (optional)                           | Nom d’utilisateur et mot de passe requis par ZooKeeper pour accéder aux znodes demandés.                                                                                                                                                                                                                                                                                                     |
| `use_compression` (optional)                    | Active la compression dans le protocole Keeper si défini sur `true`.                                                                                                                                                                                                                                                                                                                         |
| `use_xid_64` (optional)                         | Active les identifiants de transaction sur 64 bits. Définissez `true` pour activer le format étendu d’identifiant de transaction. Par défaut : `false`.                                                                                                                                                                                                                                      |
| `pass_opentelemetry_tracing_context` (optional) | Active la propagation du contexte de tracing OpenTelemetry vers les requêtes Keeper. Lorsqu’elle est activée, des spans de tracing sont créés pour les opérations Keeper, ce qui permet un traçage distribué entre ClickHouse et Keeper. Voir [Tracing ClickHouse Keeper Requests](/fr/operations/opentelemetry#tracing-clickhouse-keeper-requests) pour plus de détails. Par défaut : `false`. |

Il existe également le paramètre `zookeeper_load_balancing` (facultatif), qui permet de sélectionner l’algorithme de sélection des nœuds ZooKeeper :

| Algorithm Name                   | Description                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `random`                         | sélectionne aléatoirement l’un des nœuds ZooKeeper.                                                                                  |
| `in_order`                       | sélectionne le premier nœud ZooKeeper ; s’il n’est pas disponible, le deuxième, et ainsi de suite.                                   |
| `nearest_hostname`               | sélectionne un nœud ZooKeeper dont le hostname est le plus similaire à celui du serveur ; le hostname est comparé au préfixe du nom. |
| `hostname_levenshtein_distance`  | identique à nearest&#95;hostname, mais compare le hostname à l’aide de la distance de Levenshtein.                                   |
| `hostname_longest_common_prefix` | identique à nearest&#95;hostname, mais préfère le nœud dont le hostname partage le plus long préfixe commun avec celui du serveur.   |
| `hostname_longest_common_suffix` | identique à nearest&#95;hostname, mais préfère le nœud dont le hostname partage le plus long suffixe commun avec celui du serveur.   |
| `first_or_random`                | sélectionne le premier nœud ZooKeeper ; s’il n’est pas disponible, sélectionne aléatoirement l’un des nœuds ZooKeeper restants.      |
| `round_robin`                    | sélectionne le premier nœud ZooKeeper ; en cas de reconnexion, sélectionne le suivant.                                               |

**Exemple de configuration**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**Voir aussi**

* [Réplication](../../engines/table-engines/mergetree-family/replication.md)
* [Guide du programmeur de ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [Communication sécurisée facultative entre ClickHouse et ZooKeeper](/fr/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

Méthode de stockage des en-têtes des data parts dans ZooKeeper. Ce paramètre s’applique uniquement à la famille [`MergeTree`](/fr/engines/table-engines/mergetree-family). Il peut être défini :

**Globalement dans la section [merge&#95;tree](#merge_tree) du fichier `config.xml`**

ClickHouse utilise ce paramètre pour toutes les tables du serveur. Vous pouvez modifier ce paramètre à tout moment. Les tables existantes changent de comportement lorsque ce paramètre est modifié.

**Pour chaque table**

Lors de la création d’une table, spécifiez le [paramètre d’engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) correspondant. Le comportement d’une table existante dotée de ce paramètre ne change pas, même si le paramètre global change.

**Valeurs possibles**

* `0` — La fonctionnalité est désactivée.
* `1` — La fonctionnalité est activée.

Si [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), les tables [répliquées](../../engines/table-engines/mergetree-family/replication.md) stockent les en-têtes des data parts de manière compacte à l’aide d’un seul `znode`. Si la table contient de nombreuses colonnes, cette méthode de stockage réduit considérablement le volume de données stockées dans Zookeeper.

:::note
Après avoir appliqué `use_minimalistic_part_header_in_zookeeper = 1`, vous ne pouvez pas revenir à une version antérieure du serveur ClickHouse qui ne prend pas en charge ce paramètre. Soyez prudent lors de la mise à niveau de ClickHouse sur les serveurs d’un cluster. Ne mettez pas tous les serveurs à niveau en même temps. Il est plus sûr de tester les nouvelles versions de ClickHouse dans un environnement de test, ou sur seulement quelques serveurs du cluster.

Les en-têtes de data parts déjà stockés avec ce paramètre ne peuvent pas être restaurés dans leur représentation précédente (non compacte).
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

Gère l’exécution des [requêtes DDL distribuées](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) sur le cluster.
Fonctionne uniquement si [ZooKeeper](/fr/operations/server-configuration-parameters/settings#zookeeper) est activé.

Les paramètres configurables dans `<distributed_ddl>` incluent :

| Paramètre              | Description                                                                                                                                                     | Valeur par défaut                            |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| `cleanup_delay_period` | le nettoyage démarre après la réception d’un événement de nouveau nœud si le dernier nettoyage n’a pas eu lieu il y a moins de `cleanup_delay_period` secondes. | `60` secondes                                |
| `max_tasks_in_queue`   | le nombre maximal de tâches pouvant se trouver dans la file d’attente.                                                                                          | `1,000`                                      |
| `path`                 | le chemin dans Keeper pour `task_queue` des requêtes DDL                                                                                                        |                                              |
| `pool_size`            | le nombre de requêtes `ON CLUSTER` pouvant être exécutées simultanément                                                                                         |                                              |
| `profile`              | le profil utilisé pour exécuter les requêtes DDL                                                                                                                |                                              |
| `task_max_lifetime`    | supprime le nœud si son ancienneté dépasse cette valeur.                                                                                                        | `7 * 24 * 60 * 60` (une semaine en secondes) |

**Exemple**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

Chemin vers le dossier dans lequel un serveur ClickHouse stocke les configurations des utilisateurs et des rôles créées par des commandes SQL.

**Voir aussi**

* [Contrôle d’accès et gestion des comptes](/fr/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

Définit si les types de mot de passe en clair (non sécurisés) sont autorisés.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

Indique si le type de mot de passe non sécurisé `no_password` est autorisé.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

Interdit de créer un utilisateur sans mot de passe, à moins que &#39;IDENTIFIED WITH no&#95;password&#39; ne soit explicitement spécifié.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

Délai d’expiration de la session par défaut, en secondes.

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

Définit le type de mot de passe appliqué automatiquement dans les requêtes telles que `CREATE USER u IDENTIFIED BY 'p'`.

Les valeurs acceptées sont :

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

Section du fichier de configuration contenant les paramètres suivants :

* Chemin vers le fichier de configuration contenant les utilisateurs prédéfinis.
* Chemin vers le dossier où sont stockés les utilisateurs créés par des commandes SQL.
* Chemin du nœud ZooKeeper où sont stockés et répliqués les utilisateurs créés par des commandes SQL.

Si cette section est définie, les chemins de [users&#95;config](/fr/operations/server-configuration-parameters/settings#users_config) et de [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path) ne seront pas utilisés.

La section `user_directories` peut contenir un nombre quelconque d’éléments ; l’ordre de ces éléments détermine leur préséance (plus un élément est placé haut, plus sa préséance est élevée).

**Exemples**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Les utilisateurs, les rôles, les politiques de lignes, les quotas et les profils peuvent également être stockés dans ZooKeeper :

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

Vous pouvez également définir les sections `memory` — c’est-à-dire stocker les informations uniquement en mémoire, sans les écrire sur disque — et `ldap` — c’est-à-dire stocker les informations sur un serveur LDAP.

Pour ajouter un serveur LDAP comme répertoire distant pour des utilisateurs qui ne sont pas définis localement, définissez une seule section `ldap` avec les paramètres suivants :

| Paramètre | Description                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`   | section contenant une liste de rôles définis localement qui seront attribués à chaque utilisateur récupéré depuis le serveur LDAP. Si aucun rôle n’est spécifié, l’utilisateur ne pourra effectuer aucune action après l’authentification. Si l’un des rôles répertoriés n’est pas défini localement au moment de l’authentification, la tentative d’authentification échouera comme si le mot de passe fourni était incorrect. |
| `server`  | l’un des noms de serveur LDAP définis dans la section de config `ldap_servers`. Ce paramètre est obligatoire et ne peut pas être vide.                                                                                                                                                                                                                                                                                          |

**Exemple**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

Définit une liste de domaines de premier niveau personnalisés à ajouter, chaque entrée étant au format `<name>/path/to/file</name>`.

Par exemple :

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

Voir aussi :

* la fonction [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) et ses variantes,
  qui accepte le nom d’une liste personnalisée de TLD et renvoie la partie du domaine comprenant les sous-domaines de plus haut niveau jusqu’au premier sous-domaine significatif.

<div id="proxy">
  ## proxy
</div>

Définissez des serveurs proxy pour les requêtes HTTP et HTTPS, actuellement pris en charge pour le stockage S3, les fonctions de table S3 et les fonctions URL.

Il existe trois façons de définir des serveurs proxy :

* les variables d’environnement
* les listes de proxys
* les résolveurs de proxy distants.

Il est également possible de contourner les serveurs proxy pour des hôtes spécifiques à l’aide de `no_proxy`.

**Variables d’environnement**

Les variables d’environnement `http_proxy` et `https_proxy` vous permettent de spécifier un
serveur proxy pour un protocole donné. Si elles sont définies sur votre système, cela devrait fonctionner de manière transparente.

Il s’agit de l’approche la plus simple si un protocole donné n’a
qu’un seul serveur proxy et que ce serveur proxy ne change pas.

**Listes de proxys**

Cette approche vous permet de spécifier un ou plusieurs
serveurs proxy pour un protocole. Si plusieurs serveurs proxy sont définis,
ClickHouse utilise les différents proxys selon un mode round-robin, en répartissant la
charge entre les serveurs. Il s’agit de l’approche la plus simple s’il existe plusieurs
serveurs proxy pour un protocole et que la liste des serveurs proxy ne change pas.

**Modèle de configuration**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

Sélectionnez un champ parent dans les onglets ci-dessous pour afficher ses champs enfants :

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Champ     | Description                               |
    | --------- | ----------------------------------------- |
    | `<http>`  | Une liste d’un ou plusieurs proxies HTTP  |
    | `<https>` | Une liste d’un ou plusieurs proxies HTTPS |
  </TabItem>

  <TabItem value="http_https" label="<http> et <https>">
    | Champ   | Description    |
    | ------- | -------------- |
    | `<uri>` | L’URI du proxy |
  </TabItem>
</Tabs>

**Résolveurs de proxy distants**

Il est possible que les serveurs proxy changent dynamiquement. Dans ce
cas, vous pouvez définir le point de terminaison d’un résolveur. ClickHouse envoie
une requête GET vide à ce point de terminaison, et le résolveur distant doit renvoyer l’hôte du proxy.
ClickHouse l’utilisera pour former l’URI du proxy à l’aide du modèle suivant : `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Modèle de configuration**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

Sélectionnez un champ parent dans les onglets ci-dessous pour afficher ses champs enfants :

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Champ     | Description                                     |
    | --------- | ----------------------------------------------- |
    | `<http>`  | Une liste d&#39;un ou plusieurs résolveurs* |
    | `<https>` | Une liste d&#39;un ou plusieurs résolveurs* |
  </TabItem>

  <TabItem value="http_https" label="<http> et <https>">
    | Champ        | Description                                                      |
    | ------------ | ---------------------------------------------------------------- |
    | `<resolver>` | Le point de terminaison et les autres détails d&#39;un résolveur |

    :::note
    Vous pouvez avoir plusieurs éléments `<resolver>`, mais seul le premier
    `<resolver>` pour un protocole donné est utilisé. Tous les autres éléments `<resolver>`
    pour ce protocole sont ignorés. Cela signifie que l&#39;équilibrage de charge
    (si nécessaire) doit être implémenté par le résolveur distant.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | Champ                | Description                                                                                                                                                                                                            |
    | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `<endpoint>`         | L&#39;URI du résolveur proxy                                                                                                                                                                                           |
    | `<proxy_scheme>`     | Le protocole de l&#39;URI finale du proxy. Il peut s&#39;agir de `http` ou de `https`.                                                                                                                                 |
    | `<proxy_port>`       | Le numéro de port du résolveur proxy                                                                                                                                                                                   |
    | `<proxy_cache_time>` | La durée, en secondes, pendant laquelle les valeurs du résolveur doivent être mises en cache par ClickHouse. Définir cette valeur sur `0` amène ClickHouse à contacter le résolveur pour chaque requête HTTP ou HTTPS. |
  </TabItem>
</Tabs>

**Préséance**

Les paramètres du proxy sont déterminés dans l&#39;ordre suivant :

| Ordre | Paramètre                     |
| ----- | ----------------------------- |
| 1.    | Résolveurs proxy distants     |
| 2.    | Listes de proxy               |
| 3.    | Variables d&#39;environnement |

ClickHouse vérifie le type de résolveur ayant la priorité la plus élevée pour le protocole de la requête. S&#39;il n&#39;est pas défini,
il vérifie alors le type de résolveur ayant la priorité immédiatement inférieure, jusqu&#39;à atteindre le résolveur d&#39;environnement.
Cela permet également d&#39;utiliser une combinaison de types de résolveur.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

Par défaut, le tunneling (c.-à-d. `HTTP CONNECT`) est utilisé pour effectuer des requêtes `HTTPS` via un proxy `HTTP`. Ce paramètre permet de le désactiver.

**no&#95;proxy**

Par défaut, toutes les requêtes passent par le proxy. Pour le désactiver pour des hôtes spécifiques, la variable `no_proxy` doit être définie.
Elle peut être définie dans la clause `<proxy>` pour les résolveurs de liste et distants, et comme variable d&#39;environnement pour le résolveur d&#39;environnement.
Elle prend en charge les adresses IP, les domaines, les sous-domaines et le caractère générique `'*'` pour un contournement total. Les points en tête sont supprimés, comme le fait curl.

**Exemple**

La configuration ci-dessous contourne le proxy pour les requêtes vers `clickhouse.cloud` et tous ses sous-domaines (par ex., `auth.clickhouse.cloud`).
Il en va de même pour GitLab, même s&#39;il commence par un point. `gitlab.com` et `about.gitlab.com` contourneraient tous deux le proxy.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

Le répertoire utilisé pour stocker toutes les requêtes `CREATE WORKLOAD` et `CREATE RESOURCE`. Par défaut, le dossier `/workload/` situé dans le répertoire de travail du serveur est utilisé.

**Exemple**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**Voir aussi**

* [Hiérarchie des charges de travail](/fr/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

Le chemin vers un nœud ZooKeeper, utilisé comme emplacement de stockage pour toutes les requêtes `CREATE WORKLOAD` et `CREATE RESOURCE`. Pour garantir la cohérence, toutes les définitions SQL sont stockées comme valeur de ce znode unique. Par défaut, ZooKeeper n’est pas utilisé et les définitions sont stockées sur le [disque](#workload_path).

**Exemple**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**Voir aussi**

* [Hiérarchie des charges de travail](/fr/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

Paramètres de la [table système `zookeeper_log`](/fr/operations/system-tables/zookeeper_log).

Les paramètres suivants peuvent être configurés via des sous-balises :

<SystemLogParameters />

**Exemple**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```