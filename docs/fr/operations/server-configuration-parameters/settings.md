---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "Les Param\xE8tres Du Serveur"
---

# Les Paramètres Du Serveur {#server-settings}

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

L'intervalle en secondes avant de recharger les dictionnaires intégrés.

Clickhouse recharge les dictionnaires intégrés toutes les X secondes. Cela permet d'éditer des dictionnaires “on the fly” sans redémarrer le serveur.

Valeur par défaut: 3600.

**Exemple**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#server-settings-compression}

Paramètres de compression de données pour [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-tables de moteur.

!!! warning "Avertissement"
    Ne l'utilisez pas si vous venez de commencer à utiliser ClickHouse.

Modèle de Configuration:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` Fields:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` ou `zstd`.

Vous pouvez configurer plusieurs `<case>` section.

Actions lorsque les conditions sont remplies:

-   Si une partie de données correspond à un ensemble de conditions, ClickHouse utilise la méthode de compression spécifiée.
-   Si une partie de données correspond à plusieurs ensembles de conditions, ClickHouse utilise le premier ensemble de conditions correspondant.

Si aucune condition n'est remplie pour une partie de données, ClickHouse utilise `lz4` compression.

**Exemple**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default_database {#default-database}

La base de données par défaut.

Pour obtenir une liste de bases de données, utilisez la [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) requête.

**Exemple**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

Profil des paramètres par défaut.

Les paramètres des profils sont situés dans le fichier spécifié dans le paramètre `user_config`.

**Exemple**

``` xml
<default_profile>default</default_profile>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

Chemin d'accès au fichier de configuration des dictionnaires externes.

Chemin:

-   Spécifiez le chemin absolu ou le chemin relatif au fichier de configuration du serveur.
-   Le chemin peut contenir des caractères génériques \* et ?.

Voir aussi “[Dictionnaires externes](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**Exemple**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

Chargement paresseux des dictionnaires.

Si `true` chaque dictionnaire est créé lors de la première utilisation. Si la création du dictionnaire a échoué, la fonction qui utilisait le dictionnaire lève une exception.

Si `false`, tous les dictionnaires sont créés lorsque le serveur démarre, et si il y a une erreur, le serveur s'arrête.

La valeur par défaut est `true`.

**Exemple**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

Le chemin d'accès au répertoire avec des régimes pour l'entrée de données, tels que les schémas pour l' [CapnProto](../../interfaces/formats.md#capnproto) format.

**Exemple**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## graphite {#server_configuration_parameters-graphite}

Envoi de données à [Graphite](https://github.com/graphite-project).

Paramètre:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root_path – Prefix for keys.
-   metrics – Sending data from the [système.métrique](../../operations/system-tables.md#system_tables-metrics) table.
-   events – Sending deltas data accumulated for the time period from the [système.événement](../../operations/system-tables.md#system_tables-events) table.
-   events_cumulative – Sending cumulative data from the [système.événement](../../operations/system-tables.md#system_tables-events) table.
-   asynchronous_metrics – Sending data from the [système.asynchronous_metrics](../../operations/system-tables.md#system_tables-asynchronous_metrics) table.

Vous pouvez configurer plusieurs `<graphite>` clause. Par exemple, vous pouvez l'utiliser pour envoyer des données différentes à différents intervalles.

**Exemple**

``` xml
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

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

Paramètres pour l'amincissement des données pour le Graphite.

Pour plus de détails, voir [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Exemple**

``` xml
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

## http_port/https_port {#http-porthttps-port}

Port de connexion au serveur via HTTP(S).

Si `https_port` est spécifié, [openSSL](#server_configuration_parameters-openssl) doit être configuré.

Si `http_port` est spécifié, la configuration OpenSSL est ignorée même si elle est définie.

**Exemple**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

Page affichée par défaut lorsque vous accédez au serveur HTTP(S) ClickHouse.
La valeur par défaut est “Ok.” (avec un saut de ligne à la fin)

**Exemple**

Ouvrir `https://tabix.io/` lors de l'accès à `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include_from {#server_configuration_parameters-include_from}

Le chemin d'accès au fichier avec des substitutions.

Pour plus d'informations, consultez la section “[Fichiers de Configuration](../configuration-files.md#configuration_files)”.

**Exemple**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_http_port {#interserver-http-port}

Port pour l'échange de données entre les serveurs ClickHouse.

**Exemple**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

Le nom d'hôte qui peut être utilisé par d'autres serveurs pour accéder à ce serveur.

Si elle est omise, elle est définie de la même manière que `hostname-f` commande.

Utile pour rompre avec une interface réseau spécifique.

**Exemple**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

Le nom d'utilisateur et le mot de passe utilisés pour [réplication](../../engines/table-engines/mergetree-family/replication.md) avec les moteurs \* répliqués. Ces informations d'identification sont utilisées uniquement pour la communication entre les répliques et ne sont pas liées aux informations d'identification des clients ClickHouse. Le serveur vérifie ces informations d'identification pour la connexion de répliques et utilise les mêmes informations d'identification lors de la connexion à d'autres répliques. Donc, ces informations d'identification doivent être identiques pour tous les réplicas dans un cluster.
Par défaut, l'authentification n'est pas utilisé.

Cette section contient les paramètres suivants:

-   `user` — username.
-   `password` — password.

**Exemple**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep_alive_timeout {#keep-alive-timeout}

Le nombre de secondes que ClickHouse attend pour les demandes entrantes avant de fermer la connexion. Par défaut est de 3 secondes.

**Exemple**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

Restriction sur les hôtes dont les demandes peuvent provenir. Si vous voulez que le serveur réponde à tous, spécifiez `::`.

Exemple:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## enregistreur {#server_configuration_parameters-logger}

Paramètres de journalisation.

Touches:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`et`errorlog`. Une fois que le fichier atteint `size`, Archives ClickHouse et le renomme, et crée un nouveau fichier journal à sa place.
-   count – The number of archived log files that ClickHouse stores.

**Exemple**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

L'écriture dans le syslog est également prise en charge. Exemple de Config:

``` xml
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

Touches:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [Le mot clé syslog facility](https://en.wikipedia.org/wiki/Syslog#Facility) en majuscules avec la “LOG_” préfixe: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` et ainsi de suite).
    Valeur par défaut: `LOG_USER` si `address` est spécifié, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` et `syslog.`

## macro {#macros}

Substitutions de paramètres pour les tables répliquées.

Peut être omis si les tables répliquées ne sont pas utilisées.

Pour plus d'informations, consultez la section “[Création de tables répliquées](../../engines/table-engines/mergetree-family/replication.md)”.

**Exemple**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

Taille approximative (en octets) du cache des marques utilisées par les [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) famille.

Le cache est partagé pour le serveur et la mémoire est allouée au besoin. La taille du cache doit être d'au moins 5368709120.

**Exemple**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_concurrent_queries {#max-concurrent-queries}

Nombre maximal de demandes traitées simultanément.

**Exemple**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max_connections {#max-connections}

Le nombre maximal de connexions entrantes.

**Exemple**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

Le nombre maximal de fichiers ouverts.

Par défaut: `maximum`.

Nous vous recommandons d'utiliser cette option sous Mac OS X depuis le `getrlimit()` la fonction renvoie une valeur incorrecte.

**Exemple**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

Restriction sur la suppression de tables.

Si la taille d'un [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table dépasse `max_table_size_to_drop` (en octets), vous ne pouvez pas le supprimer à l'aide d'une requête DROP.

Si vous devez toujours supprimer la table sans redémarrer le serveur ClickHouse, créez le `<clickhouse-path>/flags/force_drop_table` fichier et exécutez la requête DROP.

Valeur par défaut: 50 Go.

La valeur 0 signifie que vous pouvez supprimer toutes les tables sans aucune restriction.

**Exemple**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge_tree {#server_configuration_parameters-merge_tree}

Réglage fin des tables dans le [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Pour plus d'informations, consultez MergeTreeSettings.h fichier d'en-tête.

**Exemple**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

Configuration client/serveur SSL.

Le Support pour SSL est fourni par le `libpoco` bibliothèque. L'interface est décrite dans le fichier [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Clés pour les paramètres Serveur/client:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contient le certificat.
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node's certificates. Details are in the description of the [Cadre](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) classe. Valeurs possibles: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Les valeurs acceptables: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. Ce paramètre est toujours recommandé car il permet d'éviter les problèmes à la fois si le serveur met en cache la session et si le client demande la mise en cache. Valeur par défaut: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**Exemple de paramètres:**

``` xml
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

## part_log {#server_configuration_parameters-part-log}

Journalisation des événements associés à [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Par exemple, ajouter ou fusionner des données. Vous pouvez utiliser le journal pour simuler des algorithmes de fusion et comparer leurs caractéristiques. Vous pouvez visualiser le processus de fusion.

Les requêtes sont enregistrées dans le [système.part_log](../../operations/system-tables.md#system_tables-part-log) table, pas dans un fichier séparé. Vous pouvez configurer le nom de cette table dans le `table` paramètre (voir ci-dessous).

Utilisez les paramètres suivants pour configurer la journalisation:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [partitionnement personnalisé clé](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**Exemple**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## chemin {#server_configuration_parameters-path}

Chemin d'accès au répertoire contenant des données.

!!! note "Note"
    La barre oblique de fin est obligatoire.

**Exemple**

``` xml
<path>/var/lib/clickhouse/</path>
```

## prometheus {#server_configuration_parameters-prometheus}

Exposer les données de métriques pour le raclage à partir [Prometheus](https://prometheus.io).

Paramètre:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [système.métrique](../system-tables.md#system_tables-metrics) table.
-   `events` – Flag that sets to expose metrics from the [système.événement](../system-tables.md#system_tables-events) table.
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [système.asynchronous_metrics](../system-tables.md#system_tables-asynchronous_metrics) table.

**Exemple**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query_log {#server_configuration_parameters-query-log}

Réglage de la journalisation des requêtes reçues avec [log_queries=1](../settings/settings.md) paramètre.

Les requêtes sont enregistrées dans le [système.query_log](../../operations/system-tables.md#system_tables-query_log) table, pas dans un fichier séparé. Vous pouvez modifier le nom de la table dans le `table` paramètre (voir ci-dessous).

Utilisez les paramètres suivants pour configurer la journalisation:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [partitionnement personnalisé clé](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) pour une table.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Si la table n'existe pas, ClickHouse la créera. Si la structure du journal des requêtes a été modifiée lors de la mise à jour du serveur ClickHouse, la table avec l'ancienne structure est renommée et une nouvelle table est créée automatiquement.

**Exemple**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query-thread-log}

Réglage de la journalisation des threads de requêtes reçues avec [log_query_threads=1](../settings/settings.md#settings-log-query-threads) paramètre.

Les requêtes sont enregistrées dans le [système.query_thread_log](../../operations/system-tables.md#system_tables-query-thread-log) table, pas dans un fichier séparé. Vous pouvez modifier le nom de la table dans le `table` paramètre (voir ci-dessous).

Utilisez les paramètres suivants pour configurer la journalisation:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [partitionnement personnalisé clé](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) pour un système de tableau.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Si la table n'existe pas, ClickHouse la créera. Si la structure du journal des threads de requête a été modifiée lors de la mise à jour du serveur ClickHouse, la table avec l'ancienne structure est renommée et une nouvelle table est créée automatiquement.

**Exemple**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace_log {#server_configuration_parameters-trace_log}

Paramètres pour le [trace_log](../../operations/system-tables.md#system_tables-trace_log) opération de table de système.

Paramètre:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [Partitionnement personnalisé clé](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) pour un système de tableau.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

Le fichier de configuration du serveur par défaut `config.xml` contient la section Paramètres suivante:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query_masking_rules {#query-masking-rules}

Règles basées sur Regexp, qui seront appliquées aux requêtes ainsi qu'à tous les messages de journal avant de les stocker dans les journaux du serveur,
`system.query_log`, `system.text_log`, `system.processes` table, et dans les journaux envoyés au client. Qui permet à la prévention de
fuite de données sensibles à partir de requêtes SQL (comme les noms, e-mails,
identificateurs ou numéros de carte de crédit) aux journaux.

**Exemple**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Config champs:
- `name` - nom de la règle (facultatif)
- `regexp` - Expression régulière compatible RE2 (obligatoire)
- `replace` - chaîne de substitution pour les données sensibles (facultatif, par défaut - six astérisques)

Les règles de masquage sont appliquées à l'ensemble de la requête (pour éviter les fuites de données sensibles provenant de requêtes malformées / Non analysables).

`system.events` table ont compteur `QueryMaskingRulesMatch` qui ont un nombre global de requête de masquage des règles de correspondances.

Pour les requêtes distribuées chaque serveur doivent être configurés séparément, sinon, les sous-requêtes transmises à d'autres
les nœuds seront stockés sans masquage.

## remote_servers {#server-settings-remote-servers}

Configuration des clusters utilisés par le [Distribué](../../engines/table-engines/special/distributed.md) moteur de table et par le `cluster` table de fonction.

**Exemple**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

Pour la valeur de l' `incl` attribut, voir la section “[Fichiers de Configuration](../configuration-files.md#configuration_files)”.

**Voir Aussi**

-   [skip_unavailable_shards](../settings/settings.md#settings-skip_unavailable_shards)

## fuseau {#server_configuration_parameters-timezone}

Le fuseau horaire du serveur.

Spécifié comme identifiant IANA pour le fuseau horaire UTC ou l'emplacement géographique (par exemple, Afrique / Abidjan).

Le fuseau horaire est nécessaire pour les conversions entre les formats String et DateTime lorsque les champs DateTime sont sortis au format texte (imprimés à l'écran ou dans un fichier) et lors de L'obtention de DateTime à partir d'une chaîne. En outre, le fuseau horaire est utilisé dans les fonctions qui fonctionnent avec l'heure et la date si elles ne reçoivent pas le fuseau horaire dans les paramètres d'entrée.

**Exemple**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

Port pour communiquer avec les clients via le protocole TCP.

**Exemple**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

Port TCP pour une communication sécurisée avec les clients. Utilisez le avec [OpenSSL](#server_configuration_parameters-openssl) paramètre.

**Valeurs possibles**

Entier positif.

**Valeur par défaut**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

Port pour communiquer avec les clients via le protocole MySQL.

**Valeurs possibles**

Entier positif.

Exemple

``` xml
<mysql_port>9004</mysql_port>
```

## tmp_path {#server-settings-tmp_path}

Chemin d'accès aux données temporaires pour le traitement des requêtes volumineuses.

!!! note "Note"
    La barre oblique de fin est obligatoire.

**Exemple**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#server-settings-tmp-policy}

La politique de [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) pour stocker des fichiers temporaires.
Si cela n'est pas [`tmp_path`](#server-settings-tmp_path) est utilisé, sinon elle est ignorée.

!!! note "Note"
    - `move_factor` est ignoré
- `keep_free_space_bytes` est ignoré
- `max_data_part_size_bytes` est ignoré
- vous devez avoir exactement un volume dans cette politique

## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

Taille du Cache (en octets) pour les données non compressées utilisées par les [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Il y a un cache partagé pour le serveur. La mémoire est allouée à la demande. Le cache est utilisé si l'option [use_uncompressed_cache](../settings/settings.md#setting-use_uncompressed_cache) est activé.

Le cache non compressé est avantageux pour les requêtes très courtes dans des cas individuels.

**Exemple**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

Le répertoire avec les fichiers utilisateur. Utilisé dans la fonction de table [fichier()](../../sql-reference/table-functions/file.md).

**Exemple**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users_config {#users-config}

Chemin d'accès au fichier qui contient:

-   Les configurations de l'utilisateur.
-   Les droits d'accès.
-   Les paramètres des profils.
-   Les paramètres de Quota.

**Exemple**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

Contient des paramètres qui permettent à ClickHouse d'interagir avec [ZooKeeper](http://zookeeper.apache.org/) cluster.

ClickHouse utilise ZooKeeper pour stocker les métadonnées des répliques lors de l'utilisation de tables répliquées. Si les tables répliquées ne sont pas utilisées, cette section de paramètres peut être omise.

Cette section contient les paramètres suivants:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    Exemple:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) qui est utilisé comme racine pour les znodes utilisés par le serveur ClickHouse. Facultatif.
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**Exemple de configuration**

``` xml
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
</zookeeper>
```

**Voir Aussi**

-   [Réplication](../../engines/table-engines/mergetree-family/replication.md)
-   [Guide du programmeur ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Méthode de stockage pour les en-têtes de partie de données dans ZooKeeper.

Ce paramètre s'applique uniquement à l' `MergeTree` famille. Il peut être spécifié:

-   À l'échelle mondiale dans le [merge_tree](#server_configuration_parameters-merge_tree) la section de la `config.xml` fichier.

    ClickHouse utilise le paramètre pour toutes les tables du serveur. Vous pouvez modifier le réglage à tout moment. Les tables existantes changent de comportement lorsque le paramètre change.

-   Pour chaque table.

    Lors de la création d'un tableau, indiquer la [moteur de réglage](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). Le comportement d'une table existante avec ce paramètre ne change pas, même si le paramètre global des changements.

**Valeurs possibles**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

Si `use_minimalistic_part_header_in_zookeeper = 1`, puis [répliqué](../../engines/table-engines/mergetree-family/replication.md) les tables stockent les en-têtes des parties de données de manière compacte à l'aide `znode`. Si la table contient plusieurs colonnes, cette méthode de stockage réduit considérablement le volume des données stockées dans Zookeeper.

!!! attention "Attention"
    Après l'application de `use_minimalistic_part_header_in_zookeeper = 1`, vous ne pouvez pas rétrograder le serveur ClickHouse vers une version qui ne prend pas en charge ce paramètre. Soyez prudent lors de la mise à niveau de ClickHouse sur les serveurs d'un cluster. Ne mettez pas à niveau tous les serveurs à la fois. Il est plus sûr de tester de nouvelles versions de ClickHouse dans un environnement de test, ou sur quelques serveurs d'un cluster.

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**Valeur par défaut:** 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

Désactive le cache DNS interne. Recommandé pour l'utilisation de ClickHouse dans les systèmes
avec des infrastructures en constante évolution telles que Kubernetes.

**Valeur par défaut:** 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

La période de mise à jour des adresses IP stockées dans le cache DNS interne de ClickHouse (en secondes).
La mise à jour est effectuée de manière asynchrone, dans un thread système séparé.

**Valeur par défaut**: 15.

## access_control_path {#access_control_path}

Chemin d'accès à un dossier dans lequel un serveur clickhouse stocke les configurations utilisateur et rôle créées par les commandes SQL.

Valeur par défaut: `/var/lib/clickhouse/access/`.

**Voir aussi**

-   [Le Contrôle d'accès et de Gestion de Compte](../access-rights.md#access-control)

[Article Original](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
