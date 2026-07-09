---
description: 'Guide d’utilisation et de configuration de la fonctionnalité de cache de requêtes dans ClickHouse'
sidebar_label: 'Cache de requêtes'
sidebar_position: 65
slug: /operations/query-cache
title: 'Cache de requêtes'
doc_type: 'guide'
---

Le cache de requêtes permet de n’exécuter les requêtes `SELECT` qu’une seule fois, puis de servir les exécutions suivantes de la même requête directement depuis le cache.
Selon le type de requêtes, cela peut réduire considérablement la latence et la consommation de ressources du serveur ClickHouse.

<div id="background-design-and-limitations">
  ## Contexte, conception et limites
</div>

Les caches de requêtes peuvent généralement être considérés comme transactionnellement cohérents ou incohérents.

* Dans les caches transactionnellement cohérents, la base de données invalide (supprime) les résultats de requête en cache si le résultat de la requête `SELECT` change
  ou est susceptible de changer. Dans ClickHouse, les opérations qui modifient les données incluent les inserts/updates/deletes dans les tables, ainsi que les
  merges avec collapsing. La mise en cache transactionnellement cohérente convient particulièrement aux bases de données OLTP, comme par exemple
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (qui a supprimé le cache de requêtes après la version 8.0) et
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
* Dans les caches transactionnellement incohérents, de légères imprécisions dans les résultat de la requête sont acceptées, en partant du principe que toutes les entrées du cache
  se voient attribuer une période de validité à l&#39;issue de laquelle elles expirent (par ex. 1 minute) et que les données sous-jacentes changent peu pendant cette période.
  Cette approche est globalement mieux adaptée aux bases de données OLAP. Un exemple où une mise en cache transactionnellement incohérente est suffisante
  est celui d&#39;un rapport de ventes horaire dans un outil de reporting, consulté simultanément par plusieurs utilisateurs. Les données de ventes changent généralement
  assez lentement pour que la base de données n&#39;ait besoin de calculer le rapport qu&#39;une seule fois (ce que représente la première requête `SELECT`). Les requêtes suivantes peuvent alors être
  servies directement depuis le cache de requêtes. Dans cet exemple, une période de validité raisonnable pourrait être de 30 min.

La mise en cache transactionnellement incohérente est traditionnellement assurée par des outils client ou des paquets proxy (par ex.
[chproxy](https://www.chproxy.org/configuration/caching/)) qui interagissent avec la base de données. Il en résulte que la même logique de mise en cache et la même
configuration sont souvent dupliquées. Avec le cache de requêtes de ClickHouse, la logique de mise en cache est déplacée côté serveur. Cela réduit les efforts de maintenance
et évite les redondances.

<div id="configuration-settings-and-usage">
  ## Paramètres de configuration et utilisation
</div>

:::note
Dans ClickHouse Cloud, vous devez utiliser les [paramètres au niveau des requêtes](/fr/operations/settings/query-level) pour modifier les paramètres du cache des requêtes. La modification des [paramètres au niveau de la configuration](/fr/operations/configuration-files) n’est actuellement pas prise en charge.
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md) exécute une seule requête à la fois. Comme la mise en cache des résultats de requête n’a pas de sens, le cache des résultats de requête est désactivé dans clickhouse-local.
:::

Le paramètre [use&#95;query&#95;cache](/fr/operations/settings/settings#use_query_cache) permet de définir si une requête spécifique ou toutes les requêtes de la
session en cours doivent utiliser le cache des requêtes. Par exemple, la première exécution de la requête

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

stockera le résultat de la requête dans le cache de requêtes. Les exécutions suivantes de la même requête (également avec le paramètre `use_query_cache = true`) liront
le résultat calculé depuis le cache et le renverront immédiatement.

:::note
Le paramètre `use_query_cache` et tous les autres paramètres liés au cache de requêtes ne prennent effet que pour les instructions `SELECT` autonomes. En particulier,
les résultats des `SELECT` sur des vues créées par `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` ne sont pas mis en cache, sauf si l&#39;instruction `SELECT`
est exécutée avec `SETTINGS use_query_cache = true`.
:::

L&#39;utilisation du cache peut être configurée plus finement à l&#39;aide des paramètres [enable&#95;writes&#95;to&#95;query&#95;cache](/fr/operations/settings/settings#enable_writes_to_query_cache)
et [enable&#95;reads&#95;from&#95;query&#95;cache](/fr/operations/settings/settings#enable_reads_from_query_cache) (tous deux à `true` par défaut). Le premier paramètre
contrôle si les résultats de la requête sont stockés dans le cache, tandis que le second détermine si la base de données doit tenter de récupérer les résultats de la requête
depuis le cache. Par exemple, la requête suivante n&#39;utilisera le cache qu&#39;en lecture seule, c&#39;est-à-dire qu&#39;elle essaiera d&#39;y lire, sans y stocker son
résultat :

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

Pour un contrôle maximal, il est généralement recommandé de n’utiliser les paramètres `use_query_cache`, `enable_writes_to_query_cache` et
`enable_reads_from_query_cache` qu’avec des requêtes spécifiques. Il est également possible d’activer le cache de requêtes au niveau de l’utilisateur ou du profil (par ex. via `SET
use_query_cache = true`), mais il faut garder à l’esprit que toutes les requêtes `SELECT` peuvent alors renvoyer des résultats mis en cache.

Le cache de requêtes peut être vidé à l’aide de l’instruction `SYSTEM CLEAR QUERY CACHE`. Le contenu du cache de requêtes est affiché dans la table système
[system.query&#95;cache](system-tables/query_cache.md). Le nombre de succès et d’échecs du cache de requêtes depuis le démarrage de la base de données est indiqué par les événements
&quot;QueryCacheHits&quot; et &quot;QueryCacheMisses&quot; dans la table système [system.events](system-tables/events.md). Les deux compteurs ne sont mis à jour que pour les
requêtes `SELECT` exécutées avec le paramètre `use_query_cache = true` ; les autres requêtes n’affectent pas &quot;QueryCacheMisses&quot;. Le champ `query_cache_usage`
dans la table système [system.query&#95;log](system-tables/query_log.md) indique, pour chaque requête exécutée, si le résultat de la requête a été écrit dans le cache de requêtes ou
lu depuis celui-ci. Les métriques `QueryCacheEntries` et `QueryCacheBytes` dans la table système
[system.metrics](system-tables/metrics.md) indiquent combien d’entrées / d’octets le cache de requêtes contient actuellement.

Le cache de requêtes existe une fois par processus serveur ClickHouse. Cependant, les résultats mis en cache ne sont, par défaut, pas partagés entre les utilisateurs. Cela peut être
modifié (voir ci-dessous), mais ce n’est pas recommandé pour des raisons de sécurité.

Les résultats des requêtes sont référencés dans le cache de requêtes par l’[Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) de
leur requête. Cela signifie que la mise en cache ne tient pas compte des majuscules/minuscules : par exemple, `SELECT 1` et `select 1` sont traités comme une seule et même requête. Pour
rendre la correspondance plus naturelle, tous les paramètres au niveau de la requête liés au cache de requêtes et au [formatage de sortie](settings/settings-formats.md))
sont supprimés de l’AST.

Si la requête a été interrompue en raison d’une exception ou d’une annulation par l’utilisateur, aucune entrée n’est écrite dans le cache de requêtes.

La taille du cache de requêtes en octets, le nombre maximal d’entrées de cache et la taille maximale des entrées de cache individuelles (en octets et en
enregistrements) peuvent être configurés à l’aide de différentes [options de configuration du serveur](/fr/operations/server-configuration-parameters/settings#query_cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

Il est également possible de limiter l’utilisation du cache pour chaque utilisateur à l’aide de [profils de paramètres](settings/settings-profiles.md) et de [contraintes sur les
paramètres](settings/constraints-on-settings.md). Plus précisément, vous pouvez restreindre la quantité maximale de mémoire (en octets) qu’un utilisateur peut
allouer au cache des requêtes, ainsi que le nombre maximal de résultats de requête stockés. Pour cela, définissez d’abord les paramètres
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/fr/operations/settings/settings#query_cache_max_size_in_bytes) et
[query&#95;cache&#95;max&#95;entries](/fr/operations/settings/settings#query_cache_max_entries) dans un profil utilisateur de `users.xml`, puis définissez ces deux paramètres en
`readonly` :

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

Pour définir la durée minimale d’exécution d’une requête pour que son résultat puisse être mis en cache, vous pouvez utiliser le paramètre
[query&#95;cache&#95;min&#95;query&#95;duration](/fr/operations/settings/settings#query_cache_min_query_duration). Par exemple, le résultat de la requête

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

n&#39;est mis en cache que si la requête s&#39;exécute pendant plus de 5 secondes. Il est également possible de spécifier le nombre d&#39;exécutions nécessaires d&#39;une requête pour que son résultat soit
mis en cache - pour cela, utilisez le paramètre [query&#95;cache&#95;min&#95;query&#95;runs](/fr/operations/settings/settings#query_cache_min_query_runs).

Les entrées du cache de requêtes deviennent obsolètes après une certaine durée (time-to-live). Par défaut, cette durée est de 60 secondes, mais une
valeur différente peut être définie au niveau de la session, du profil ou de la requête à l&#39;aide du paramètre [query&#95;cache&#95;ttl](/fr/operations/settings/settings#query_cache_ttl). Le cache de requêtes
évince les entrées de manière &quot;paresseuse&quot;, c.-à-d. que lorsqu&#39;une entrée devient obsolète, elle n&#39;est pas immédiatement supprimée du cache. À la place, lorsqu&#39;une nouvelle entrée
doit être insérée dans le cache de requêtes, la base de données vérifie si le cache dispose de suffisamment d&#39;espace libre pour cette nouvelle entrée. Si ce n&#39;est pas le
cas, la base de données tente de supprimer toutes les entrées obsolètes. Si le cache ne dispose toujours pas de suffisamment d&#39;espace libre, la nouvelle entrée n&#39;est pas insérée.

Si la requête est exécutée via HTTP, ClickHouse définit alors les en-têtes `Age` et `Expires` avec l&#39;ancienneté (en secondes) et le timestamp d&#39;expiration de l&#39;entrée
mise en cache.

Les entrées du cache de requêtes sont compressées par défaut. Cela réduit la consommation mémoire globale, au prix d&#39;écritures et de lectures plus lentes dans le cache de requêtes.
Pour désactiver la compression, utilisez le paramètre [query&#95;cache&#95;compress&#95;entries](/fr/operations/settings/settings#query_cache_compress_entries).

Il est parfois utile de conserver en cache plusieurs résultats pour une même requête. Cela peut être réalisé à l&#39;aide du paramètre
[query&#95;cache&#95;tag](/fr/operations/settings/settings#query_cache_tag), qui agit comme une étiquette (ou un espace de noms) pour les entrées du cache de requêtes. Le cache de requêtes
considère comme différents les résultats d&#39;une même requête ayant des tags différents.

Exemple de création de trois entrées différentes dans le cache de requêtes pour la même requête :

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

Pour supprimer uniquement les entrées portant le tag `tag` du cache de requêtes, vous pouvez utiliser l’instruction `SYSTEM CLEAR QUERY CACHE TAG 'tag'`.

<div id="subquery-caching">
  ## Mise en cache des sous-requêtes
</div>

Par défaut, `use_query_cache` sur la requête externe ne s’applique pas aux sous-requêtes. Cela signifie que chaque sous-requête doit activer explicitement la mise en cache :

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

Dans cet exemple, seul le résultat de la sous-requête interne est mis en cache. La requête externe n’est pas mise en cache.

Pour activer la mise en cache de toutes les sous-requêtes en une seule opération, utilisez le paramètre `query_cache_for_subqueries` :

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Pour désactiver explicitement la mise en cache pour une sous-requête spécifique lorsque la propagation groupée est activée, définissez `use_query_cache = false` dans cette sous-requête :

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Les entrées du cache des sous-requêtes sont visibles dans [system.query&#95;cache](system-tables/query_cache.md) avec `is_subquery = 1`. Le paramètre `query_cache_ttl` s&#39;applique également aux entrées du cache des sous-requêtes et peut être défini pour chaque sous-requête.

ClickHouse lit les données des tables en blocks de [max&#95;block&#95;size](/fr/operations/settings/settings#max_block_size) rows. En raison du filtrage, de l&#39;aggregation,
etc., les blocks de résultat sont généralement bien plus petits que &#39;max&#95;block&#95;size&#39;, mais il arrive aussi qu&#39;ils soient beaucoup plus grands. Le paramètre
[query&#95;cache&#95;squash&#95;partial&#95;results](/fr/operations/settings/settings#query_cache_squash_partial_results) (activé par défaut) détermine si les blocks de résultat
sont regroupés (s&#39;ils sont très petits) ou scindés (s&#39;ils sont volumineux) en blocks de taille &#39;max&#95;block&#95;size&#39; avant leur insertion dans le cache des résultats de requête. Cela réduit les performances d&#39;écriture dans le cache de requêtes, mais améliore le taux de compression des entrées du cache et fournit une
granularité de block plus naturelle lorsque les résultats de la requête sont ensuite servis depuis le cache de requêtes.

Par conséquent, le cache de requêtes stocke pour chaque requête plusieurs blocks de résultat
(partiels). Bien que ce comportement constitue un bon choix par défaut, il peut être désactivé à l&#39;aide du paramètre
[query&#95;cache&#95;squash&#95;partial&#95;results](/fr/operations/settings/settings#query_cache_squash_partial_results).

De plus, les résultats des requêtes comportant des fonctions non déterministes ne sont pas mis en cache par défaut. Ces fonctions incluent

* les fonctions d&#39;accès aux Dictionaries : [`dictGet()`](/fr/sql-reference/functions/ext-dict-functions) etc.
* les [fonctions définies par l&#39;utilisateur](../sql-reference/statements/create/function.md) sans la balise `<deterministic>true</deterministic>` dans leur définition XML,
* les fonctions qui renvoient la date ou l&#39;heure actuelles : [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) etc.,
* les fonctions qui renvoient des valeurs aléatoires : [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) etc.,
* les fonctions dont le résultat dépend de la taille et de l&#39;ordre, ou des chunks internes utilisés pour le query processing :
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) etc.,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) etc.,
* les fonctions qui dépendent de l&#39;environnement : [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/fr/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) etc.

Pour forcer malgré tout la mise en cache des résultats des requêtes comportant des fonctions non déterministes, utilisez le paramètre
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/fr/operations/settings/settings#query_cache_nondeterministic_function_handling).

Les résultats des requêtes qui impliquent des system tables (par ex. [system.processes](system-tables/processes.md)&#96; ou
[information&#95;schema.tables](system-tables/information_schema.md)) ne sont pas mis en cache par défaut. Pour forcer malgré tout la mise en cache des résultats des requêtes avec
des system tables, utilisez le paramètre [query&#95;cache&#95;system&#95;table&#95;handling](/fr/operations/settings/settings#query_cache_system_table_handling).

Enfin, les entrées du cache de requêtes ne sont pas partagées entre les utilisateurs pour des raisons de sécurité. Par exemple, l&#39;utilisateur A ne doit pas pouvoir contourner une
politique d&#39;accès au niveau des lignes sur une table en exécutant la même requête qu&#39;un autre utilisateur B pour lequel aucune politique de ce type n&#39;existe. Cependant, si nécessaire, les entrées du cache peuvent
être rendues accessibles à d&#39;autres utilisateurs (c&#39;est-à-dire partagées) en spécifiant le paramètre
[query&#95;cache&#95;share&#95;between&#95;users](/fr/operations/settings/settings#query_cache_share_between_users).

<div id="related-content">
  ## Articles connexes
</div>

* Blog : [Présentation du cache de requêtes de ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)