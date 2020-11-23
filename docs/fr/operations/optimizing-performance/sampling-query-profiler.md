---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Profilage De Requ\xEAte"
---

# Échantillonnage Du Profileur De Requête {#sampling-query-profiler}

ClickHouse exécute un profileur d'échantillonnage qui permet d'analyser l'exécution des requêtes. En utilisant profiler, vous pouvez trouver des routines de code source qui ont utilisé le plus fréquemment lors de l'exécution de la requête. Vous pouvez suivre le temps CPU et le temps d'horloge murale passé, y compris le temps d'inactivité.

Utilisation du générateur de profils:

-   Installation de la [trace_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) la section de la configuration du serveur.

    Cette section configure le [trace_log](../../operations/system-tables.md#system_tables-trace_log) tableau système contenant les résultats du fonctionnement du profileur. Il est configuré par défaut. Rappelez-vous que les données de ce tableau est valable que pour un serveur en cours d'exécution. Après le redémarrage du serveur, ClickHouse ne nettoie pas la table et toute l'adresse de mémoire virtuelle stockée peut devenir invalide.

-   Installation de la [query_profiler_cpu_time_period_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) ou [query_profiler_real_time_period_ns](../settings/settings.md#query_profiler_real_time_period_ns) paramètre. Les deux paramètres peuvent être utilisés simultanément.

    Ces paramètres vous permettent de configurer les minuteries du profileur. Comme il s'agit des paramètres de session, vous pouvez obtenir une fréquence d'échantillonnage différente pour l'ensemble du serveur, les utilisateurs individuels ou les profils d'utilisateurs, pour votre session interactive et pour chaque requête individuelle.

La fréquence d'échantillonnage par défaut est d'un échantillon par seconde et le processeur et les minuteries réelles sont activés. Cette fréquence permet de collecter suffisamment d'informations sur le cluster ClickHouse. En même temps, en travaillant avec cette fréquence, profiler n'affecte pas les performances du serveur ClickHouse. Si vous avez besoin de profiler chaque requête individuelle, essayez d'utiliser une fréquence d'échantillonnage plus élevée.

Pour analyser les `trace_log` système de table:

-   Installer le `clickhouse-common-static-dbg` paquet. Voir [Installer à partir de paquets DEB](../../getting-started/install.md#install-from-deb-packages).

-   Autoriser les fonctions d'introspection par [allow_introspection_functions](../settings/settings.md#settings-allow_introspection_functions) paramètre.

    Pour des raisons de sécurité, les fonctions d'introspection sont désactivées par défaut.

-   L'utilisation de la `addressToLine`, `addressToSymbol` et `demangle` [fonctions d'introspection](../../sql-reference/functions/introspection.md) pour obtenir les noms de fonctions et leurs positions dans le code ClickHouse. Pour obtenir un profil pour une requête, vous devez agréger les données du `trace_log` table. Vous pouvez agréger des données par des fonctions individuelles ou par l'ensemble des traces de la pile.

Si vous avez besoin de visualiser `trace_log` info, essayez [flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) et [speedscope](https://github.com/laplab/clickhouse-speedscope).

## Exemple {#example}

Dans cet exemple, nous:

-   Filtrage `trace_log` données par un identifiant de requête et la date actuelle.

-   Agrégation par trace de pile.

-   En utilisant les fonctions d'introspection, nous obtiendrons un rapport de:

    -   Noms des symboles et des fonctions de code source correspondantes.
    -   Emplacements de code Source de ces fonctions.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
