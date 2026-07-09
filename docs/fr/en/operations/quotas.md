---
description: 'Guide de configuration et de gestion des quotas d’utilisation des ressources dans ClickHouse'
sidebar_label: 'Quotas'
sidebar_position: 51
slug: /operations/quotas
title: 'Quotas'
doc_type: 'guide'
---

:::note Quotas dans ClickHouse Cloud
Les quotas sont pris en charge dans ClickHouse Cloud, mais doivent être créés à l’aide de la [syntaxe DDL](/fr/sql-reference/statements/create/quota). L’approche de configuration XML décrite ci-dessous n’est **pas prise en charge**.
:::

Les quotas permettent de limiter l’utilisation des ressources sur une période donnée ou d’en suivre l’utilisation.
Les quotas sont configurés dans la config des utilisateurs, généralement dans &#39;users.xml&#39;.

Le système dispose également d’une fonctionnalité permettant de limiter la complexité d’une requête unique. Voir la section [Restrictions sur la complexité des requêtes](../operations/settings/query-complexity.md).

Contrairement aux restrictions sur la complexité des requêtes, les quotas :

* imposent des restrictions sur un ensemble de requêtes pouvant être exécutées sur une période donnée, au lieu de limiter une seule requête.
* prennent en compte les ressources consommées sur tous les serveurs distants dans le cadre du traitement distribué des requêtes.

Examinons la section du fichier &#39;users.xml&#39; qui définit les quotas.

```xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <query_selects>0</query_selects>
            <query_inserts>0</query_inserts>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

Par défaut, le quota comptabilise la consommation des ressources heure par heure, sans en limiter l’utilisation.
La consommation des ressources calculée pour chaque intervalle est consignée dans le journal du serveur après chaque requête.

```xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <written_bytes>5000000</written_bytes>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
        <failed_sequential_authentications>5</failed_sequential_authentications>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <result_bytes>160000000000</result_bytes>
        <read_rows>500000000000</read_rows>
        <result_bytes>16000000000000</result_bytes>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

Pour le quota &#39;statbox&#39;, des restrictions sont définies pour chaque heure et pour chaque période de 24 heures (86 400 secondes). L’intervalle de temps est compté à partir d’un instant fixe défini par l’implémentation. En d’autres termes, l’intervalle de 24 heures ne commence pas nécessairement à minuit.

Lorsque l’intervalle se termine, toutes les valeurs collectées sont réinitialisées. Pour l’heure suivante, le calcul du quota repart de zéro.

Voici les quantités qui peuvent être limitées :

`queries` – Le nombre total de requêtes.

`query_selects` – Le nombre total de requêtes SELECT.

`query_inserts` – Le nombre total de requêtes d’insertion.

`errors` – Le nombre de requêtes ayant généré une exception.

`result_rows` – Le nombre total de lignes renvoyées en résultat.

`result_bytes` - La taille totale des lignes renvoyées en résultat.

`read_rows` – Le nombre total de lignes lues depuis les tables pour exécuter la requête sur tous les serveurs distants.

`read_bytes` - La taille totale lue depuis les tables pour exécuter la requête sur tous les serveurs distants.

`written_bytes` - Le volume total des opérations d’écriture.

`execution_time` – Le temps d’exécution total des requêtes, en secondes (temps écoulé).

`failed_sequential_authentications` - Le nombre total d’erreurs d’authentification séquentielles.

`queries_per_normalized_hash` – Le nombre maximal d’exécutions d’une même requête normalisée. Les requêtes normalisées sont des requêtes dont les littéraux sont remplacés par des marqueurs de substitution, de sorte que `SELECT 1` et `SELECT 2` sont considérées comme la même requête normalisée. Cette limite est suivie indépendamment pour chaque motif distinct de requête normalisée.

Si la limite est dépassée sur au moins un intervalle de temps, une exception est levée avec un message indiquant quelle restriction a été dépassée, pour quel intervalle, et quand le nouvel intervalle commence (c’est-à-dire quand des requêtes peuvent à nouveau être envoyées).

Les quotas peuvent utiliser la fonctionnalité &quot;clé de quota&quot; pour suivre indépendamment les ressources de plusieurs clés. En voici un exemple :

```xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)

        Instead of <keyed_by_ip /> you can use <keyed_by_forwarded_ip />, so the address
        from the X-Forwarded-For header is used as the quota key.

        For both <keyed_by_ip /> and <keyed_by_forwarded_ip /> you can additionally specify
        <ipv4_prefix_bits> and <ipv6_prefix_bits> to group clients by subnet instead of by a
        single address: the IP address is masked to the given prefix length before being used
        as the quota key. For example, <ipv4_prefix_bits>24</ipv4_prefix_bits> shares one bucket
        across a /24 IPv4 subnet, and <ipv6_prefix_bits>64</ipv6_prefix_bits> across a /64 IPv6
        subnet. These elements can only be used together with <keyed_by_ip /> or
        <keyed_by_forwarded_ip />.
    -->
    <keyed />
```

Vous pouvez également indexer les quotas sur le hash de la requête normalisée, de sorte que chaque requête distincte dispose de son propre quota indépendant. Dans la configuration XML, cela s’écrit `<keyed_by_normalized_query_hash />`:

```xml
<my_quota>
    <keyed_by_normalized_query_hash />
    <interval>
        <duration>3600</duration>
        <queries>100</queries>
    </interval>
</my_quota>
```

La même chose peut s&#39;exprimer avec la syntaxe DDL :

```sql
CREATE QUOTA my_quota KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO my_user;
```

Dans cet exemple, l’utilisateur peut exécuter jusqu’à 100 occurrences de chaque requête normalisée distincte par heure. `SELECT number FROM numbers(1)` et `SELECT number FROM numbers(2)` partagent le même bucket (car ils ont la même forme normalisée), mais `SELECT number, number FROM numbers(1)` utilise un bucket distinct.

Le quota est attribué aux utilisateurs dans la section &#39;users&#39; de la config. Voir la section &quot;Droits d’accès&quot;.

Pour le traitement distribué des requêtes, les quantités cumulées sont stockées sur le serveur à l’origine de la requête. Ainsi, si l’utilisateur passe à un autre serveur, le quota y &quot;repartira de zéro&quot;.

Lorsque le serveur redémarre, les quotas sont réinitialisés.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Créer des applications monopage avec ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)