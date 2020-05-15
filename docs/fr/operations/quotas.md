---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: Quota
---

# Quota {#quotas}

Les Quotas permettent de limiter l'utilisation des ressources au cours d'une période de temps ou de suivre l'utilisation des ressources.
Les Quotas sont configurés dans la configuration utilisateur, qui est généralement ‘users.xml’.

Le système dispose également d'une fonctionnalité pour limiter la complexité d'une seule requête. Voir la section “Restrictions on query complexity”).

Contrairement aux restrictions de complexité des requêtes, les quotas:

-   Placez des restrictions sur un ensemble de requêtes qui peuvent être exécutées sur une période de temps, au lieu de limiter une seule requête.
-   Compte des ressources dépensées sur tous les serveurs distants pour le traitement des requêtes distribuées.

Regardons la section de la ‘users.xml’ fichier qui définit les quotas.

``` xml
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
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

Par défaut, le quota suit la consommation de ressources pour chaque heure, sans limiter l'utilisation.
La consommation de ressources calculé pour chaque intervalle est sortie dans le journal du serveur après chaque demande.

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

Pour l' ‘statbox’ quota, restrictions sont fixées pour toutes les heures et pour toutes les 24 heures (86 400 secondes). L'intervalle de temps est compté, à partir d'un moment fixe défini par l'implémentation. En d'autres termes, l'intervalle de 24 heures ne commence pas nécessairement à minuit.

Lorsque l'intervalle se termine, toutes les valeurs collectées sont effacées. Pour l'heure suivante, le calcul du quota recommence.

Voici les montants qui peuvent être restreint:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as a result.

`read_rows` – The total number of source rows read from tables for running the query on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

Si la limite est dépassée pendant au moins un intervalle de temps, une exception est levée avec un texte indiquant quelle restriction a été dépassée, pour quel intervalle et quand le nouvel intervalle commence (lorsque les requêtes peuvent être envoyées à nouveau).

Les Quotas peuvent utiliser le “quota key” fonctionnalité de rapport sur les ressources pour plusieurs clés indépendamment. Voici un exemple de ce:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

Le quota est attribué aux utilisateurs dans le ‘users’ section de la configuration. Voir la section “Access rights”.

Pour le traitement des requêtes distribuées, les montants accumulés sont stockés sur le serveur demandeur. Donc, si l'utilisateur se rend sur un autre serveur, le quota y sera “start over”.

Lorsque le serveur est redémarré, les quotas sont réinitialisés.

[Article Original](https://clickhouse.tech/docs/en/operations/quotas/) <!--hide-->
