---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 14
toc_title: "R\xE9cr\xE9ation"
---

# Clickhouse Aire De Jeux {#clickhouse-playground}

[Clickhouse Aire De Jeux](https://play.clickhouse.tech?file=welcome) permet aux utilisateurs d'expérimenter avec ClickHouse en exécutant des requêtes instantanément, sans configurer leur serveur ou leur cluster.
Plusieurs exemples de jeux de données sont disponibles dans le terrain de jeu ainsi que des exemples de requêtes qui montrent les fonctionnalités de ClickHouse.

Les requêtes sont exécutées comme un utilisateur en lecture seule. Cela implique certaines limites:

-   Les requêtes DDL ne sont pas autorisées
-   Les requêtes D'insertion ne sont pas autorisées

Les paramètres suivants sont également appliquées:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

Clickhouse Playground donne l'expérience du m2.Petite
[Service géré pour ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
exemple hébergé dans [Yandex.Nuage](https://cloud.yandex.com/).
Plus d'informations sur [les fournisseurs de cloud](../commercial/cloud.md).

Clickhouse Playground interface web fait des demandes via ClickHouse [HTTP API](../interfaces/http.md).
Le backend Playground est juste un cluster ClickHouse sans aucune application Côté Serveur supplémentaire.
ClickHouse HTTPS endpoint est également disponible dans le cadre du terrain de jeu.

Vous pouvez effectuer des requêtes sur playground en utilisant n'importe quel client HTTP, par exemple [curl](https://curl.haxx.se) ou [wget](https://www.gnu.org/software/wget/), ou configurer une connexion en utilisant [JDBC](../interfaces/jdbc.md) ou [ODBC](../interfaces/odbc.md) pilote.
Plus d'informations sur les produits logiciels qui prennent en charge ClickHouse est disponible [ici](../interfaces/index.md).

| Paramètre    | Valeur                                        |
|:-------------|:----------------------------------------------|
| Terminaison  | https://play-api.clickhouse.technologie: 8443 |
| Utilisateur  | `playground`                                  |
| Mot de passe | `clickhouse`                                  |

Notez que ce paramètre nécessite une connexion sécurisée.

Exemple:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
