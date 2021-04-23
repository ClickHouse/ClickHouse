---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 29
toc_title: Proxy
---

# Serveurs Proxy de développeurs tiers {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy), est un proxy HTTP et un équilibreur de charge pour la base de données ClickHouse.

Caractéristique:

-   Routage par utilisateur et mise en cache des réponses.
-   Limites flexibles.
-   Renouvellement automatique du certificat SSL.

Mis en œuvre dans Go.

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) est conçu pour être un proxy local entre ClickHouse et serveur d'applications dans le cas où il est impossible ou gênant d'insérer des données en mémoire tampon du côté de votre application.

Caractéristique:

-   En mémoire et sur disque de données en mémoire tampon.
-   Routage par table.
-   Équilibrage de charge et vérification de la santé.

Mis en œuvre dans Go.

## ClickHouse-Vrac {#clickhouse-bulk}

[ClickHouse-Vrac](https://github.com/nikepan/clickhouse-bulk) est un collecteur simple D'insertion de ClickHouse.

Caractéristique:

-   Groupez les demandes et envoyez-les par seuil ou intervalle.
-   Plusieurs serveurs distants.
-   L'authentification de base.

Mis en œuvre dans Go.

[Article Original](https://clickhouse.tech/docs/en/interfaces/third-party/proxy/) <!--hide-->
