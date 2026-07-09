---
description: 'Décrit les solutions proxy tierces disponibles pour ClickHouse'
sidebar_label: 'Proxies'
sidebar_position: 29
slug: /interfaces/third-party/proxy
title: 'Serveurs proxy développés par des tiers'
doc_type: 'référence'
---

<div id="chproxy">
  ## chproxy
</div>

[chproxy](https://github.com/Vertamedia/chproxy) est un proxy HTTP et un load balancer pour la base de données ClickHouse.

Fonctionnalités :

* Routage par utilisateur et mise en cache des réponses.
* Limites flexibles.
* Renouvellement automatique des certificats SSL.

Implémenté en Go.

<div id="kittenhouse">
  ## KittenHouse
</div>

[KittenHouse](https://github.com/VKCOM/kittenhouse) est conçu pour servir de proxy local entre ClickHouse et le serveur applicatif lorsqu&#39;il est impossible ou peu pratique de mettre en tampon les données d&#39;`INSERT` côté application.

Fonctionnalités :

* Mise en tampon des données en mémoire et sur disque.
* Routage par table.
* Répartition de charge et contrôle d&#39;état.

Implémenté en Go.

<div id="clickhouse-bulk">
  ## ClickHouse-Bulk
</div>

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) est un collecteur simple pour les insertions dans ClickHouse.

Fonctionnalités :

* Regroupe les requêtes et les envoie selon un seuil ou un intervalle.
* Plusieurs serveurs distants.
* Authentification de base.

Implémenté en Go.