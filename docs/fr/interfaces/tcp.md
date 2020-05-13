---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 18
toc_title: Interface Native (TCP)
---

# Interface Native (TCP) {#native-interface-tcp}

Le protocole natif est utilisé dans le [client de ligne de commande](cli.md), pour la communication inter-serveur pendant le traitement de requête distribué, et également dans d’autres programmes C++. Malheureusement, le protocole clickhouse natif n’a pas encore de spécification formelle, mais il peut être rétro-conçu à partir du code source ClickHouse (démarrage [ici](https://github.com/ClickHouse/ClickHouse/tree/master/dbms/Client)) et/ou en interceptant et en analysant le trafic TCP.

[Article Original](https://clickhouse.tech/docs/en/interfaces/tcp/) <!--hide-->
