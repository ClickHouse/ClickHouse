---
description: 'Limites des connexions TCP.'
sidebar_label: 'Limites des connexions TCP'
slug: /operations/settings/tcp-connection-limits
title: 'Limites des connexions TCP'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

Il est possible qu’une connexion TCP ClickHouse (c’est-à-dire une connexion via le [client en ligne de commande](https://clickhouse.com/docs/interfaces/client))
se déconnecte automatiquement après un certain nombre de requêtes ou une certaine durée.
Après la déconnexion, aucune reconnexion automatique n’a lieu (sauf si elle est déclenchée par autre chose,
par exemple l’envoi d’une autre requête dans le client en ligne de commande).

Les limites de connexion sont activées en définissant les paramètres du serveur
`tcp_close_connection_after_queries_num` (pour la limite de requêtes)
ou `tcp_close_connection_after_queries_seconds` (pour la limite de durée) sur une valeur supérieure à 0.
Si les deux limites sont activées, la connexion se ferme dès que l’une des deux est atteinte.

Lorsqu’une limite est atteinte et que la déconnexion a lieu, le client reçoit une
exception `TCP_CONNECTION_LIMIT_REACHED`, et **la requête à l’origine de la déconnexion n’est jamais traitée**.

<div id="query-limits">
  ## Limites de requêtes
</div>

En supposant que `tcp_close_connection_after_queries_num` soit défini sur N, la connexion autorise
N requêtes réussies. Ensuite, à la requête N + 1, le client se déconnecte.

Chaque requête traitée est comptabilisée dans la limite de requêtes. Ainsi, lors de la connexion d’un client en ligne de commande,
une requête automatique initiale sur les avertissements système peut être exécutée et compter dans la limite.

Lorsqu’une connexion TCP est inactive (c.-à-d. n’a traité aucune requête pendant une certaine durée,
spécifiée par le paramètre de session `poll_interval`), le nombre de requêtes comptabilisées jusque-là est réinitialisé à 0.
Cela signifie que le nombre total de requêtes sur une même connexion peut dépasser
`tcp_close_connection_after_queries_num` en cas de période d’inactivité.

<div id="duration-limits">
  ## Limites de durée
</div>

La durée de la connexion est mesurée à partir du moment où le client se connecte.
Le client est déconnecté lors de la première requête une fois `tcp_close_connection_after_queries_seconds` secondes écoulées.