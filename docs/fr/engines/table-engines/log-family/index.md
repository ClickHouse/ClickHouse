---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Journal De La Famille
toc_title: Introduction
---

# Famille De Moteurs En Rondins {#log-engine-family}

Ces moteurs ont été développés pour les scénarios où vous devez écrire rapidement de nombreuses petites tables (jusqu'à environ 1 million de lignes) et les lire plus tard dans leur ensemble.

Les moteurs de la famille:

-   [StripeLog](stripelog.md)
-   [Journal](log.md)
-   [TinyLog](tinylog.md)

## Propriétés Communes {#common-properties}

Moteur:

-   Stocker des données sur un disque.

-   Ajouter des données à la fin du fichier lors de l'écriture.

-   Bloque simultanées dans l'accès aux données.

    Lors `INSERT` requêtes, la table est verrouillée, et d'autres requêtes pour la lecture et l'écriture de données attendent que la table se déverrouille. S'il n'y a pas de requêtes d'écriture de données, un certain nombre de requêtes de lecture de données peuvent être effectuées simultanément.

-   Ne prennent pas en charge [mutation](../../../sql-reference/statements/alter.md#alter-mutations) opérations.

-   Ne prennent pas en charge les index.

    Cela signifie que `SELECT` les requêtes pour les plages de données ne sont pas efficaces.

-   N'écrivez pas de données de manière atomique.

    Vous pouvez obtenir une table avec des données corrompues si quelque chose interrompt l'opération d'écriture, par exemple, un arrêt anormal du serveur.

## Différence {#differences}

Le `TinyLog` le moteur est le plus simple de la famille et offre la fonctionnalité la plus pauvre et la plus faible efficacité. Le `TinyLog` le moteur ne prend pas en charge la lecture de données parallèles par plusieurs threads. Il lit les données plus lentement que les autres moteurs de la famille qui prennent en charge la lecture parallèle et utilise presque autant de descripteurs que `Log` moteur, car il stocke chaque colonne dans un fichier séparé. Utilisez-le dans des scénarios simples à faible charge.

Le `Log` et `StripeLog` les moteurs prennent en charge la lecture de données parallèle. Lors de la lecture de données, ClickHouse utilise plusieurs threads. Chaque thread traite un bloc de données séparé. Le `Log` le moteur utilise un fichier distinct pour chaque colonne de la table. `StripeLog` stocke toutes les données dans un seul fichier. En conséquence, la `StripeLog` moteur utilise moins de descripteurs dans le système d'exploitation, mais le `Log` moteur fournit une plus grande efficacité lors de la lecture des données.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/log_family/) <!--hide-->
