---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 6
toc_title: Performance
---

# Performance {#performance}

Selon les résultats des tests internes chez Yandex, ClickHouse affiche les meilleures performances (à la fois le débit le plus élevé pour les requêtes longues et la latence la plus faible pour les requêtes courtes) pour des scénarios d'exploitation comparables parmi les systèmes de sa classe disponibles pour les tests. Vous pouvez afficher les résultats du test sur un [page séparée](https://clickhouse.tech/benchmark/dbms/).

De nombreux points de repère indépendants sont arrivés à des conclusions similaires. Ils ne sont pas difficiles à trouver en utilisant une recherche sur internet, ou vous pouvez voir [notre petite collection de liens](https://clickhouse.tech/#independent-benchmarks).

## Débit pour une seule grande requête {#throughput-for-a-single-large-query}

Le débit peut être mesuré en lignes par seconde ou en mégaoctets par seconde. Si les données sont placées dans le cache de page, une requête pas trop complexe est traitée sur du matériel moderne à une vitesse d'environ 2-10 GB / s de données non compressées sur un seul serveur (pour les cas les plus simples, la vitesse peut atteindre 30 GB/s). Si les données ne sont pas placées dans le cache de page, la vitesse dépend du sous-système de disque et du taux de compression des données. Par exemple, si le sous-système de disque permet de lire des données à 400 Mo/s et que le taux de compression des données est de 3, la vitesse devrait être d'environ 1,2 Go/s. Pour obtenir la vitesse en lignes par seconde, divisez la vitesse en octets par seconde par la taille totale des colonnes utilisées dans la requête. Par exemple, si 10 octets de colonnes sont extraites, la vitesse devrait être d'environ 100 à 200 millions de lignes par seconde.

La vitesse de traitement augmente presque linéairement pour le traitement distribué, mais seulement si le nombre de lignes résultant de l'agrégation ou du tri n'est pas trop important.

## Latence Lors Du Traitement Des Requêtes Courtes {#latency-when-processing-short-queries}

Si une requête utilise une clé primaire et ne sélectionne pas trop de colonnes et de lignes à traiter (des centaines de milliers), Vous pouvez vous attendre à moins de 50 millisecondes de latence (un seul chiffre de millisecondes dans le meilleur des cas) si les données sont placées dans le cache de page. Sinon, la latence est principalement dominée par le nombre de recherches. Si vous utilisez des lecteurs de disque rotatifs, pour un système qui n'est pas surchargé, la latence peut être estimée avec cette formule: `seek time (10 ms) * count of columns queried * count of data parts`.

## Débit lors du traitement d'une grande quantité de requêtes courtes {#throughput-when-processing-a-large-quantity-of-short-queries}

Dans les mêmes conditions, ClickHouse peut traiter plusieurs centaines de requêtes par seconde sur un seul serveur (jusqu'à plusieurs milliers dans le meilleur des cas). Étant donné que ce scénario n'est pas typique pour les SGBD analytiques, nous vous recommandons d'attendre un maximum de 100 requêtes par seconde.

## Performances Lors De L'Insertion De Données {#performance-when-inserting-data}

Nous vous recommandons d'insérer des données dans des paquets d'au moins 1000 lignes, ou pas plus qu'une seule demande par seconde. Lors de l'insertion dans une table MergeTree à partir d'un dump séparé par des tabulations, la vitesse d'insertion peut être de 50 à 200 Mo/s. Si les lignes insérées ont une taille d'environ 1 KO, La vitesse sera de 50 000 à 200 000 lignes par seconde. Si les lignes sont petites, les performances peuvent être plus élevées en lignes par seconde (sur les données du système de bannière -`>` 500 000 lignes par seconde; sur les données de Graphite -`>` 1 000 000 lignes par seconde). Pour améliorer les performances, vous pouvez effectuer plusieurs requêtes D'insertion en parallèle, qui s'adaptent linéairement.

[Article Original](https://clickhouse.tech/docs/en/introduction/performance/) <!--hide-->
