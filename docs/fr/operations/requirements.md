---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Exigence
---

# Exigence {#requirements}

## CPU {#cpu}

Pour l'installation à partir de paquets deb prédéfinis, utilisez un processeur avec l'architecture x86\_64 et la prise en charge des instructions SSE 4.2. Pour exécuter ClickHouse avec des processeurs qui ne prennent pas en charge SSE 4.2 ou qui ont une architecture AArch64 ou PowerPC64LE, vous devez créer ClickHouse à partir de sources.

ClickHouse implémente le traitement parallèle des données et utilise toutes les ressources matérielles disponibles. Lors du choix d'un processeur, tenez compte du fait que ClickHouse fonctionne plus efficacement dans les configurations avec un grand nombre de cœurs mais une fréquence d'horloge plus faible que dans les configurations avec moins de cœurs et une fréquence d'horloge plus élevée. Par exemple, 16 cœurs avec 2600 MHz est préférable à 8 cœurs avec 3600 MHz.

Il est recommandé d'utiliser **Turbo Boost** et **la technologie hyper-threading** technologie. Il améliore considérablement les performances avec une charge de travail typique.

## RAM {#ram}

Nous vous recommandons d'utiliser un minimum de 4 Go de RAM pour effectuer des requêtes non triviales. Le serveur ClickHouse peut fonctionner avec une quantité beaucoup plus petite de RAM, mais il nécessite de la mémoire pour traiter les requêtes.

Le volume de RAM requis dépend de:

-   La complexité des requêtes.
-   La quantité de données traitées dans les requêtes.

Pour calculer le volume de RAM requis, vous devez estimer la taille des données temporaires pour [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) et d'autres opérations que vous utilisez.

ClickHouse peut utiliser la mémoire externe pour les données temporaires. Voir [Groupe par dans la mémoire externe](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) pour plus de détails.

## Fichier D'Échange {#swap-file}

Désactiver le fichier d'échange pour les environnements de production.

## Sous-Système De Stockage {#storage-subsystem}

Vous devez avoir 2 Go d'espace disque libre pour installer ClickHouse.

Le volume de stockage requis pour vos données doit être calculé séparément. L'évaluation devrait inclure:

-   Estimation du volume de données.

    Vous pouvez prendre un échantillon des données et obtenir la taille moyenne d'une ligne. Ensuite, multipliez la valeur par le nombre de lignes que vous souhaitez stocker.

-   Le coefficient de compression des données.

    Pour estimer le coefficient de compression des données, chargez un échantillon de vos données dans ClickHouse et comparez la taille réelle des données avec la taille de la table stockée. Par exemple, les données de flux de clics sont généralement compressées de 6 à 10 fois.

Pour calculer le volume final de données à stocker, appliquez le coefficient de compression au volume de données estimé. Si vous prévoyez de stocker des données dans plusieurs répliques, puis multipliez le volume estimé par le nombre de réplicas.

## Réseau {#network}

Si possible, utilisez des réseaux de classe 10G ou supérieure.

La bande passante du réseau est essentielle pour traiter les requêtes distribuées avec une grande quantité de données intermédiaires. En outre, la vitesse du réseau affecte les processus de réplication.

## Logiciel {#software}

ClickHouse est développé principalement pour la famille de systèmes D'exploitation Linux. La distribution Linux recommandée est Ubuntu. Le `tzdata` paquet doit être installé dans le système.

ClickHouse peut également fonctionner dans d'autres familles de systèmes d'exploitation. Voir les détails dans le [Prise en main](../getting-started/index.md) section de la documentation.
