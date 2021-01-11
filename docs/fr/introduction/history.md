---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 7
toc_title: Histoire
---

# Histoire De ClickHouse {#clickhouse-history}

ClickHouse a été développé initialement au pouvoir [Yandex.Metrica](https://metrica.yandex.com/), [la deuxième plus grande plateforme d'analyse dans le monde](http://w3techs.com/technologies/overview/traffic_analysis/all) et continue à être le composant de base de ce système. Avec plus de 13 Billions d'enregistrements dans la base de données et plus de 20 milliards d'événements par jour, ClickHouse permet de générer des rapports personnalisés à la volée directement à partir de données non agrégées. Cet article couvre brièvement les objectifs de ClickHouse dans les premiers stades de son développement.

Yandex.Metrica construit des rapports personnalisés à la volée en fonction des hits et des sessions, avec des segments arbitraires définis par l'utilisateur. Faisant souvent requiert la construction d'agrégats complexes, tels que le nombre d'utilisateurs uniques. De nouvelles données pour la création d'un rapport arrivent en temps réel.

En avril 2014, Yandex.Metrica suivait environ 12 milliards d'événements (pages vues et clics) par jour. Tous ces événements doivent être stockés à créer des rapports personnalisés. Une seule requête peut exiger de la numérisation de millions de lignes en quelques centaines de millisecondes, ou des centaines de millions de lignes en quelques secondes.

## Utilisation dans Yandex.Metrica et autres Services Yandex {#usage-in-yandex-metrica-and-other-yandex-services}

ClickHouse sert à des fins multiples dans Yandex.Metrica.
Sa tâche principale est de créer des rapports en mode en ligne en utilisant des données non agrégées. Il utilise un cluster de 374 serveurs qui stockent plus de 20,3 billions de lignes dans la base de données. Le volume de données compressées est d'environ 2 PB, sans tenir compte des doublons et des répliques. Le volume de données non compressées (au format TSV) serait d'environ 17 PB.

ClickHouse joue également un rôle clé dans les processus suivants:

-   Stockage des données pour la relecture de Session de Yandex.Metrica.
-   Traitement des données intermédiaires.
-   Création de rapports globaux avec Analytics.
-   Exécution de requêtes pour le débogage du Yandex.Moteur Metrica.
-   Analyse des journaux de L'API et de l'interface utilisateur.

De nos jours, il existe plusieurs dizaines d'installations ClickHouse dans D'autres services et départements Yandex: recherche verticale, E-commerce, Publicité, business analytics, développement mobile, Services personnels et autres.

## Données agrégées et non agrégées {#aggregated-and-non-aggregated-data}

Il y a une opinion répandue que pour calculer efficacement les statistiques, vous devez agréger les données car cela réduit le volume de données.

Mais l'agrégation de données est livré avec beaucoup de limitations:

-   Vous devez disposer d'une liste prédéfinie des rapports requis.
-   L'utilisateur ne peut pas créer de rapports personnalisés.
-   Lors de l'agrégation sur un grand nombre de clés distinctes, le volume de données est à peine réduit, l'agrégation est donc inutile.
-   Pour un grand nombre de rapports, il y a trop de variations d'agrégation (explosion combinatoire).
-   Lors de l'agrégation de clés avec une cardinalité élevée (telles que les URL), le volume de données n'est pas réduit de beaucoup (moins de deux fois).
-   Pour cette raison, le volume de données avec l'agrégation peut augmenter au lieu de diminuer.
-   Les utilisateurs ne voient pas tous les rapports que nous générons pour eux. Une grande partie de ces calculs est inutile.
-   L'intégrité logique des données peut être violée pour diverses agrégations.

Si nous n'agrégeons rien et travaillons avec des données non agrégées, cela pourrait réduire le volume des calculs.

Cependant, avec l'agrégation, une partie importante du travail est déconnectée et achevée relativement calmement. En revanche, les calculs en ligne nécessitent un calcul aussi rapide que possible, car l'utilisateur attend le résultat.

Yandex.Metrica dispose d'un système spécialisé d'agrégation des données appelé Metrage, qui a été utilisé pour la majorité des rapports.
À partir de 2009, Yandex.Metrica a également utilisé une base de données OLAP spécialisée pour les données non agrégées appelée OLAPServer, qui était auparavant utilisée pour le générateur de rapports.
OLAPServer a bien fonctionné pour les données non agrégées, mais il avait de nombreuses restrictions qui ne lui permettaient pas d'être utilisé pour tous les rapports comme souhaité. Ceux-ci comprenaient le manque de prise en charge des types de données (uniquement des nombres) et l'incapacité de mettre à jour progressivement les données en temps réel (cela ne pouvait être fait qu'en réécrivant les données quotidiennement). OLAPServer n'est pas un SGBD, mais une base de données spécialisée.

L'objectif initial de ClickHouse était de supprimer les limites D'OLAPServer et de résoudre le problème du travail avec des données non agrégées pour tous les rapports, mais au fil des ans, il est devenu un système de gestion de base de données polyvalent adapté à un large éventail de tâches analytiques.

[Article Original](https://clickhouse.tech/docs/en/introduction/history/) <!--hide-->
