---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 28
toc_title: Les Interfaces Visuelles
---

# Interfaces visuelles de développeurs tiers {#visual-interfaces-from-third-party-developers}

## Open-Source {#open-source}

### Tabix {#tabix}

Interface Web pour ClickHouse dans le [Tabix](https://github.com/tabixio/tabix) projet.

Caractéristique:

-   Fonctionne avec ClickHouse directement à partir du navigateur, sans avoir besoin d'installer un logiciel supplémentaire.
-   Éditeur de requête avec coloration syntaxique.
-   L'Auto-complétion des commandes.
-   Outils d'analyse graphique de l'exécution des requêtes.
-   Options de jeu de couleurs.

[Documentation Tabix](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) est une interface utilisateur / IDE pour OSX, Linux et Windows.

Caractéristique:

-   Générateur de requêtes avec coloration syntaxique. Affichez la réponse dans une table ou une vue JSON.
-   Exporter les résultats de la requête au format CSV ou JSON.
-   Liste des processus avec des descriptions. Le mode d'écriture. Capacité à arrêter (`KILL`) processus.
-   Base de données du graphique. Affiche toutes les tables et leurs colonnes avec des informations supplémentaires.
-   Une vue rapide de la taille de la colonne.
-   La configuration du serveur.

Les fonctionnalités suivantes sont prévues pour le développement:

-   Gestion de base de données.
-   La gestion des utilisateurs.
-   En temps réel l'analyse des données.
-   Surveillance de Cluster.
-   La gestion de Cluster.
-   Suivi des tables répliquées et Kafka.

### Phare {#lighthouse}

[Phare](https://github.com/VKCOM/lighthouse) est une interface web légère pour ClickHouse.

Caractéristique:

-   Liste de Table avec filtrage et métadonnées.
-   Aperçu de la Table avec filtrage et tri.
-   Les requêtes en lecture seule exécution.

### Redash {#redash}

[Redash](https://github.com/getredash/redash) est une plate-forme pour la visualisation des données.

Prise en charge de plusieurs sources de données, y compris ClickHouse, Redash peut joindre les résultats des requêtes provenant de différentes sources de données dans un ensemble de données final.

Caractéristique:

-   Puissant éditeur de requêtes.
-   Explorateur de base de données.
-   Des outils de visualisation qui vous permettent de représenter des données sous différentes formes.

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - client de base de données de bureau universel avec support ClickHouse.

Caractéristique:

-   Développement de requêtes avec mise en évidence de la syntaxe et complétion automatique.
-   Liste de Table avec filtres et recherche de métadonnées.
-   Aperçu des données de la Table.
-   Recherche en texte intégral.

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) est un client de ligne de commande alternatif pour ClickHouse, écrit en Python 3.

Caractéristique:

-   Complétion.
-   Coloration syntaxique pour les requêtes et la sortie de données.
-   Support Pager pour la sortie de données.
-   Commandes personnalisées de type PostgreSQL.

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) est un outil spécialisé pour visualiser la `system.trace_log` comme [flamegraph](http://www.brendangregg.com/flamegraphs.html).

### clickhouse-plantuml {#clickhouse-plantuml}

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) est un script à générer [PlantUML](https://plantuml.com/) schéma des schémas des tableaux.

## Commercial {#commercial}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) est un IDE de base de données de JetBrains avec un support dédié pour ClickHouse. Il est également intégré dans D'autres outils basés sur IntelliJ: PyCharm, IntelliJ IDEA, GoLand, PhpStorm et autres.

Caractéristique:

-   Achèvement du code très rapide.
-   Mise en évidence de la syntaxe ClickHouse.
-   Prise en charge des fonctionnalités spécifiques à ClickHouse, par exemple, les colonnes imbriquées, les moteurs de table.
-   Éditeur De Données.
-   Refactoring.
-   Recherche et Navigation.

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens) est un service de visualisation et d'analyse de données.

Caractéristique:

-   Large gamme de visualisations disponibles, des graphiques à barres simples aux tableaux de bord complexes.
-   Les tableaux de bord pourraient être rendus publics.
-   Prise en charge de plusieurs sources de données, y compris ClickHouse.
-   Stockage de données matérialisées basé sur ClickHouse.

DataLens est [disponible gratuitement](https://cloud.yandex.com/docs/datalens/pricing) pour les projets à faible charge, même pour un usage commercial.

-   [Documentation DataLens](https://cloud.yandex.com/docs/datalens/).
-   [Tutoriel](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) sur la visualisation des données à partir d'une base de données ClickHouse.

### Logiciel Holistics {#holistics-software}

[Holistics](https://www.holistics.io/) est une plate-forme de données à pile complète et un outil de business intelligence.

Caractéristique:

-   E-mail automatisé, Slack et Google Sheet horaires de rapports.
-   Éditeur SQL avec visualisations, contrôle de version, auto-complétion, composants de requête réutilisables et filtres dynamiques.
-   Analyse intégrée des rapports et des tableaux de bord via iframe.
-   Préparation des données et capacités ETL.
-   Prise en charge de la modélisation des données SQL pour la cartographie relationnelle des données.

### Looker {#looker}

[Looker](https://looker.com) est une plate-forme de données et un outil de business intelligence avec prise en charge de plus de 50 dialectes de base de données, y compris ClickHouse. Looker est disponible en tant que plate-forme SaaS et auto-hébergé. Les utilisateurs peuvent utiliser Looker via le navigateur pour explorer les données, créer des visualisations et des tableaux de bord, planifier des rapports et partager leurs idées avec des collègues. Looker fournit un riche ensemble d'outils pour intégrer ces fonctionnalités dans d'autres applications, et une API
pour intégrer les données avec d'autres applications.

Caractéristique:

-   Développement facile et agile en utilisant LookML, un langage qui prend en charge curated
    [La Modélisation Des Données](https://looker.com/platform/data-modeling) pour soutenir les auteurs et les utilisateurs finaux.
-   Intégration de flux de travail puissante via Looker [Actions De Données](https://looker.com/platform/actions).

[Comment configurer ClickHouse dans Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[Article Original](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
