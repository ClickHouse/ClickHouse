---
description: 'Liste d’outils GUI et d’applications tierces pour utiliser ClickHouse'
sidebar_label: 'Interfaces visuelles'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: 'Interfaces visuelles tierces'
doc_type: 'reference'
---

<div id="open-source">
  ## Open source
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx) est une application de bureau développée avec Tauri et SvelteKit, qui offre une interface moderne pour explorer les données et exécuter des requêtes à l’aide du moteur de base de données embarqué de ClickHouse (chdb).

* S’appuie sur chdb lors de l’exécution de l’application native.
* Peut se connecter à une instance ClickHouse lors de l’exécution de l’instance web.
* Éditeur Monaco, pour une prise en main immédiate.
* Visualisations de données variées et en constante évolution.

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui) est une interface applicative React.js simple pour les bases de données ClickHouse, conçue pour exécuter des requêtes et visualiser les données. Développée avec React et le client ClickHouse pour le web, elle offre une interface utilisateur élégante et conviviale pour interagir facilement avec la base de données.

Fonctionnalités :

* ClickHouse Integration : gérez facilement les connexions et exécutez des requêtes.
* Gestion dynamique des onglets : gérez plusieurs onglets de manière dynamique, comme les onglets de requête et de table.
* Optimisations des performances : utilise IndexedDB pour une mise en cache efficace et la gestion de l’état.
* Stockage local des données : toutes les données sont stockées localement dans le navigateur, ce qui garantit qu’elles ne sont envoyées nulle part ailleurs.

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io) est un outil gratuit et open source permettant de visualiser et de concevoir des schémas de base de données, y compris pour ClickHouse, à partir d’une seule requête. Développé avec React, il offre une expérience fluide et conviviale, sans nécessiter d’identifiants de base de données ni d’inscription pour commencer.

Fonctionnalités :

* Visualisation du schéma : importez et visualisez instantanément votre schéma ClickHouse, y compris des diagrammes ER avec des vues matérialisées et des vues standard, montrant les références entre les tables.
* Export DDL assisté par IA : générez facilement des scripts DDL pour améliorer la gestion et la documentation du schéma.
* Prise en charge de plusieurs dialectes SQL : compatible avec une gamme de dialectes SQL, ce qui en fait un outil polyvalent pour différents environnements de base de données.
* Aucune inscription ni aucun identifiant requis : toutes les fonctionnalités sont accessibles directement dans le navigateur, pour une expérience simple et sécurisée.

[Code source de ChartDB](https://github.com/chartdb/chartdb).

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria) est une application de console web basée sur l’IA qui permet de gérer plusieurs clusters ClickHouse depuis une interface unique.

Fonctionnalités :

* **Intelligence basée sur l’IA** : utilisez le langage naturel pour explorer les données, optimiser et corriger les requêtes SQL, et visualiser vos données.
* **Intégration officielle de ClickHouse Agent Skills** : appuyez-vous sur les [meilleures pratiques officielles](https://github.com/ClickHouse/agent-skills) pour demander à l’IA des optimisations de base de données et des recommandations.
* **Diagnostic intelligent des erreurs** : repérez instantanément les erreurs de syntaxe grâce à une mise en surbrillance précise des lignes et des colonnes, et obtenez en un clic des suggestions de correction basées sur l’IA.
* **Inspection des tables système** : explorez en profondeur `system.query_log`, `system.query_views_log`, `system.zookeeper`, `system.ddl_distributed_queue`, `system.part_log` et `system.processes` grâce à un puissant tableau de bord de visualisation et à des filtres, afin de comprendre rapidement votre cluster.
* **Explain en un clic** : comprenez instantanément les plans d’exécution des requêtes grâce à des vues visuelles de l’AST et du pipeline.
* **Graphe de dépendances** : visualisez les relations entre les tables et suivez les flux de données à travers les vues matérialisées, les tables Distributed et les systèmes externes.
* **Supervision du cluster** : surveillez tous les nœuds avec des métriques en temps réel, les opérations de fusion, l’état de la réplication, les performances des requêtes, et bien plus encore.
* **Confidentialité et sécurité** : toutes les requêtes SQL s’exécutent directement depuis votre navigateur vers votre serveur ClickHouse, garantissant une confidentialité totale.

[Documentation de DataStoria](https://docs.datastoria.app).

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup) est un client de base de données moderne, multiplateforme et assisté par l’IA, avec prise en charge native de ClickHouse.

Fonctionnalités :

* Assistance IA pour les requêtes SQL avec suggestions intelligentes
* Prise en charge native des connexions ClickHouse avec gestion sécurisée des identifiants
* Interface soignée et accessible avec plusieurs thèmes (clair, sombre et variantes colorées)
* Filtrage avancé et exploration du résultat de la requête
* Compatibilité multiplateforme (macOS, Windows, Linux)
* Performances rapides et réactives
* Open source et sous licence MIT

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory) est un espace de travail SQL nativement conçu pour l’IA, avec une prise en charge de premier ordre de ClickHouse et une IA intégrée.

Fonctionnalités :

* Copilote IA pour la génération, l’explication et le débogage SQL
* Gestion et interrogation de plusieurs clusters ClickHouse depuis un espace de travail unifié
* Autocomplétion SQL basée sur le schéma et espace de travail de requête à plusieurs onglets
* Exploration interactive des résultat de la requête avec filtrage et visualisation
* Résumés de tables générés par l’IA pour mieux comprendre les jeux de données
* Connexions directes à ClickHouse avec prise en charge des tunnels SSH
* Interface moderne pensée pour les développeurs, avec modes clair et sombre et prise en charge des thèmes
* Application de bureau multiplateforme (macOS, Windows, Linux), avec prise en charge de Docker
* Open source et sous licence MIT

<div id="clickhouse-schemaflow-visualizer">
  ### Visualiseur de flux du schéma ClickHouse
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer) est une application web open source qui permet de visualiser les relations entre les tables ClickHouse.
Elle se connecte à une instance ClickHouse, analyse les métadonnées de `system.tables` (types de moteur, dépendances, requêtes SELECT des vues matérialisées) et génère des diagrammes interactifs de flux de données au niveau des tables, ainsi que des relations au niveau des colonnes avec l’expression de transformation indiquée sur chaque arête. Les diagrammes sont agencés avec Dagre et rendus sous forme de SVG intégré simple — aucun runtime de création de diagrammes côté client n’est chargé.

Fonctionnalités :

* Parcourez les bases de données et les tables ClickHouse via une barre latérale intuitive
* Vue Data Flow : sources amont au niveau des tables et vues matérialisées en aval
* Vue Relationships : correspondance au niveau des colonnes avec l’expression de transformation analysée sur chaque arête (par ex. `toStartOfHour(scheduled_departure)`, `avgState(delay_minutes)`)
* Icônes et code couleur adaptés aux moteurs pour `MergeTree`, `Replicated*`, `Distributed`, `MaterializedView` et `Dictionary`
* Cliquez sur une colonne dans la vue Relationships pour mettre en évidence l’intégralité de son chemin de données dans le pipeline
* Filtre dynamique dans la barre latérale et palette de commandes `Ctrl+K` / `⌘K` pour accéder rapidement à n’importe quelle table, colonne ou moteur
* Superposition facultative de métadonnées affichant le nombre de lignes et la taille sur disque par table
* Exportez le diagramme actuel sous forme de fichier HTML autonome
* Connexion TLS à ClickHouse, avec possibilité d’ignorer la vérification et d’utiliser des certificats CA / client personnalisés

[ClickHouse Schema Flow Visualizer - code source](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

Interface web pour ClickHouse du projet [Tabix](https://github.com/tabixio/tabix).

Fonctionnalités :

* Fonctionne avec ClickHouse directement depuis le navigateur, sans installer de logiciel supplémentaire.
* Éditeur de requêtes avec coloration syntaxique.
* Complétion automatique des commandes.
* Outils d’analyse graphique de l’exécution des requêtes.
* Options de palette de couleurs.

[Documentation de Tabix](https://tabix.io/doc/).

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps) est une interface/IDE pour OSX, Linux et Windows.

Fonctionnalités :

* Générateur de requêtes avec coloration syntaxique. Affichez la réponse dans un tableau ou en vue JSON.
* Exportez le résultat de la requête au format CSV ou JSON.
* Liste des processus avec leur description. Mode édition. Possibilité d&#39;arrêter (`KILL`) un processus.
* Graphe de la base de données. Affiche toutes les tables et leurs colonnes avec des informations supplémentaires.
* Aperçu rapide de la taille des colonnes.
* Configuration du serveur.

Les fonctionnalités suivantes sont prévues :

* Gestion de la base de données.
* Gestion des utilisateurs.
* Analyse des données en temps réel.
* Supervision du cluster.
* Gestion du cluster.
* Surveillance des tables répliquées et Kafka.

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse) est une interface web légère pour ClickHouse.

Fonctionnalités :

* Liste des tables avec filtrage et métadonnées.
* Aperçu des tables avec filtrage et tri.
* Exécution de requêtes en lecture seule.

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash) est une plateforme de visualisation de données.

Compatible avec plusieurs sources de données, dont ClickHouse, Redash peut combiner les résultats de requêtes issues de différentes sources de données en un seul jeu de données final.

Fonctionnalités :

* Éditeur de requêtes puissant.
* Explorateur de bases de données.
* Outil de visualisation permettant de représenter les données sous différentes formes.

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) est une plateforme de supervision et de visualisation.

« Grafana vous permet d’interroger, de visualiser, d’alerter sur vos métriques et de les comprendre, où qu’elles soient stockées. Créez, explorez et partagez des dashboards avec votre équipe, et favorisez une culture pilotée par les données. Adopté et apprécié par la communauté » — grafana.com.

Le plugin ClickHouse data source permet d’utiliser ClickHouse comme base de données back-end.

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in) est une stack d’observabilité polyglotte et haute performance pour ClickHouse *(anciennement cLoki)*, avec des intégrations Grafana natives permettant aux utilisateurs d’ingérer et d’analyser des logs, des métriques et des traces de télémétrie depuis n’importe quel agent prenant en charge Loki/LogQL, Prometheus/PromQL, OTLP/Tempo, Elastic, InfluxDB et bien d’autres.

Fonctionnalités :

* UI Explore intégrée et CLI LogQL pour interroger, extraire et visualiser les données
* Prise en charge native des API Grafana pour l’interrogation, le traitement, l’ingestion, le tracing et la génération d’alertes sans plugin
* Pipeline puissant pour rechercher, filtrer et extraire dynamiquement des données à partir des logs, événements, traces et plus encore
* API d’ingestion et PUSH compatibles de manière transparente avec LogQL, PromQL, InfluxDB, Elastic et bien d’autres
* Prêt à l’emploi avec des agents tels que Promtail, Grafana-Agent, Vector, Logstash, Telegraf et bien d’autres

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - client de bases de données de bureau universel avec prise en charge de ClickHouse.

Fonctionnalités :

* Rédaction de requêtes avec coloration syntaxique et autocomplétion.
* Liste des tables avec filtres et recherche dans les métadonnées.
* Aperçu des données des tables.
* Recherche plein texte.

Par défaut, DBeaver n&#39;établit pas de connexion avec une session (le CLI, par exemple, le fait). Si vous avez besoin de la prise en charge des sessions (par exemple pour définir des paramètres pour votre session), modifiez les propriétés de connexion du pilote et définissez `session_id` sur une chaîne aléatoire (il utilise la connexion HTTP en arrière-plan). Vous pourrez ensuite utiliser n&#39;importe quel paramètre depuis la fenêtre de requête.

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) est un client en ligne de commande alternatif pour ClickHouse, écrit en Python 3.

Fonctionnalités :

* Autocomplétion.
* Coloration syntaxique des requêtes et de la sortie des données.
* Prise en charge d’un pager pour la sortie des données.
* Commandes personnalisées de type PostgreSQL.

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) est un outil spécialisé qui permet de visualiser `system.trace_log` sous forme de [flamegraph](http://www.brendangregg.com/flamegraphs.html).

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) est un script qui permet de générer un diagramme [PlantUML](https://plantuml.com/) des schémas de tables.

<div id="clickhouse-table-graph">
  ### ClickHouse table graph
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph) est un outil CLI simple qui permet de visualiser les dépendances entre les tables ClickHouse. Cet outil récupère les connexions entre les tables à partir de la table `system.tables` et génère un diagramme de flux des dépendances au format [mermaid](https://mermaid.js.org/syntax/flowchart.html). Avec cet outil, vous pouvez facilement visualiser les dépendances entre les tables et comprendre le flux de données dans votre base de données ClickHouse. Grâce à mermaid, le diagramme obtenu est esthétique et peut être facilement ajouté à votre documentation Markdown.

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) est un noyau Jupyter pour ClickHouse, qui permet d’interroger des données ClickHouse en SQL dans Jupyter.

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/) est une couche d’IA open source pour les bases de données, dont ClickHouse, qui vous permet de développer, d’entraîner et de déployer facilement des modèles de machine learning de pointe. MindsDB Studio(GUI) vous permet d’entraîner de nouveaux modèles à partir d’une base de données, d’interpréter les prédictions du modèle, d’identifier d’éventuels biais dans les données, ainsi que d’évaluer et de visualiser la précision du modèle grâce à la fonctionnalité d’IA explicable, afin d’adapter et d’affiner plus rapidement vos modèles de machine learning.

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) est un outil visuel de gestion pour ClickHouse !

Fonctionnalités :

* Prend en charge l’historique des requêtes (pagination, tout effacer, etc.)
* Prend en charge les requêtes avec des clauses SQL sélectionnées
* Prend en charge l’interruption des requêtes
* Prend en charge la gestion des tables (métadonnées, suppression, aperçu)
* Prend en charge la gestion des bases de données (suppression, création)
* Prend en charge les requêtes personnalisées
* Prend en charge la gestion de plusieurs sources de données (test de connexion, monitoring)
* Prend en charge la supervision (processor, connexion, requête)
* Prend en charge la migration de données

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com) est un outil web open source de gestion des changements de schéma et de contrôle de version pour les équipes. Il prend en charge diverses bases de données, dont ClickHouse.

Fonctionnalités :

* Revue de schéma entre développeurs et DBA.
* Database-as-Code : gestion de version du schéma dans des VCS comme GitLab et déclenchement du déploiement lors d’un commit de code.
* Déploiement simplifié avec une politique par environnement.
* Historique complet des migrations.
* Détection de la dérive de schéma.
* Sauvegarde et restauration.
* RBAC.

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse) est un interpréteur [Zeppelin](https://zeppelin.apache.org) pour ClickHouse. Par rapport à l’interpréteur JDBC, il permet de mieux contrôler les délais d’expiration des requêtes longues.

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat) est une interface conviviale qui vous permet de rechercher, d’explorer et de visualiser vos données ClickHouse.

Fonctionnalités :

* Un éditeur SQL en ligne qui permet d’exécuter votre code SQL sans installation.
* Vous pouvez observer tous les processus et toutes les mutations. Pour les processus non terminés, vous pouvez les interrompre dans l’interface.
* Les métriques incluent l’analyse du cluster, l’analyse des données et l’analyse des requêtes.

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) ClickVisual est une plateforme open source légère de requête, d’analyse de logs et de visualisation des alertes.

Fonctionnalités :

* Prend en charge la création en un clic de bibliothèques d’analyse de logs
* Prend en charge la gestion de la configuration de collecte des logs
* Prend en charge la configuration d’index personnalisée
* Prend en charge la configuration des alertes
* Prend en charge une granularité des permissions au niveau des bibliothèques et des tables

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate) est un client web Angular et une interface utilisateur permettant de rechercher et d’explorer des données dans ClickHouse.

Fonctionnalités :

* Autocomplétion des requêtes ClickHouse SQL
* Navigation rapide dans l’arborescence des bases de données et des tables
* Filtrage et tri avancés des résultats
* Documentation ClickHouse SQL intégrée
* Préréglages de requêtes et historique
* 100 % basé sur le navigateur, sans serveur/backend

Le client est disponible immédiatement via GitHub Pages : https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace) est un outil APM qui fournit du traçage distribué et des métriques, basés sur OpenTelemetry et ClickHouse.

Fonctionnalités :

* [Traçage OpenTelemetry](https://uptrace.dev/opentelemetry/distributed-tracing.html), métriques et logs.
* Notifications par e-mail/Slack/PagerDuty via AlertManager.
* Langage de requête de type SQL pour agréger les spans.
* Langage de type PromQL pour interroger les métriques.
* Tableaux de bord de métriques préconfigurés.
* Plusieurs utilisateurs/projets via une config YAML.

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring) est un tableau de bord Next.js simple qui s’appuie sur les tables `system.*` pour aider à surveiller votre cluster ClickHouse et à en fournir une vue d’ensemble.

Fonctionnalités :

* Suivi des requêtes : requêtes en cours, historique des requêtes, ressources des requêtes (mémoire, parts lues, file&#95;open, ...), requêtes les plus coûteuses, tables ou colonnes les plus utilisées, etc.
* Suivi du cluster : utilisation totale de la mémoire/du CPU, file d’attente distribuée, paramètres globaux, paramètres MergeTree, métriques, etc.
* Informations sur les tables et les parts : taille, nombre de lignes, compression, taille des parts, etc., avec un niveau de détail jusqu’à la colonne.
* Outils utiles : exploration des données ZooKeeper, EXPLAIN des requêtes, arrêt forcé des requêtes, etc.
* Graphiques de visualisation des métriques : requêtes et utilisation des ressources, nombre de merges et de mutations, performances des merges, performances des requêtes, etc.

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana) est un service léger qui vous permet de rechercher, d’explorer et de visualiser facilement les données ClickHouse à l’aide de l’interface native de Kibana.

Fonctionnalités :

* Traduit les requêtes de graphiques de l’interface native de Kibana en syntaxe de requête ClickHouse.
* Prend en charge des fonctionnalités avancées telles que l’échantillonnage et la mise en cache afin d’améliorer les performances des requêtes.
* Réduit la courbe d’apprentissage pour les utilisateurs qui migrent d’ElasticSearch vers ClickHouse.

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/) est une interface web moderne pour explorer les logs stockés dans ClickHouse. Elle offre une UI conviviale pour interroger, visualiser et gérer les données de logs avec un contrôle d’accès granulaire.

Fonctionnalités :

* UI claire et réactive, avec des filtres puissants et une sélection de champs personnalisable.
* Syntaxe FlyQL pour un filtrage des logs intuitif et expressif.
* Graphique temporel avec prise en charge du group-by, y compris pour les champs JSON imbriqués, Map et Array.
* Prise en charge facultative des requêtes `WHERE` en SQL brut pour un filtrage avancé (avec vérification des permissions).
* Vues enregistrées : enregistrez et partagez des configurations d’UI personnalisées pour les requêtes et l’agencement.
* Contrôle d’accès basé sur les rôles (RBAC) et intégration de l’authentification GitHub.
* Aucun agent ni composant supplémentaire n’est requis côté ClickHouse.

[Telescope Code source](https://github.com/iamtelescope/telescope) · [Démo en direct](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/) est une interface web moderne, puissante et conviviale pour gérer et surveiller les bases de données ClickHouse. Elle propose une suite complète d’outils permettant aux développeurs, analystes et administrateurs d’interagir efficacement avec leurs clusters ClickHouse. ClickHouse est une remarquable base de données analytique, mais sa gestion via la CLI ou des outils rudimentaires peut s’avérer complexe. ClickLens comble ce manque en offrant :

* Discover - Exploration de données flexible, dans l’esprit de Kibana, pour n’importe quelle table
* SQL Console - Rédigez, exécutez et analysez des requêtes avec coloration syntaxique et résultats en streaming
* Monitoring en temps réel - Surveillez l’état de santé de votre cluster, les performances des requêtes et l’utilisation des ressources
* Schema Explorer - Parcourez les bases de données, les tables, les colonnes, les parts, et bien plus encore
* Contrôle d’accès - Gérez les utilisateurs et les rôles directement depuis l’UI
* RBAC natif - Les permissions de votre UI sont dérivées directement de vos grants ClickHouse

[Code source de ClickLens](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com) est une interface web ClickHouse open source et auto-hébergée, conçue pour **les équipes qui exploitent ClickHouse en production**. La plupart des outils excellent dans un seul domaine — un espace de travail de requêtes, un tableau de bord, un assistant IA, un moniteur de cluster ; CHouse UI réunit *le tout* : une couche d&#39;accès pour les équipes, associée à la supervision d&#39;une flotte multi-cluster et à un SRE IA autonome en lecture seule. Contrairement aux clients qui nécessitent des identifiants de base de données directs, il les stocke chiffrés côté serveur et contrôle l&#39;accès avec sa propre couche de **Role-Based Access Control (RBAC)**, de sorte que le navigateur ne voit jamais de mot de passe ClickHouse.

Fonctionnalités :

* **Accès des équipes et sécurité** - RBAC au niveau de l&#39;application (rôles prédéfinis + rôles personnalisés, règles granulaires d&#39;accès aux données par base de données/table), journalisation d&#39;audit avec contexte de session réel, et identifiants côté serveur chiffrés en AES-256-GCM.
* **Flotte multi-cluster** - Surveillez chaque cluster configuré dans un seul panneau (statut, mémoire, queries actives, exceptions, mini-graphiques de tendance), chaque carte étant interrogée indépendamment, avec en arrière-plan un processus backend de collecte d&#39;instantanés.
* **Chouse AI — Fleet Doctor** - Un SRE IA autonome en lecture seule : il analyse la flotte avec un outil `SELECT` protégé, limité à `system.*` (ClickHouse `readonly=1`), identifie les causes racines et rédige un rapport structuré avec une analyse approfondie des requêtes lourdes et des réécritures suggérées. Il ne modifie jamais le cluster.
* **IA dans les onglets de supervision** - « Optimize with Chouse AI » sur une ligne de Query Logs (réécriture + estimation `EXPLAIN` avant→après + ouverture dans l&#39;espace de travail SQL), ainsi qu&#39;un bouton « Diagnose » en un clic sur une ligne `system.errors` ou une entrée du journal des parts.
* **Alertes de seuil** - Règles sur le % de mémoire du nœud, la mémoire par requête et les requêtes de longue durée, envoyées vers Slack et par e-mail — avec une analyse autonome de la cause racine jointe en cas de dépassement.
* **Espace de travail complet** - Éditeur SQL Monaco, explorateur de schéma, vue des requêtes en direct avec prise en charge de l&#39;arrêt, supervision native ClickHouse (répartition de la mémoire, parts/merges, retard des répliques, percentiles de latence), et import/export de données.

Open source (Apache 2.0), avec priorité au déploiement on-prem — toutes les fonctionnalités sont incluses, sans offre payante.

[Code source de CHouse UI](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow) est un outil open source permettant de visualiser les flux de données et les dépendances entre les tables, les vues et les vues matérialisées dans ClickHouse.

Fonctionnalités :

* Génère automatiquement un graphe du schéma à partir des métadonnées ClickHouse.
* Visualise les flux de données à travers les vues matérialisées.
* UI interactive pour explorer la structure du schéma.
* Exporte les diagrammes aux formats PDF ou SVG pour la documentation et le partage.
* Déploiement basé sur Docker pour une configuration rapide dans les environnements de développement.

<div id="commercial">
  ## Commercial
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/) est un IDE de base de données de JetBrains offrant une prise en charge dédiée de ClickHouse. Il est également intégré à d&#39;autres outils basés sur IntelliJ : PyCharm, IntelliJ IDEA, GoLand, PhpStorm, etc.

Fonctionnalités :

* Autocomplétion du code très rapide.
* Coloration syntaxique pour ClickHouse.
* Prise en charge des fonctionnalités propres à ClickHouse, par exemple les colonnes imbriquées et les moteurs de table.
* Éditeur de données.
* Refactorisations.
* Recherche et navigation.

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens) est un service de visualisation de données et d’analyse.

Fonctionnalités :

* Large choix de visualisations, des simples graphiques en barres aux tableaux de bord complexes.
* Les tableaux de bord peuvent être rendus accessibles au public.
* Prise en charge de multiples sources de données, notamment ClickHouse.
* Stockage de données matérialisées basé sur ClickHouse.

DataLens est [disponible gratuitement](https://yandex.cloud/en/docs/datalens/pricing) pour les projets à faible trafic, y compris pour un usage commercial.

* [Documentation DataLens](https://yandex.cloud/en/docs/datalens/).
* [Tutoriel](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization) sur la visualisation de données issues d’une base de données ClickHouse.

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/) est une plateforme de données full-stack et un outil de business intelligence.

Fonctionnalités :

* Planification automatisée de l’envoi de rapports par e-mail, sur Slack et dans Google Sheets.
* Éditeur SQL avec visualisations, gestion de versions, autocomplétion, éléments de requête réutilisables et filtres dynamiques.
* Analytique embarquée des rapports et des tableaux de bord via iframe.
* Fonctionnalités de préparation des données et d’ETL.
* Prise en charge de la modélisation SQL des données pour le mappage relationnel.

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com) est une plateforme de données et un outil de business intelligence prenant en charge plus de 50 dialectes de bases de données, dont ClickHouse. Looker est disponible en tant que plateforme SaaS et en version autohébergée. Les utilisateurs peuvent utiliser Looker dans le navigateur pour explorer les données, créer des visualisations et des tableaux de bord, planifier des rapports et partager leurs analyses avec leurs collègues. Looker fournit un large éventail d’outils pour intégrer ces fonctionnalités dans d’autres applications, ainsi qu’une API
pour intégrer les données à d’autres applications.

Fonctionnalités :

* Développement simple et agile avec LookML, un langage qui prend en charge la
  [modélisation des données](https://looker.com/platform/data-modeling) afin d’aider les créateurs de rapports et les utilisateurs finaux.
* Intégration puissante aux workflows via les [Data Actions](https://looker.com/platform/actions) de Looker.

[Comment configurer ClickHouse dans Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com) est un outil de BI en libre-service pour l’exploration de données et le reporting opérationnel. Il est disponible à la fois en tant que service cloud et en version auto-hébergée. Les rapports SeekTable peuvent être intégrés à n’importe quelle application web.

Fonctionnalités :

* Générateur de rapports convivial pour les utilisateurs métier.
* Paramètres de rapport puissants pour le filtrage SQL et les personnalisations de requêtes propres aux rapports.
* Peut se connecter à ClickHouse aussi bien via un endpoint TCP/IP natif que via une interface HTTP(S) (2 drivers différents).
* Il est possible d’exploiter toute la puissance du dialecte SQL de ClickHouse dans les définitions des dimensions/mesures.
* [API Web](https://www.seektable.com/help/web-api-integration) pour la génération automatisée de rapports.
* Prend en charge un workflow de développement de rapports avec [sauvegarde/restauration](https://www.seektable.com/help/self-hosted-backup-restore) des données du compte ; la configuration des modèles de données (cubes) / rapports est en XML lisible par l’utilisateur et peut être stockée dans un système de contrôle de versions.

SeekTable est [gratuit](https://www.seektable.com/help/cloud-pricing) pour un usage personnel/individuel.

[Comment configurer une connexion à ClickHouse dans SeekTable.](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin) est une UI simple qui vous permet de visualiser les requêtes en cours d’exécution sur votre cluster ClickHouse, ainsi que les informations les concernant, et de les interrompre si vous le souhaitez.

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/) — un outil en ligne de requêtage et d’analyse pour l’ETL et la visualisation. Il permet de se connecter à ClickHouse, d’interroger les données via une console SQL polyvalente, ainsi que de charger des données à partir de fichiers statiques et de services tiers. TABLUM.IO peut visualiser les résultats sous forme de graphiques et de tableaux.

Fonctionnalités :

* ETL : chargement de données depuis des bases de données populaires, des fichiers locaux et distants, et des appels d’API.
* Console SQL polyvalente avec coloration syntaxique et générateur visuel de requêtes.
* Visualisation des données sous forme de graphiques et de tableaux.
* Matérialisation des données et sous-requêtes.
* Envoi de rapports vers Slack, Telegram ou par e-mail.
* Création de pipelines de données via une API propriétaire.
* Exportation des données aux formats JSON, CSV, SQL et HTML.
* Interface web.

TABLUM.IO peut être déployé en mode auto-hébergé (sous forme d’image Docker) ou dans le cloud.
Licence : produit [commercial](https://tablum.io/pricing) avec 3 mois d’essai gratuit.

Essayez-le gratuitement [dans le cloud](https://tablum.io/try).
En savoir plus sur le produit sur [TABLUM.IO](https://tablum.io/)

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman) est un outil de gestion et de supervision des clusters ClickHouse !

Fonctionnalités :

* Déploiement automatisé rapide et pratique des clusters via une interface web
* Les clusters peuvent être redimensionnés
* Équilibrage de charge des données du cluster
* Mise à niveau du cluster en ligne
* Modification de la configuration du cluster depuis la page
* Supervision des nœuds du cluster et de ZooKeeper
* Surveillance de l&#39;état des tables et des partitions, ainsi que des instructions SQL lentes
* Fournit une page d&#39;exécution SQL facile à utiliser

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev) est une interface graphique de bureau native pour plusieurs bases de données, avec une excellente prise en charge de ClickHouse — vue d’ensemble du serveur, gestion du schéma, recherche vectorielle et consultation de grands jeux de résultats.

Fonctionnalités :

* Vue d’ensemble du serveur à la connexion — version, temps de fonctionnement, requêtes en cours, merges actifs, taille des parts et du stockage, état des répliques, clusters et nœuds en un coup d’œil.
* Générateur de requêtes visuel (sélecteurs de colonnes, filtres, tri, limite) aux côtés d’un éditeur SQL Monaco avec coloration syntaxique et historique des requêtes par connexion.
* Assistant visuel `CREATE TABLE` prenant en charge les variantes de `MergeTree`, `ORDER BY`, `PARTITION BY`, `SETTINGS` et l’encapsulation automatique dans `Nullable()`.
* Prise en charge native des types ClickHouse — `Nullable`, `Array`, `LowCardinality`, objets imbriqués.
* Prise en charge de la recherche vectorielle — colonnes d’embeddings `Array(Float32)` affichées sous forme de cellules vectorielles compactes, visualisation d’embeddings en 2D et recherche d’éléments similaires via `cosineDistance`.
* Modification directe des données dans les tables de résultats avec enregistrement par lot, ainsi qu’export et import CSV/JSON/SQL à l’aide des formats natifs de ClickHouse.
* Options de connexion : HTTP/HTTPS, tunnel SSH pour les clusters privés derrière un pare-feu, mode lecture seule facultatif pour une consultation sûre en production.
* Fonctionne avec ClickHouse Cloud et en mode auto-hébergé.