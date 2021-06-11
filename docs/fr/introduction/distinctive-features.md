---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 4
toc_title: "particularit\xE9"
---

# Caractéristiques distinctives de ClickHouse {#distinctive-features-of-clickhouse}

## Vrai SGBD orienté colonne {#true-column-oriented-dbms}

Dans un vrai SGBD orienté colonne, aucune donnée supplémentaire n'est stockée avec les valeurs. Entre autres choses, cela signifie que les valeurs de longueur constante doivent être prises en charge, pour éviter de stocker leur longueur “number” à côté de ces valeurs. Par exemple, un milliard de valeurs de type UInt8 devrait consommer environ 1 Go non compressé, ou cela affecte fortement l'utilisation du processeur. Il est essentiel de stocker des données de manière compacte (sans “garbage”) même lorsqu'il n'est pas compressé, puisque la vitesse de décompression (utilisation du processeur) dépend principalement du volume de données non compressées.

Il est à noter car il existe des systèmes qui peuvent stocker des valeurs de différentes colonnes séparément, mais qui ne peuvent pas traiter efficacement les requêtes analytiques en raison de leur optimisation pour d'autres scénarios. Les exemples sont HBase, BigTable, Cassandra et HyperTable. Dans ces systèmes, vous obtiendriez un débit d'environ cent mille lignes par seconde, mais pas des centaines de millions de lignes par seconde.

Il est également intéressant de noter que ClickHouse est un système de gestion de base de données, pas une seule base de données. ClickHouse permet de créer des tables et des bases de données en cours d'exécution, de charger des données et d'exécuter des requêtes sans reconfigurer et redémarrer le serveur.

## Compression De Données {#data-compression}

Certains SGBD orientés colonne (InfiniDB CE et MonetDB) n'utilisent pas la compression de données. Cependant, la compression des données joue un rôle clé dans la réalisation d'excellentes performances.

## Stockage de données sur disque {#disk-storage-of-data}

Garder les données physiquement triées par clé primaire permet d'extraire des données pour ses valeurs spécifiques ou plages de valeurs avec une faible latence, moins de quelques dizaines de millisecondes. Certains SGBD orientés colonne (tels que SAP HANA et Google PowerDrill) ne peuvent fonctionner qu'en RAM. Cette approche encourage l'allocation d'un budget matériel plus important que ce qui est nécessaire pour l'analyse en temps réel. ClickHouse est conçu pour fonctionner sur des disques durs réguliers, ce qui signifie que le coût par Go de stockage de données est faible, mais SSD et RAM supplémentaire sont également entièrement utilisés si disponible.

## Traitement parallèle sur plusieurs cœurs {#parallel-processing-on-multiple-cores}

Les grandes requêtes sont parallélisées naturellement, en prenant toutes les ressources nécessaires disponibles sur le serveur actuel.

## Traitement distribué sur plusieurs serveurs {#distributed-processing-on-multiple-servers}

Presque aucun des SGBD en colonnes mentionnés ci-dessus ne prend en charge le traitement des requêtes distribuées.
Dans ClickHouse, les données peuvent résider sur différents fragments. Chaque fragment peut être un groupe de répliques utilisées pour la tolérance aux pannes. Tous les fragments sont utilisés pour exécuter une requête en parallèle, de façon transparente pour l'utilisateur.

## Prise en charge SQL {#sql-support}

ClickHouse prend en charge un langage de requête déclarative basé sur SQL qui est identique à la norme SQL dans de nombreux cas.
Les requêtes prises en charge incluent les clauses GROUP BY, ORDER BY, les sous-requêtes in FROM, IN et JOIN, ainsi que les sous-requêtes scalaires.
Les sous-requêtes dépendantes et les fonctions de fenêtre ne sont pas prises en charge.

## Moteur Vectoriel {#vector-engine}

Les données ne sont pas seulement stockées par des colonnes, mais sont traitées par des vecteurs (parties de colonnes), ce qui permet d'atteindre une efficacité élevée du processeur.

## Données en temps réel des Mises à jour {#real-time-data-updates}

ClickHouse prend en charge les tables avec une clé primaire. Pour effectuer rapidement des requêtes sur la plage de la clé primaire, les données sont triées progressivement à l'aide de l'arborescence de fusion. Pour cette raison, les données peuvent être continuellement ajoutées à la table. Pas de verrouillage lorsque de nouvelles données sont ingérés.

## Index {#index}

Avoir une donnée physiquement triée par clé primaire permet d'extraire des données pour ses valeurs spécifiques ou plages de valeurs avec une faible latence, moins de quelques dizaines de millisecondes.

## Convient pour les requêtes en ligne {#suitable-for-online-queries}

Faible latence signifie que les requêtes peuvent être traitées sans délai et sans essayer de préparer une réponse à l'avance, au même moment pendant le chargement de la page de l'interface utilisateur. En d'autres termes, en ligne.

## Prise en charge des calculs approximatifs {#support-for-approximated-calculations}

ClickHouse offre différentes façons d'échanger la précision pour la performance:

1.  Fonctions d'agrégation pour le calcul approximatif du nombre de valeurs distinctes, de médianes et de quantiles.
2.  L'exécution d'une requête basée sur une partie (échantillon) de données et obtenir un pseudo résultat. Dans ce cas, proportionnellement, moins de données sont récupérées à partir du disque.
3.  L'exécution d'une agrégation pour un nombre limité de clés aléatoires, au lieu de toutes les clés. Sous certaines conditions pour la distribution des clés dans les données, cela fournit un résultat raisonnablement précis tout en utilisant moins de ressources.

## Prise en charge de la réplication et de l'intégrité des données {#data-replication-and-data-integrity-support}

ClickHouse utilise la réplication multi-maître asynchrone. Après avoir été écrit dans n'importe quelle réplique disponible, toutes les répliques restantes récupèrent leur copie en arrière-plan. Le système conserve des données identiques sur différentes répliques. La récupération après la plupart des échecs est effectuée automatiquement ou semi-automatiquement dans les cas complexes.

Pour plus d'informations, consultez la section [Réplication des données](../engines/table-engines/mergetree-family/replication.md).

## Caractéristiques qui peuvent être considérées comme des inconvénients {#clickhouse-features-that-can-be-considered-disadvantages}

1.  Pas de transactions à part entière.
2.  Manque de capacité à modifier ou supprimer des données déjà insérées avec un taux élevé et une faible latence. Des suppressions et des mises à jour par lots sont disponibles pour nettoyer ou modifier les données, par exemple pour [GDPR](https://gdpr-info.eu).
3.  L'index clairsemé rend ClickHouse pas si approprié pour les requêtes ponctuelles récupérant des lignes simples par leurs clés.

[Article Original](https://clickhouse.tech/docs/en/introduction/distinctive_features/) <!--hide-->
