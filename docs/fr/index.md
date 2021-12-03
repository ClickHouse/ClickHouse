---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 0
toc_title: "Aper\xE7u"
---

# Qu'Est-Ce Que ClickHouse? {#what-is-clickhouse}

ClickHouse est un système de gestion de base de données orienté colonne (SGBD) pour le traitement analytique en ligne des requêtes (OLAP).

Dans un “normal” SGBD orienté ligne, les données sont stockées dans cet ordre:

| Rangée | WatchID     | JavaEnable | Intitulé                         | GoodEvent | EventTime           |
|--------|-------------|------------|----------------------------------|-----------|---------------------|
| #0    | 89354350662 | 1          | Relations Avec Les Investisseurs | 1         | 2016-05-18 05:19:20 |
| #1    | 90329509958 | 0          | Contacter                        | 1         | 2016-05-18 08:10:20 |
| #2    | 89953706054 | 1          | Mission                          | 1         | 2016-05-18 07:38:00 |
| #N    | …           | …          | …                                | …         | …                   |

En d'autres termes, toutes les valeurs liées à une ligne sont physiquement stockées l'une à côté de l'autre.

Des exemples d'un SGBD orienté ligne sont MySQL, Postgres et MS SQL Server.

Dans un SGBD orienté colonne, les données sont stockées comme ceci:

| Rangée:     | #0                              | #1                 | #2                 | #N |
|-------------|----------------------------------|---------------------|---------------------|-----|
| WatchID:    | 89354350662                      | 90329509958         | 89953706054         | …   |
| JavaEnable: | 1                                | 0                   | 1                   | …   |
| Intitulé:   | Relations Avec Les Investisseurs | Contacter           | Mission             | …   |
| GoodEvent:  | 1                                | 1                   | 1                   | …   |
| EventTime:  | 2016-05-18 05:19:20              | 2016-05-18 08:10:20 | 2016-05-18 07:38:00 | …   |

Ces exemples montrent l'ordre que les données sont organisées en. Les valeurs de différentes colonnes sont stockés séparément, et les données de la même colonne sont stockées ensemble.

Exemples D'un SGBD orienté colonne: Vertica, Paraccel (matrice Actian et Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise et Actian Vector), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid et kdb+.

Different orders for storing data are better suited to different scenarios. The data access scenario refers to what queries are made, how often, and in what proportion; how much data is read for each type of query – rows, columns, and bytes; the relationship between reading and updating data; the working size of the data and how locally it is used; whether transactions are used, and how isolated they are; requirements for data replication and logical integrity; requirements for latency and throughput for each type of query, and so on.

Plus la charge sur le système est élevée, plus il est important de personnaliser le système configuré pour correspondre aux exigences du scénario d'utilisation, et plus cette personnalisation devient fine. Il n'y a pas de système qui soit aussi bien adapté à des scénarios significativement différents. Si un système est adaptable à un large ensemble de scénarios, sous une charge élevée, le système traitera tous les scénarios de manière également médiocre, ou fonctionnera bien pour un ou quelques-uns des scénarios possibles.

## Propriétés clés du scénario OLAP {#key-properties-of-olap-scenario}

-   La grande majorité des demandes concernent l'accès en lecture.
-   Les données sont mises à jour en lots assez importants (\> 1000 lignes), pas par des lignes simples; ou elles ne sont pas mises à jour du tout.
-   Les données sont ajoutées à la base de données mais ne sont pas modifiées.
-   Pour les lectures, un assez grand nombre de lignes sont extraites de la base de données, mais seulement un petit sous-ensemble de colonnes.
-   Les Tables sont “wide,” ce qui signifie qu'ils contiennent un grand nombre de colonnes.
-   Les requêtes sont relativement rares (généralement des centaines de requêtes par serveur ou moins par seconde).
-   Pour les requêtes simples, les latences autour de 50 ms sont autorisées.
-   Les valeurs de colonne sont assez petites: nombres et chaînes courtes (par exemple, 60 octets par URL).
-   Nécessite un débit élevé lors du traitement d'une seule requête (jusqu'à des milliards de lignes par seconde par serveur).
-   Les Transactions ne sont pas nécessaires.
-   Faibles exigences en matière de cohérence des données.
-   Il y a une grande table par requête. Toutes les tables sont petites, sauf une.
-   Un résultat de requête est significativement plus petit que les données source. En d'autres termes, les données sont filtrées ou agrégées, de sorte que le résultat s'intègre dans la RAM d'un seul serveur.

Il est facile de voir que le scénario OLAP est très différent des autres scénarios populaires (tels que OLTP ou key-Value access). Il n'est donc pas logique d'essayer D'utiliser OLTP ou une base de données clé-valeur pour traiter les requêtes analytiques si vous voulez obtenir des performances décentes. Par exemple, si vous essayez D'utiliser MongoDB ou Redis pour l'analyse, vous obtiendrez des performances très médiocres par rapport aux bases de données OLAP.

## Pourquoi les bases de données orientées colonne fonctionnent mieux dans le scénario OLAP {#why-column-oriented-databases-work-better-in-the-olap-scenario}

Les bases de données orientées colonne sont mieux adaptées aux scénarios OLAP: elles sont au moins 100 fois plus rapides dans le traitement de la plupart des requêtes. Les raisons sont expliquées en détail ci-dessous, mais le fait est plus facile de démontrer visuellement:

**SGBD orienté ligne**

![Row-oriented](images/row-oriented.gif#)

**SGBD orienté colonne**

![Column-oriented](images/column-oriented.gif#)

Vous voyez la différence?

### D'entrée/sortie {#inputoutput}

1.  Pour une requête analytique, seul un petit nombre de colonnes de table doit être lu. Dans une base de données orientée colonne, vous pouvez lire uniquement les données dont vous avez besoin. Par exemple, si vous avez besoin de 5 colonnes sur 100, Vous pouvez vous attendre à une réduction de 20 fois des e / s.
2.  Puisque les données sont lues en paquets, il est plus facile de les compresser. Les données dans les colonnes sont également plus faciles à compresser. Cela réduit d'autant le volume d'e/S.
3.  En raison de la réduction des E / S, Plus de données s'insèrent dans le cache du système.

Par exemple, la requête “count the number of records for each advertising platform” nécessite la lecture d'un “advertising platform ID” colonne, qui prend 1 octet non compressé. Si la majeure partie du trafic ne provenait pas de plates-formes publicitaires, vous pouvez vous attendre à une compression d'au moins 10 fois de cette colonne. Lors de l'utilisation d'un algorithme de compression rapide, la décompression des données est possible à une vitesse d'au moins plusieurs gigaoctets de données non compressées par seconde. En d'autres termes, cette requête ne peut être traitée qu'à une vitesse d'environ plusieurs milliards de lignes par seconde sur un seul serveur. Cette vitesse est effectivement atteinte dans la pratique.

### CPU {#cpu}

Étant donné que l'exécution d'une requête nécessite le traitement d'un grand nombre de lignes, il est utile de répartir toutes les opérations pour des vecteurs entiers au lieu de lignes séparées, ou d'implémenter le moteur de requête de sorte qu'il n'y ait presque aucun coût d'expédition. Si vous ne le faites pas, avec un sous-système de disque à moitié décent, l'interpréteur de requête bloque inévitablement le processeur. Il est logique de stocker des données dans des colonnes et de les traiter, si possible, par des colonnes.

Il y a deux façons de le faire:

1.  Un moteur vectoriel. Toutes les opérations sont écrites pour les vecteurs, au lieu de valeurs séparées. Cela signifie que vous n'avez pas besoin d'appeler les opérations très souvent, et les coûts d'expédition sont négligeables. Le code d'opération contient un cycle interne optimisé.

2.  La génération de Code. Le code généré pour la requête contient tous les appels indirects.

Ce n'est pas fait dans “normal” bases de données, car cela n'a pas de sens lors de l'exécution de requêtes simples. Cependant, il y a des exceptions. Par exemple, MemSQL utilise la génération de code pour réduire la latence lors du traitement des requêtes SQL. (À titre de comparaison, les SGBD analytiques nécessitent une optimisation du débit, et non une latence.)

Notez que pour l'efficacité du processeur, le langage de requête doit être déclaratif (SQL ou MDX), ou au moins un vecteur (J, K). La requête ne doit contenir que des boucles implicites, permettant une optimisation.

{## [Article Original](https://clickhouse.tech/docs/en/) ##}
