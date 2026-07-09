---
description: 'Documentation sur la famille de moteurs Log'
sidebar_label: 'Famille Log'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Famille de moteurs Log'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Famille de moteurs de table Log
</div>

<CloudNotSupportedBadge />

Ces moteurs ont été développés pour les cas où vous devez écrire rapidement de nombreuses petites tables (jusqu’à environ 1 million de lignes) et les relire ensuite en bloc.

Moteurs de la famille :

| Moteurs Log                                                 |
| ----------------------------------------------------------- |
| [StripeLog](/fr/engines/table-engines/log-family/stripelog.md) |
| [Log](/fr/engines/table-engines/log-family/log.md)             |
| [TinyLog](/fr/engines/table-engines/log-family/tinylog.md)     |

Les moteurs de table de la famille `Log` peuvent stocker des données sur des systèmes de fichiers distribués [HDFS](/fr/engines/table-engines/integrations/hdfs) ou [S3](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).

:::warning Ce moteur n’est pas destiné aux données de logs.
Malgré son nom, *les moteurs de table Log ne sont pas conçus pour stocker des données de logs. Ils ne doivent être utilisés que pour de petits volumes à écrire rapidement.
:::

<div id="common-properties">
  ## Propriétés communes
</div>

Moteurs :

* Stockent les données sur un disque.

* Ajoutent les données à la fin du fichier lors de l&#39;écriture.

* Prennent en charge les verrous pour l&#39;accès concurrent aux données.

  Pendant les requêtes `INSERT`, la table est verrouillée, et toutes les autres requêtes de lecture et d&#39;écriture doivent attendre que la table soit déverrouillée. S&#39;il n&#39;y a pas de requêtes d&#39;écriture, un nombre quelconque de requêtes de lecture peut être exécuté de manière concurrente.

* Ne prennent pas en charge les [mutations](/fr/sql-reference/statements/alter#mutations).

* Ne prennent pas en charge les index.

  Cela signifie que les requêtes `SELECT` sur des plages de données ne sont pas efficaces.

* N&#39;écrivent pas les données de manière atomique.

  Vous pouvez vous retrouver avec une table contenant des données corrompues si quelque chose interrompt l&#39;opération d&#39;écriture, par exemple un arrêt anormal du serveur.

<div id="differences">
  ## Différences
</div>

Le moteur `TinyLog` est le plus simple de la famille et offre les fonctionnalités les plus limitées ainsi que les performances les plus faibles. Le moteur `TinyLog` ne prend pas en charge la lecture parallèle des données par plusieurs threads au sein d’une même requête. Il lit les données plus lentement que les autres moteurs de la famille qui prennent en charge la lecture parallèle dans le cadre d’une même requête, et il utilise presque autant de descripteurs de fichiers que le moteur `Log`, car il stocke chaque colonne dans un fichier distinct. Utilisez-le uniquement dans des scénarios simples.

Les moteurs `Log` et `StripeLog` prennent en charge la lecture parallèle des données. Lors de la lecture des données, ClickHouse utilise plusieurs threads. Chaque thread traite un bloc de données distinct. Le moteur `Log` utilise un fichier distinct pour chaque colonne de la table. `StripeLog` stocke toutes les données dans un seul fichier. Par conséquent, le moteur `StripeLog` utilise moins de descripteurs de fichiers, mais le moteur `Log` offre de meilleures performances lors de la lecture des données.