---
description: 'Page de description du moteur de base de données `Shared`, disponible dans ClickHouse Cloud'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # Moteur de base de données Shared
</div>

Le moteur de base de données `Shared` fonctionne avec Shared Catalog pour gérer les bases de données dont les tables utilisent des moteurs de table sans état, tels que [`SharedMergeTree`](/fr/cloud/reference/shared-merge-tree).
Ces moteurs de table n’écrivent pas d’état persistant sur disque et sont compatibles avec les environnements de calcul dynamiques.

Dans Cloud, le moteur de base de données `Shared` supprime la dépendance aux disques locaux.
Il s’agit d’un moteur entièrement en mémoire, qui ne nécessite que du CPU et de la mémoire.

<div id="how-it-works">
  ## Comment cela fonctionne-t-il ?
</div>

Le moteur de base de données `Shared` stocke toutes les définitions de bases de données et de tables dans un Shared Catalog central s’appuyant sur Keeper. Au lieu d’écrire sur le disque local, il maintient un état global versionné unique partagé entre tous les nœuds de calcul.

Chaque nœud ne conserve que la dernière version appliquée et, au démarrage, récupère l’état le plus récent sans avoir besoin de fichiers locaux ni de configuration manuelle.

<div id="syntax">
  ## Syntaxe
</div>

Pour les utilisateurs finaux, l’utilisation de Shared Catalog et du moteur de base de données Shared ne nécessite aucune configuration supplémentaire. La création d’une base de données se fait comme d’habitude :

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud attribue automatiquement le moteur de base de données Shared aux bases de données. Toutes les tables créées dans une telle base de données à l’aide de moteurs sans état bénéficient automatiquement des capacités de réplication et de coordination de Shared Catalog.

:::tip
Pour en savoir plus sur Shared Catalog et ses avantages, consultez [&quot;Shared catalog and shared database engine&quot;](/fr/cloud/reference/shared-catalog) dans la section Reference de Cloud.
:::