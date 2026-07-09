---
description: 'Lors de l’exécution de requêtes, ClickHouse utilise différents caches.'
sidebar_label: 'Caches'
sidebar_position: 65
slug: /operations/caches
title: 'Types de caches'
keywords: ['cache']
doc_type: 'référence'
---

Lors de l’exécution de requêtes, ClickHouse utilise différents caches pour accélérer les requêtes
et réduire les lectures et écritures sur disque.

Les principaux types de caches sont :

* `mark_cache` — Cache des [marks](/fr/development/architecture#merge-tree) utilisé par les moteurs de table de la famille [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* `uncompressed_cache` — Cache des données non compressées utilisé par les moteurs de table de la famille [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* Cache de pages du système d’exploitation (utilisé indirectement pour les fichiers contenant les données réelles).

Il existe également de nombreux autres types de caches :

* Cache DNS.
* Cache [Regexp](/fr/interfaces/formats/Regexp).
* Cache des expressions compilées.
* Cache de [Vector similarity index](../engines/table-engines/mergetree-family/annindexes.md).
* Cache de [Text index](../engines/table-engines/mergetree-family/textindexes.md#caching).
* Cache des schémas du [Avro format](/fr/interfaces/formats/Avro).
* Cache de données de [Dictionaries](../sql-reference/statements/create/dictionary/overview.md).
* Cache d’inférence de schéma.
* [Filesystem cache](storing-data.md) sur S3, Azure, Local et d’autres disques.
* [Userspace page cache](/fr/operations/userspace-page-cache)
* [Query cache](query-cache.md).
* [Query condition cache](query-condition-cache.md).
* Cache des schémas de format.

Si vous souhaitez vider l’un des caches, à des fins d’optimisation des performances, de dépannage ou de cohérence des données,
vous pouvez utiliser l’instruction [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md).