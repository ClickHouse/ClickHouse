---
description: 'Al ejecutar consultas, ClickHouse utiliza diferentes cachés.'
sidebar_label: 'Cachés'
sidebar_position: 65
slug: /operations/caches
title: 'Tipos de caché'
keywords: ['cache']
doc_type: 'reference'
---

Al ejecutar consultas, ClickHouse utiliza diferentes cachés para acelerarlas
y reducir la necesidad de leer del disco o escribir en él.

Los principales tipos de caché son:

* `mark_cache` — Caché de [marks](/es/development/architecture#merge-tree) utilizada por los motores de tabla de la familia [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* `uncompressed_cache` — Caché de datos sin comprimir utilizada por los motores de tabla de la familia [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* Caché de páginas del sistema operativo (utilizada indirectamente para archivos con datos reales).

También hay varios tipos de caché adicionales:

* Caché de DNS.
* Caché de [Regexp](/es/interfaces/formats/Regexp).
* Caché de expresiones compiladas.
* Caché de [índice de similitud vectorial](../engines/table-engines/mergetree-family/annindexes.md).
* Caché de [índice de texto](../engines/table-engines/mergetree-family/textindexes.md#caching).
* Caché de esquemas de [formato Avro](/es/interfaces/formats/Avro).
* Caché de datos de [Dictionaries](../sql-reference/statements/create/dictionary/overview.md).
* Caché de inferencia de esquemas.
* [Caché del sistema de archivos](storing-data.md) sobre S3, Azure, Local y otros discos.
* [Caché de páginas en espacio de usuario](/es/operations/userspace-page-cache)
* [Caché de consultas](query-cache.md).
* [Caché de condiciones de consulta](query-condition-cache.md).
* Caché de esquemas de formato.

Si desea borrar una de las cachés, ya sea por ajuste del rendimiento, solución de problemas o consistencia de los datos,
puede usar la sentencia [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md).