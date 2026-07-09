---
description: 'Página que describe el motor de base de datos `Shared`, disponible en ClickHouse Cloud'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # Motor de base de datos Shared
</div>

El motor de base de datos `Shared` funciona junto con Shared Catalog para gestionar bases de datos cuyas tablas usan motores de tabla sin estado, como [`SharedMergeTree`](/es/cloud/reference/shared-merge-tree).
Estos motores de tabla no escriben estado persistente en disco y son compatibles con entornos dinámicos de compute.

El motor de base de datos `Shared` en Cloud elimina la dependencia de discos locales.
Es un motor completamente en memoria que solo requiere CPU y memoria.

<div id="how-it-works">
  ## ¿Cómo funciona?
</div>

El motor de base de datos `Shared` almacena todas las definiciones de bases de datos y tablas en un Shared Catalog central respaldado por Keeper. En lugar de escribir en el disco local, mantiene un único estado global versionado que comparten todos los nodos de cómputo.

Cada nodo solo mantiene el registro de la última versión aplicada y, al iniciarse, obtiene el estado más reciente sin necesidad de archivos locales ni de configuración manual.

<div id="syntax">
  ## Sintaxis
</div>

Para los usuarios finales, usar Shared Catalog y el motor de base de datos Shared no requiere ninguna configuración adicional. La creación de la base de datos se realiza como siempre:

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud asigna automáticamente el motor de base de datos Shared a las bases de datos. Cualquier tabla creada dentro de una base de datos de este tipo con motores sin estado se beneficiará automáticamente de las capacidades de replicación y coordinación de Shared Catalog.

:::tip
Para obtener más información sobre Shared Catalog y sus ventajas, consulta [&quot;Shared catalog and shared database engine&quot;](/es/cloud/reference/shared-catalog) en la sección Reference de Cloud.
:::