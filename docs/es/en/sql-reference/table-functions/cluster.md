---
description: 'Permite acceder a todos los segmentos (configurados en la sección `remote_servers`)
  de un clúster sin crear una tabla [Distributed](../../engines/table-engines/special/distributed.md).'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'reference'
---

Permite acceder a todos los segmentos (configurados en la sección `remote_servers`) de un clúster sin crear una tabla [Distributed](../../engines/table-engines/special/distributed.md). Solo se consulta una réplica de cada segmento.

La función `clusterAllReplicas` es igual que `cluster`, pero consulta todas las réplicas. Cada réplica de un clúster se usa como un segmento/conexión independiente.

:::note
Todos los clústeres disponibles se enumeran en la tabla [system.clusters](../../operations/system-tables/clusters.md).
:::

<div id="syntax">
  ## Sintaxis
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumentos                 | Tipo                                                                                                                                                                                   |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`             | Nombre de un clúster que se utiliza para construir un conjunto de direcciones y parámetros de conexión para servidores remotos y locales; si no se especifica, se establece `default`. |
| `db.table` o `db`, `table` | Nombre de una base de datos y una tabla.                                                                                                                                               |
| `sharding_key`             | Una clave de segmentación. Opcional. Debe especificarse si el clúster tiene más de un segmento.                                                                                        |

<div id="returned_value">
  ## Valor devuelto
</div>

El conjunto de datos de los clústeres.

<div id="using_macros">
  ## Uso de macros
</div>

`cluster_name` puede contener macros: sustituciones entre `{}`. El valor sustituido se toma de la sección [macros](../../operations/server-configuration-parameters/settings.md#macros) del archivo de configuración del servidor.

Ejemplo:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## Uso y recomendaciones
</div>

Usar las funciones de tabla `cluster` y `clusterAllReplicas` es menos eficiente que crear una tabla `Distributed` porque, en este caso, la conexión con el servidor se restablece para cada petición. Cuando se procesa un gran número de consultas, cree siempre la tabla `Distributed` de antemano y no use las funciones de tabla `cluster` y `clusterAllReplicas`.

Las funciones de tabla `cluster` y `clusterAllReplicas` pueden ser útiles en los siguientes casos:

* Acceder a un clúster específico para comparar datos, depurar y hacer pruebas.
* Consultas a varios clústeres y réplicas de ClickHouse con fines de investigación.
* Peticiones distribuidas poco frecuentes realizadas manualmente.

Los ajustes de conexión, como `host`, `port`, `user`, `password`, `compression` y `secure`, se toman de la sección de configuración `<remote_servers>`. Consulte los detalles en [motor Distributed](../../engines/table-engines/special/distributed.md).

<div id="related">
  ## Véase también
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)