---
description: 'Puede supervisar el uso de los recursos de hardware, así como las métricas del servidor de ClickHouse.'
keywords: ['monitorización', 'observabilidad', 'dashboard avanzado', 'dashboard', 'dashboard de observabilidad']
sidebar_label: 'Monitorización'
sidebar_position: 45
slug: /operations/monitoring
title: 'Monitorización'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # Monitorización
</div>

:::note
Los datos de monitorización descritos en esta guía están disponibles en ClickHouse Cloud. Además de mostrarse en el dashboard integrado que se describe a continuación, las métricas de rendimiento, tanto básicas como avanzadas, también pueden consultarse directamente en la consola principal del servicio.
:::

Puede supervisar:

* El uso de los recursos de hardware.
* Las métricas del servidor de ClickHouse.

<div id="built-in-advanced-observability-dashboard">
  ## Panel avanzado de observabilidad integrado
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="Captura de pantalla 2023-11-12 a las 6 08 58 PM" size="md" />

ClickHouse incluye un panel avanzado de observabilidad integrado al que se puede acceder mediante `$HOST:$PORT/dashboard` (requiere usuario y contraseña) y que muestra las siguientes métricas:

* Consultas por segundo
* Uso de CPU (núcleos)
* Consultas en ejecución
* Fusiones en ejecución
* Bytes seleccionados por segundo
* Espera de E/S
* Espera de CPU
* Uso de CPU del SO (espacio de usuario)
* Uso de CPU del SO (kernel)
* Lectura desde disco
* Lectura desde el sistema de archivos
* Memoria (rastreada)
* Filas insertadas por segundo
* Total de partes de MergeTree
* Máximo de partes por partición

<div id="resource-utilization">
  ## Uso de recursos
</div>

ClickHouse también supervisa por sí mismo el estado de los recursos de hardware, como:

* La carga y la temperatura de los procesadores.
* El uso del sistema de almacenamiento, la RAM y la red.

Estos datos se recopilan en la tabla `system.asynchronous_metric_log`.

<div id="clickhouse-server-metrics">
  ## Métricas del servidor de ClickHouse
</div>

El servidor de ClickHouse incorpora mecanismos integrados para supervisar su propio estado.

Para hacer seguimiento de los eventos del servidor, use los logs del servidor. Consulte la sección [logger](../operations/server-configuration-parameters/settings.md#logger) del archivo de configuración.

ClickHouse recopila:

* Distintas métricas sobre cómo el servidor utiliza los recursos de cómputo.
* Estadísticas generales sobre el procesamiento de consultas.

Puede encontrar métricas en las tablas [system.metrics](/es/operations/system-tables/metrics), [system.events](/es/operations/system-tables/events) y [system.asynchronous&#95;metrics](/es/operations/system-tables/asynchronous_metrics).

Puede configurar ClickHouse para exportar métricas a [Graphite](https://github.com/graphite-project). Consulte la [sección Graphite](../operations/server-configuration-parameters/settings.md#graphite) del archivo de configuración del servidor de ClickHouse. Antes de configurar la exportación de métricas, debe configurar Graphite siguiendo su [guía](https://graphite.readthedocs.io/en/latest/install.html) oficial.

Puede configurar ClickHouse para exportar métricas a [Prometheus](https://prometheus.io). Consulte la [sección Prometheus](../operations/server-configuration-parameters/settings.md#prometheus) del archivo de configuración del servidor de ClickHouse. Antes de configurar la exportación de métricas, debe configurar Prometheus siguiendo su [guía](https://prometheus.io/docs/prometheus/latest/installation/) oficial.

Además, puede supervisar la disponibilidad del servidor mediante la API HTTP. Envíe una solicitud `HTTP GET` a `/ping`. Si el servidor está disponible, responde con `200 OK`.

Para supervisar servidores en una configuración de cluster, debe establecer el parámetro [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) y usar el recurso HTTP `/replicas_status`. Una solicitud a `/replicas_status` devuelve `200 OK` si la réplica está disponible y no presenta retraso con respecto a las demás réplicas. Si una réplica está retrasada, devuelve `503 HTTP_SERVICE_UNAVAILABLE` con información sobre el desfase.