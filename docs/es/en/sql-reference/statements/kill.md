---
description: 'Documentación de KILL'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'Sentencias KILL'
doc_type: 'reference'
---

Existen dos tipos de sentencias KILL: una para finalizar una consulta y otra para finalizar una mutación

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Intenta terminar forzosamente las consultas que se están ejecutando en ese momento.
Las consultas que se van a terminar se seleccionan de la tabla system.processes según los criterios definidos en la cláusula `WHERE` de la consulta `KILL`.

Ejemplos:

Primero, tendrás que obtener la lista de consultas incompletas. Esta consulta SQL las muestra ordenadas por las que llevan más tiempo ejecutándose:

Lista de un único nodo de ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Listado de un clúster de ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Detenga la consulta:

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
Si está terminando una consulta en ClickHouse Cloud o en un clúster autogestionado, asegúrese de usar la opción `ON CLUSTER [cluster-name]` para garantizar que la consulta se termine en todas las réplicas.
:::

Los usuarios de solo lectura solo pueden detener sus propias consultas.

De forma predeterminada, se usa la versión asíncrona de las consultas (`ASYNC`), que no espera la confirmación de que las consultas se hayan detenido.

La versión síncrona (`SYNC`) espera a que todas las consultas se detengan y muestra información sobre cada proceso a medida que se va deteniendo.
La respuesta contiene la columna `kill_status`, que puede tomar los siguientes valores:

1. `finished` – La consulta se terminó correctamente.
2. `waiting` – En espera de que la consulta finalice después de enviarle una señal de terminación.
3. Los demás valores explican por qué no se puede detener la consulta.

Una consulta de prueba (`TEST`) solo comprueba los permisos del usuario y muestra una lista de consultas que se van a detener.

<div id="kill-mutation">
  ## KILL MUTATION
</div>

La presencia de mutaciones de larga duración o mutaciones incompletas suele indicar que un servicio de ClickHouse no está funcionando correctamente. La naturaleza asíncrona de las mutaciones puede hacer que consuman todos los recursos disponibles del sistema. Es posible que tenga que hacer una de estas dos cosas:

* Pausar todas las mutaciones nuevas, los `INSERT` y los `SELECT`, y dejar que la cola de mutaciones termine de procesarse.
* O cancelar manualmente algunas de estas mutaciones enviando un comando `KILL`.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Intenta cancelar y eliminar las [mutaciones](/es/sql-reference/statements/alter#mutations) que se están ejecutando en ese momento. Las mutaciones que se van a cancelar se seleccionan de la tabla [`system.mutations`](/es/operations/system-tables/mutations) mediante el filtro especificado en la cláusula `WHERE` de la consulta `KILL`.

Una consulta de prueba (`TEST`) solo comprueba los permisos del usuario y muestra una lista de mutaciones que se van a detener.

Ejemplos:

Obtener un `count()` del número de mutaciones incompletas:

Recuento de mutaciones de un solo nodo de ClickHouse:

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Número de mutaciones en un clúster de réplicas de ClickHouse:

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Consulta la lista de mutaciones incompletas:

Lista de mutaciones de un único nodo de ClickHouse:

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

Lista de mutaciones de un clúster de ClickHouse:

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Cancele las mutaciones según sea necesario:

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

La consulta es útil cuando una mutación se queda atascada y no puede finalizar (p. ej., si alguna función de la consulta de mutación lanza una excepción al aplicarse a los datos contenidos en la tabla).

Los cambios ya realizados por la mutación no se revierten.

:::note
La columna `is_killed=1` (solo en ClickHouse Cloud) de la tabla [system.mutations](/es/operations/system-tables/mutations) no significa necesariamente que la mutación haya finalizado por completo. Es posible que una mutación permanezca durante un período prolongado en un estado en el que `is_killed=1` e `is_done=0`. Esto puede ocurrir si otra mutación de larga duración está bloqueando la mutación detenida. Esta es una situación normal.
:::