---
description: 'Un proceso hijo de sacrificio que atrae al mecanismo OOM killer de Linux antes
  que el servidor de ClickHouse, dándole al servidor la oportunidad de reducir la carga y sobrevivir.'
sidebar_label: 'canario OOM'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'canario OOM'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
El canario OOM es experimental y está desactivado de forma predeterminada. Su comportamiento puede cambiar
entre versiones de ClickHouse hasta que se complete su validación para producción.
:::

<div id="overview">
  ## Descripción general
</div>

Cuando un host o un cgroup de memoria se queda sin memoria, el OOM killer de
Linux finaliza un proceso con `SIGKILL`, por lo general el que más memoria
consume, que en un host dedicado suele ser el propio `clickhouse-server`. Se
pierde el servidor completo en lugar de darle la oportunidad de recuperarse.

El canario OOM cambia quién muere primero. Ejecuta un pequeño proceso hijo *de
sacrificio* que se convierte en el objetivo OOM más atractivo, para que el
kernel lo mate a él en lugar del servidor. Entonces el servidor detecta la
terminación, confirma que fue un evento OOM y reduce la presión de memoria para
poder sobrevivir.

El canario no aumenta ningún límite de memoria y no sustituye a una
configuración correcta de los límites (consulta [Memory overcommit](/es/operations/settings/memory-overcommit) y
`max_server_memory_usage`). Es una última línea de defensa que intercambia una
cantidad pequeña y fija de memoria por la posibilidad de sobrevivir a un pico
de uso de memoria.

<div id="how-it-works">
  ## Cómo funciona
</div>

El canario es un proceso `clickhouse oom-canary` independiente. Establece su propio
`oom_score_adj` al máximo (`1000`) para que el kernel lo elija como primer objetivo; luego
asigna, toca y aplica `mlock` a `oom_canary_size` bytes (100 MB de forma predeterminada) para que
su conjunto residente esté realmente en memoria. Se elimina automáticamente si el servidor se detiene.

En el servidor, un hilo de monitorización observa el canario (mediante `pidfd`) y reacciona cuando
muere:

* Si `SIGKILL` lo mata **con** evidencia de OOM → ejecuta la respuesta ante OOM y luego
  relanza un canario nuevo.
* Si muere **sin** evidencia de OOM (por ejemplo, por un `kill -9` manual), o termina
  con un fallo transitorio → solo se relanza, sin respuesta.
* Si hay un fallo permanente de configuración, o el servidor se apaga → el canario se desactiva.

La evidencia de OOM proviene únicamente del contador `oom_kill` de `memory.events.local` en cgroup v2.
Es deliberadamente local al cgroup: los contadores jerárquicos o de todo el host pueden
verse incrementados por procesos no relacionados y desencadenarían respuestas falsas.

Ante un OOM confirmado, la respuesta ejecuta estos pasos independientes: registrar un mensaje `FATAL`,
purgar las arenas del allocator (jemalloc), intentar cancelar todas las
queries en ejecución, cancelar todos los merges y mutations, y poner en cola un evento en
[`system.crash_log`](/es/operations/system-tables/crash_log). Los system logs no se
vacían de forma síncrona, porque forzar IO bajo presión de memoria puede empeorar las cosas.

<div id="requirements">
  ## Requisitos
</div>

* **Linux ≥ 5.3.** El monitor mantiene el control del canario mediante `pidfd_open`; en kernels más antiguos,
  el canario se desactiva al arrancar. Es un no-op en plataformas que no son Linux.
* **cgroup v2 con `memory.events.local`** para la respuesta ante OOM. Sin esto, el
  canario sigue relanzándose después de un `SIGKILL`, pero no puede confirmar un OOM, por lo que la
  respuesta nunca se ejecuta (se registra una advertencia al arrancar).
* **capacidad `mlock` (opcional).** Bloquear la memoria del canario requiere
  `CAP_IPC_LOCK` o un `RLIMIT_MEMLOCK` suficiente; si falla, el canario registra una
  advertencia y su memoria puede pasar a swap, lo que debilita su función como objetivo de OOM.

:::warning memory.oom.group
Si `memory.oom.group` de cgroup v2 está habilitado para el cgroup del servidor, el kernel
mata todo el cgroup como una sola unidad ante un OOM: el servidor muere junto con el
canario y la respuesta nunca se ejecuta. El canario no puede proteger al servidor en este
modo; se registra una advertencia al arrancar.
:::

<div id="configuration">
  ## Configuración
</div>

El canario OOM se controla mediante las [opciones de configuración a nivel de servidor](/es/operations/server-configuration-parameters/settings),
definidas como elementos de nivel superior de la configuración del servidor y aplicadas tras reiniciar.

| Setting                              | Default              | Description                                                                                                                                                                                                                                     |
| ------------------------------------ | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | Habilita el canario OOM.                                                                                                                                                                                                                        |
| `oom_canary_size`                    | `104857600` (100 MB) | Bytes que el canario asigna y utiliza. Los valores mayores hacen que sea un objetivo OOM más probable.                                                                                                                                          |
| `oom_canary_relaunch`                | `true`               | Reinicia el canario después de que termine (salvo que se trate de un fallo permanente de configuración o de un apagado), sujeto a los límites indicados a continuación.                                                                         |
| `oom_canary_max_rapid_relaunches`    | `10`                 | Número máximo de reinicios *rápidos* consecutivos antes de desactivar el reinicio automático, para evitar ciclos continuos de reinicio. Se restablece cuando un canario permanece en ejecución más tiempo que `oom_canary_max_backoff_seconds`. |
| `oom_canary_initial_backoff_seconds` | `1`                  | Retraso inicial entre reinicios; se duplica cada vez hasta alcanzar el máximo.                                                                                                                                                                  |
| `oom_canary_max_backoff_seconds`     | `60`                 | Retraso máximo entre reinicios.                                                                                                                                                                                                                 |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## Observabilidad
</div>

Un OOM confirmado genera una fila en
[`system.crash_log`](/es/operations/system-tables/crash_log) con `signal = 9` y una
`signal_description` que menciona `canario OOM`:

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

El ciclo de vida del canario y cada paso de la respuesta ante OOM también se registran en el log del servidor.