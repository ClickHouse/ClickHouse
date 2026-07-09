---
description: 'Límites de conexión TCP.'
sidebar_label: 'Límites de conexión TCP'
slug: /operations/settings/tcp-connection-limits
title: 'Límites de conexión TCP'
doc_type: 'reference'
---

<div id="overview">
  ## Descripción general
</div>

Una conexión TCP de ClickHouse (es decir, una a través del [cliente de línea de comandos](https://clickhouse.com/docs/interfaces/client))
puede desconectarse automáticamente después de cierto número de consultas o de cierto tiempo.
Tras desconectarse, no se vuelve a conectar automáticamente (a menos que esto se active de otro modo,
por ejemplo, al enviar otra consulta desde el cliente de línea de comandos).

Los límites de conexión se habilitan estableciendo los ajustes del servidor
`tcp_close_connection_after_queries_num` (para el límite de consultas)
o `tcp_close_connection_after_queries_seconds` (para el límite de duración) en un valor mayor que 0.
Si ambos límites están habilitados, la conexión se cierra en cuanto se alcanza primero cualquiera de los dos.

Cuando se alcanza un límite y la conexión se cierra, el Client recibe una
excepción `TCP_CONNECTION_LIMIT_REACHED`, y **la consulta que provoca la desconexión nunca se procesa**.

<div id="query-limits">
  ## Límites de consulta
</div>

Suponiendo que `tcp_close_connection_after_queries_num` esté configurado en N, la conexión permite
N consultas exitosas. Luego, en la consulta N + 1, el cliente se desconecta.

Cada consulta procesada cuenta para el límite de consultas. Por lo tanto, al conectar un Client de línea de comandos,
puede haber una consulta inicial automática de advertencias del sistema que cuenta para el límite.

Cuando una conexión TCP está inactiva (es decir, no ha procesado consultas durante cierto período de tiempo,
especificado por la configuración de la sesión `poll_interval`), el número de consultas contabilizadas hasta ese momento se restablece a 0.
Esto significa que el número total de consultas en una sola conexión puede superar
`tcp_close_connection_after_queries_num` si se produce inactividad.

<div id="duration-limits">
  ## Límites de duración
</div>

La duración de la conexión se mide a partir del momento en que el Client se conecta.
El Client se desconecta en la primera consulta una vez transcurridos `tcp_close_connection_after_queries_seconds` segundos.