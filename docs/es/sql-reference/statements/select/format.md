---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# FORMAT Cláusula {#format-clause}

ClickHouse soporta una amplia gama de [formatos de serialización](../../../interfaces/formats.md) que se pueden usar en los resultados de consultas, entre otras cosas. Hay varias formas de elegir un formato para `SELECT` salida, uno de ellos es especificar `FORMAT format` al final de la consulta para obtener los datos resultantes en cualquier formato específico.

El formato específico se puede utilizar ya sea por conveniencia, integración con otros sistemas o ganancia de rendimiento.

## Formato predeterminado {#default-format}

Si el `FORMAT` se omite la cláusula, se utiliza el formato predeterminado, que depende tanto de la configuración como de la interfaz utilizada para acceder al servidor ClickHouse. Para el [Interfaz HTTP](../../../interfaces/http.md) y el [cliente de línea de comandos](../../../interfaces/cli.md) En el modo batch, el formato predeterminado es `TabSeparated`. Para el cliente de línea de comandos en modo interactivo, el formato predeterminado es `PrettyCompact` (produce tablas compactas legibles por humanos).

## Detalles de implementación {#implementation-details}

Cuando se utiliza el cliente de línea de comandos, los datos siempre se pasan a través de la red en un formato interno eficiente (`Native`). El cliente interpreta independientemente el `FORMAT` cláusula de la consulta y formatea los datos en sí (aliviando así la red y el servidor de la carga adicional).
