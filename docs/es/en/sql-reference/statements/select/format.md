---
description: 'Documentación de la cláusula FORMAT'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'Cláusula FORMAT'
doc_type: 'reference'
---

ClickHouse admite una amplia variedad de [formatos de serialización](../../../interfaces/formats.md) que pueden usarse, entre otras cosas, con los resultados de las consultas. Hay varias formas de elegir un formato para la salida de `SELECT`; una de ellas es especificar `FORMAT format` al final de la consulta para obtener los datos resultantes en un formato concreto.

Puede usarse un formato concreto por comodidad, para la integración con otros sistemas o para mejorar el rendimiento.

<div id="default-format">
  ## Formato predeterminado
</div>

Si se omite la cláusula `FORMAT`, se usa el formato predeterminado, que depende tanto de la configuración como de la interfaz utilizada para acceder al servidor de ClickHouse. Para la [interfaz HTTP](/es/interfaces/http) y el [client](../../../interfaces/client.md) en modo por lotes, el formato predeterminado es `TabSeparated`. Para el client en modo interactivo, el formato predeterminado es `PrettyCompact` (produce tablas compactas legibles para humanos).

<div id="implementation-details">
  ## Detalles de implementación
</div>

Al usar el cliente de línea de comandos, los datos siempre se transmiten por la red en un formato interno eficiente (`Native`). El client interpreta de forma independiente la cláusula `FORMAT` de la consulta y da formato a los datos por sí mismo (lo que evita una carga adicional en la red y el servidor).