---
description: 'Un conjunto de datos que siempre está en RAM. Está pensado para usarse en el lado
  derecho del operador `IN`.'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Motor de tabla Set'
doc_type: 'reference'
---

:::note
En ClickHouse Cloud, si su servicio se creó con una versión anterior a la 25.4, deberá establecer la compatibilidad en al menos la 25.4 mediante `SET compatibility=25.4`.
:::

Un conjunto de datos que siempre está en RAM. Está pensado para usarse en el lado derecho del operador `IN` (consulte la sección &quot;operadores IN&quot;).

Puede usar `INSERT` para insertar datos en la tabla. Los elementos nuevos se añadirán al conjunto de datos, mientras que los duplicados se ignorarán.
Pero no puede realizar `SELECT` en la tabla. La única forma de recuperar datos es usándola en la parte derecha del operador `IN`.

Los datos siempre se encuentran en RAM. Para `INSERT`, los bloques de datos insertados también se escriben en el directorio de tablas en el disco. Al iniciar el servidor, estos datos se cargan en RAM. En otras palabras, después de reiniciar, los datos permanecen en su lugar.

En caso de un reinicio brusco del servidor, el bloque de datos en el disco podría perderse o dañarse. En este último caso, puede que necesite eliminar manualmente el archivo con los datos dañados.

<div id="join-limitations-and-settings">
  ### Limitaciones y configuración
</div>

Al crear una tabla, se aplican los siguientes ajustes:

<div id="persistent">
  #### Persistencia
</div>

Desactiva la persistencia de los motores de tabla Set y [Join](/es/engines/table-engines/special/join).

Reduce la sobrecarga de E/S. Adecuado para escenarios que priorizan el rendimiento y no requieren persistencia.

Posibles valores:

* 1 — Habilitado.
* 0 — Deshabilitado.

Valor predeterminado: `1`.