---
description: 'Al escribir en una tabla `Null`, los datos se ignoran. Al leer de una
  tabla `Null`, la respuesta está vacía.'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Motor de tabla Null'
doc_type: 'referencia'
---

Al escribir datos en una tabla `Null`, los datos se ignoran.
Al leer de una tabla `Null`, la respuesta está vacía.

El motor de tabla `Null` es útil para transformaciones de datos en las que ya no se necesitan los datos originales una vez transformados.
Para ello, puede crear una vista materializada en una tabla `Null`.
La vista consumirá los datos escritos en la tabla, pero los datos originales sin procesar se descartarán.