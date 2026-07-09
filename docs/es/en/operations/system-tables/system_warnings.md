---
description: 'Esta tabla contiene mensajes de advertencia del servidor de ClickHouse.'
keywords: [ 'tabla del sistema', 'advertencias' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## Descripción
</div>

Esta tabla muestra advertencias sobre el servidor de ClickHouse.
Las advertencias del mismo tipo se combinan en una sola.
Por ejemplo, si el número N de bases de datos adjuntas supera un umbral configurable T, se muestra una única entrada con el valor actual N en lugar de N entradas independientes.
Si el valor actual cae por debajo del umbral, la entrada se elimina de la tabla.

La tabla puede configurarse con estos ajustes:

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/es/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/es/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## Columnas
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — Mensaje de advertencia.
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Cadena de formato utilizada para formatear el mensaje.

<div id="example">
  ## Ejemplo
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```