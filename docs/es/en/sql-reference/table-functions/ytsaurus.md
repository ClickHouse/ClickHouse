---
description: 'La función de tabla permite leer datos desde el clúster de YTsaurus.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # Función de tabla YTsaurus
</div>

<ExperimentalBadge />

La función de tabla permite leer datos del clúster de YTsaurus.

<div id="syntax">
  ## Sintaxis
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
Esta es una característica experimental que puede cambiar de forma incompatible con versiones anteriores en futuras versiones.
Habilite el uso de la función de tabla YTsaurus
con la opción de configuración [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/es/operations/settings/settings#allow_experimental_ytsaurus_table_engine).
Introduzca el comando `set allow_experimental_ytsaurus_table_function = 1`.
:::

<div id="arguments">
  ## Argumentos
</div>

* `http_proxy_url` — URL del proxy HTTP de YTsaurus.
* `cypress_path` — Ruta de Cypress a la fuente de datos.
* `oauth_token` — Token de OAuth.
* `format` — El [formato](/es/interfaces/formats) de la fuente de datos.

**Valor devuelto**

Una tabla con la estructura especificada para leer datos desde la ruta de Cypress de YTsaurus especificada en el clúster de YTsaurus.

**Véase también**

* [motor YTsaurus](/es/engines/table-engines/integrations/ytsaurus.md)