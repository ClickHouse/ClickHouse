---
description: 'Página de resumen de la configuración.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'Resumen de la configuración'
doc_type: 'reference'
---

<div id="overview">
  ## Resumen general
</div>

:::note
Los Settings Profiles basados en XML y los [archivos de configuración](/es/operations/configuration-files) no son compatibles actualmente con ClickHouse Cloud. Para especificar ajustes para su servicio de ClickHouse Cloud, debe usar [Settings Profiles basados en SQL](/es/operations/access-rights#settings-profiles-management).
:::

Estos son los principales grupos de ajustes de ClickHouse:

* Ajustes globales del servidor
* Ajustes de sesión
* Ajustes de consulta
* Ajustes de operaciones en segundo plano

Los ajustes globales se aplican de forma predeterminada, salvo que se sobrescriban en niveles más específicos. Los ajustes de sesión pueden especificarse mediante perfiles, la configuración de usuario y los comandos SET. Los ajustes de consulta pueden proporcionarse mediante la cláusula SETTINGS y se aplican a consultas individuales. Los ajustes de operaciones en segundo plano se aplican a Mutations, Merges y, potencialmente, a otras operaciones ejecutadas de forma asíncrona en segundo plano.

<div id="see-non-default-settings">
  ## Ver los ajustes distintos de los predeterminados
</div>

Para ver qué ajustes se han modificado con respecto a su valor predeterminado, puede consultar la
tabla `system.settings`:

```sql
SELECT name, value FROM system.settings WHERE changed
```

Si no se ha modificado ninguna configuración con respecto a su valor predeterminado, ClickHouse
no devolverá nada.

Para comprobar el valor de una configuración concreta, puede especificar el `name` de la
configuración en su consulta:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

Esto devolverá algo así:

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## Más información
</div>

* Consulte [el ajuste global del servidor](/es/operations/server-configuration-parameters/settings.md) para obtener más información sobre cómo configurar su
  servidor de ClickHouse a nivel global.
* Consulte [el ajuste de sesión](/es/operations/settings/settings-query-level.md) para obtener más información sobre cómo configurar su servidor de ClickHouse
  a nivel de sesión.
* Consulte [la jerarquía de Context](/es/development/architecture.md#context) para obtener más información sobre el procesamiento de la configuración en ClickHouse.