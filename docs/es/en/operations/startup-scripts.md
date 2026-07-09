---
description: 'Guía para configurar y usar scripts SQL de inicio en ClickHouse para
  la creación automática del esquema y las migraciones'
sidebar_label: 'Scripts de inicio'
slug: /operations/startup-scripts
title: 'Scripts de inicio'
doc_type: 'guide'
---

ClickHouse puede ejecutar consultas SQL arbitrarias desde la configuración del servidor durante el inicio. Esto puede ser útil para migraciones o para la creación automática del esquema.

```xml
<clickhouse>
    <startup_scripts>
        <throw_on_error>false</throw_on_error>
        <scripts>
            <query>CREATE ROLE OR REPLACE test_role</query>
        </scripts>
        <scripts>
            <query>CREATE TABLE TestTable (id UInt64) ENGINE=TinyLog</query>
            <condition>SELECT 1;</condition>
        </scripts>
        <scripts>
            <query>CREATE DICTIONARY test_dict (...) SOURCE(CLICKHOUSE(...))</query>
            <user>default</user>
        </scripts>
    </startup_scripts>
</clickhouse>
```

ClickHouse ejecuta todas las consultas de `startup_scripts` secuencialmente en el orden especificado. Si alguna de las consultas falla, la ejecución de las consultas siguientes no se interrumpirá. Sin embargo, si `throw_on_error` se establece en `true`,
el servidor no se iniciará si se produce un error durante la ejecución del script.

Puede especificar una consulta condicional en la configuración. En ese caso, la consulta correspondiente solo se ejecuta cuando la consulta de condición devuelve el valor `1` o `true`.

:::note
Si la consulta de condición devuelve cualquier valor distinto de `1` o `true`, el resultado se interpretará como `false` y la consulta correspondiente no se ejecutará.
:::