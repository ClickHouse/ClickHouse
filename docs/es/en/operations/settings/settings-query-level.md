---
description: 'Configuración a nivel de consulta'
sidebar_label: 'Configuración de sesión a nivel de consulta'
slug: /operations/settings/query-level
title: 'Configuración de sesión a nivel de consulta'
doc_type: 'reference'
---

<div id="overview">
  ## Descripción general
</div>

Hay varias formas de ejecutar sentencias con configuraciones específicas.
La configuración se aplica por capas, y cada capa posterior redefine los valores establecidos en la anterior.

<div id="order-of-priority">
  ## Orden de prioridad
</div>

El orden de prioridad para definir una configuración es:

1. Aplicar una configuración directamente a un usuario o dentro de un perfil de configuración

   * SQL (recomendado)
   * añadir uno o más archivos XML o YAML a `/etc/clickhouse-server/users.d`

2. Configuraciones de sesión

   * Envíe `SET setting=value` desde SQL Console de ClickHouse Cloud o con
     `clickhouse client` en modo interactivo. De forma similar, puede usar sesiones de ClickHouse
     en el protocolo HTTP. Para ello, debe especificar el
     parámetro HTTP `session_id`.

3. Configuraciones de consulta

   * Al iniciar `clickhouse client` en modo no interactivo, establezca el parámetro
     de inicio `--setting=value`.
   * Al usar la API HTTP, pase parámetros CGI (`URL?setting_1=value&setting_2=value...`).
   * Defina configuraciones en la
     cláusula [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     de la consulta SELECT. El valor de la configuración se aplica solo a esa consulta
     y se restablece al valor predeterminado o anterior una vez ejecutada la consulta.

<div id="converting-a-setting-to-its-default-value">
  ## Convertir una configuración en su valor predeterminado
</div>

Si cambia una configuración y desea volver a su valor predeterminado, establezca el valor en `DEFAULT`. La sintaxis es la siguiente:

```sql
SET setting_name = DEFAULT
```

Por ejemplo, el valor predeterminado de `async_insert` es `0`. Supongamos que cambia este valor a `1`:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

La respuesta es:

```response
┌─value──┐
│ 1      │
└────────┘
```

El siguiente comando restablece su valor a 0:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

La configuración ha vuelto a su valor predeterminado:

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## Ajustes personalizados
</div>

Además de los [ajustes](/es/operations/settings/settings.md) comunes, los usuarios pueden definir ajustes personalizados.
Los ajustes personalizados permiten pasar **parámetros específicos de la sesión** que pueden referenciarse en consultas, políticas o funciones. Esto resulta útil cuando se necesita:

* Filtrar datos en función de la identidad del usuario o de la organización
* Aplicar una lógica de negocio diferente según el contexto
* Mantener información de estado entre consultas dentro de una sesión

El nombre de un ajuste personalizado debe comenzar con uno de los prefijos predefinidos de una lista que usted defina.
La lista de prefijos puede especificarse mediante el ajuste del servidor [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes), definido en su archivo de configuración del servidor.

En el ejemplo siguiente, se elige `SQL_` como prefijo personalizado:

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
En ClickHouse Cloud no es posible especificar un prefijo personalizado.
Todas las opciones de configuración personalizadas del usuario comienzan con el prefijo `SQL_`.
:::

Para definir una opción de configuración personalizada, use el comando `SET`:

```sql
SET SQL_a = 123;
```

Para obtener el valor actual de una configuración personalizada, utilice la función `getSetting()`:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## Ejemplos
</div>

Todos estos ejemplos establecen el valor del ajuste `async_insert` en `1` y
muestran cómo examinar los ajustes en un sistema en funcionamiento.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### Uso de SQL para aplicar directamente un ajuste a un usuario
</div>

Esto crea el usuario `ingester` con el ajuste `async_inset = 1`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### Revise el perfil de configuración y su asignación
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### Uso de SQL para crear un perfil de configuración y asignárselo a un usuario
</div>

Con esto se crea el perfil `log_ingest` con la configuración `async_inset = 1`:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

Esto crea el usuario `ingester` y le asigna el perfil de configuración `log_ingest`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### Uso de XML para crear un perfil de configuración y un usuario
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment">
  #### Revise el perfil de configuración y su asignación
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### Asignar un ajuste a una sesión
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### Establecer una configuración en una consulta
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## Véase también
</div>

* Consulte la página de [Ajustes](/es/operations/settings/settings.md) para obtener una descripción de los ajustes de ClickHouse.
* [Ajustes globales del servidor](/es/operations/server-configuration-parameters/settings.md)