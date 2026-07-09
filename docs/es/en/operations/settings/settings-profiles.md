---
description: 'Una colección de ajustes agrupados bajo el mismo nombre.'
sidebar_label: 'Perfiles de configuración'
sidebar_position: 61
slug: /operations/settings/settings-profiles
title: 'Perfiles de configuración'
doc_type: 'reference'
---

Un perfil de configuración es una colección de ajustes agrupados bajo el mismo nombre.

:::note
ClickHouse también admite un [flujo de trabajo basado en SQL](/es/operations/access-rights#access-control-usage) para gestionar perfiles de configuración. Recomendamos usarlo.
:::

El perfil puede tener cualquier nombre. Puede especificar el mismo perfil para distintos usuarios. El ajuste más importante que puede incluir en el perfil de configuración es `readonly=1`, que garantiza acceso de solo lectura.

Los perfiles de configuración pueden heredar unos de otros. Para usar la herencia, indique uno o varios ajustes `profile` antes de los demás ajustes incluidos en el perfil. En caso de que un ajuste esté definido en distintos perfiles, se usa el definido más recientemente.

Para aplicar todos los ajustes de un perfil, establezca el ajuste `profile`.

Ejemplo:

Instale el perfil `web`.

```sql
SET profile = 'web'
```

Los perfiles de configuración se definen en el archivo de configuración de usuarios. Normalmente, es `users.xml`.

Ejemplo:

```xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Background operations settings -->
    <background>
        <!-- Re-defining maximum number of threads for background operations -->
        <max_threads>12</max_threads>
    </background>

    <!-- Settings for queries from the user interface -->
    <web>
        <max_rows_to_read>1000000000</max_rows_to_read>
        <max_bytes_to_read>100000000000</max_bytes_to_read>

        <max_rows_to_group_by>1000000</max_rows_to_group_by>
        <group_by_overflow_mode>any</group_by_overflow_mode>

        <max_rows_to_sort>1000000</max_rows_to_sort>
        <max_bytes_to_sort>1000000000</max_bytes_to_sort>

        <max_result_rows>100000</max_result_rows>
        <max_result_bytes>100000000</max_result_bytes>
        <result_overflow_mode>break</result_overflow_mode>

        <max_execution_time>600</max_execution_time>
        <min_execution_speed>1000000</min_execution_speed>
        <timeout_before_checking_execution_speed>15</timeout_before_checking_execution_speed>

        <max_columns_to_read>25</max_columns_to_read>
        <max_temporary_columns>100</max_temporary_columns>
        <max_temporary_non_const_columns>50</max_temporary_non_const_columns>

        <max_subquery_depth>2</max_subquery_depth>
        <max_pipeline_depth>25</max_pipeline_depth>
        <max_ast_depth>50</max_ast_depth>
        <max_ast_elements>100</max_ast_elements>

        <max_sessions_for_user>4</max_sessions_for_user>

        <readonly>1</readonly>
    </web>
</profiles>
```

El ejemplo especifica dos perfiles: `default` y `web`.

El perfil `default` tiene un propósito especial: siempre debe estar presente y se aplica al iniciar el servidor. En otras palabras, el perfil `default` contiene la configuración predeterminada. El nombre del perfil predeterminado se puede cambiar mediante la configuración del servidor `default_profile`.

El perfil `background` tiene un propósito especial: puede estar presente para sobrescribir la configuración de las operaciones en segundo plano. El parámetro es opcional y su nombre se puede cambiar mediante la configuración del servidor `background_profile`.

El perfil `web` es un perfil normal que se puede establecer mediante la consulta `SET` o usando un parámetro de URL en una consulta HTTP.