# Perfiles de configuración {#settings-profiles}

Un perfil de configuración es una colección de configuraciones agrupadas con el mismo nombre. Cada usuario de ClickHouse tiene un perfil.
Para aplicar todos los ajustes de un perfil, establezca el `profile` configuración.

Ejemplo:

Instale el `web` perfil.

``` sql
SET profile = 'web'
```

Los perfiles de configuración se declaran en el archivo de configuración del usuario. Esto suele ser `users.xml`.

Ejemplo:

``` xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Settings for quries from the user interface -->
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

        <readonly>1</readonly>
    </web>
</profiles>
```

El ejemplo especifica dos perfiles: `default` y `web`. El `default` tiene un propósito especial: siempre debe estar presente y se aplica al iniciar el servidor. En otras palabras, el `default` perfil contiene la configuración predeterminada. El `web` profile es un perfil regular que se puede establecer utilizando el `SET` consulta o utilizando un parámetro URL en una consulta HTTP.

Los perfiles de configuración pueden heredar unos de otros. Para usar la herencia, indique una o varias `profile` configuraciones antes de las demás configuraciones que se enumeran en el perfil. En caso de que se defina una configuración en diferentes perfiles, se utiliza la última definida.

[Artículo Original](https://clickhouse.tech/docs/es/operations/settings/settings_profiles/) <!--hide-->
