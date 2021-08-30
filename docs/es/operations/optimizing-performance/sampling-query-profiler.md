---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Generaci\xF3n de perfiles de consultas"
---

# Analizador de consultas de muestreo {#sampling-query-profiler}

ClickHouse ejecuta el generador de perfiles de muestreo que permite analizar la ejecución de consultas. Utilizando el generador de perfiles puede encontrar rutinas de código fuente que se utilizan con más frecuencia durante la ejecución de la consulta. Puede rastrear el tiempo de CPU y el tiempo de reloj de pared invertido, incluido el tiempo de inactividad.

Para usar el generador de perfiles:

-   Configurar el [trace_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) sección de la configuración del servidor.

    Esta sección configura la [trace_log](../../operations/system-tables.md#system_tables-trace_log) tabla del sistema que contiene los resultados del funcionamiento del generador de perfiles. Está configurado de forma predeterminada. Recuerde que los datos de esta tabla solo son válidos para un servidor en ejecución. Después de reiniciar el servidor, ClickHouse no limpia la tabla y toda la dirección de memoria virtual almacenada puede dejar de ser válida.

-   Configurar el [Los resultados de la prueba](../settings/settings.md#query_profiler_cpu_time_period_ns) o [query_profiler_real_time_period_ns](../settings/settings.md#query_profiler_real_time_period_ns) configuración. Ambos ajustes se pueden utilizar simultáneamente.

    Estas opciones le permiten configurar temporizadores del generador de perfiles. Como estos son los ajustes de sesión, puede obtener diferentes frecuencias de muestreo para todo el servidor, usuarios individuales o perfiles de usuario, para su sesión interactiva y para cada consulta individual.

La frecuencia de muestreo predeterminada es una muestra por segundo y tanto la CPU como los temporizadores reales están habilitados. Esta frecuencia permite recopilar suficiente información sobre el clúster ClickHouse. Al mismo tiempo, al trabajar con esta frecuencia, el generador de perfiles no afecta el rendimiento del servidor ClickHouse. Si necesita perfilar cada consulta individual, intente usar una mayor frecuencia de muestreo.

Para analizar el `trace_log` tabla del sistema:

-   Instale el `clickhouse-common-static-dbg` paquete. Ver [Instalar desde paquetes DEB](../../getting-started/install.md#install-from-deb-packages).

-   Permitir funciones de introspección [allow_introspection_functions](../settings/settings.md#settings-allow_introspection_functions) configuración.

    Por razones de seguridad, las funciones de introspección están deshabilitadas de forma predeterminada.

-   Utilice el `addressToLine`, `addressToSymbol` y `demangle` [funciones de la introspección](../../sql-reference/functions/introspection.md) para obtener nombres de funciones y sus posiciones en el código ClickHouse. Para obtener un perfil para alguna consulta, debe agregar datos del `trace_log` tabla. Puede agregar datos por funciones individuales o por los seguimientos de pila completos.

Si necesita visualizar `trace_log` información, intente [Flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) y [Nivel de Cifrado WEP](https://github.com/laplab/clickhouse-speedscope).

## Ejemplo {#example}

En este ejemplo nos:

-   Filtrado `trace_log` datos por un identificador de consulta y la fecha actual.

-   Agregando por seguimiento de pila.

-   Usando funciones de introspección, obtendremos un informe de:

    -   Nombres de símbolos y funciones de código fuente correspondientes.
    -   Ubicaciones del código fuente de estas funciones.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
