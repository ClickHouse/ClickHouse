---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Familia de registro
toc_priority: 29
toc_title: "Implantaci\xF3n"
---

# Familia del motor de registro {#log-engine-family}

Estos motores fueron desarrollados para escenarios en los que necesita escribir rápidamente muchas tablas pequeñas (hasta aproximadamente 1 millón de filas) y leerlas más tarde en su conjunto.

Motores de la familia:

-   [StripeLog](stripelog.md)
-   [Registro](log.md)
-   [TinyLog](tinylog.md)

## Propiedades comunes {#common-properties}

Motor:

-   Almacenar datos en un disco.

-   Agregue datos al final del archivo al escribir.

-   Bloqueos de soporte para el acceso a datos simultáneos.

    Durante `INSERT` consultas, la tabla está bloqueada y otras consultas para leer y escribir datos esperan a que la tabla se desbloquee. Si no hay consultas de escritura de datos, se puede realizar cualquier número de consultas de lectura de datos simultáneamente.

-   No apoyo [mutación](../../../sql-reference/statements/alter.md#alter-mutations) operación.

-   No admite índices.

    Esto significa que `SELECT` las consultas para rangos de datos no son eficientes.

-   No escriba datos atómicamente.

    Puede obtener una tabla con datos dañados si algo rompe la operación de escritura, por ejemplo, un cierre anormal del servidor.

## Diferencia {#differences}

El `TinyLog` es el más simple de la familia y proporciona la funcionalidad más pobre y la eficiencia más baja. El `TinyLog` el motor no admite la lectura de datos paralelos por varios hilos. Lee datos más lentamente que otros motores de la familia que admiten lectura paralela y utiliza casi tantos descriptores como los `Log` motor porque almacena cada columna en un archivo separado. Úselo en escenarios simples de baja carga.

El `Log` y `StripeLog` Los motores admiten lectura de datos paralela. Al leer datos, ClickHouse usa múltiples hilos. Cada subproceso procesa un bloque de datos separado. El `Log` utiliza un archivo separado para cada columna de la tabla. `StripeLog` almacena todos los datos en un archivo. Como resultado, el `StripeLog` el motor utiliza menos descriptores en el sistema operativo, pero el `Log` proporciona una mayor eficiencia al leer datos.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/log_family/) <!--hide-->
