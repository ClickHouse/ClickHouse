---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Requisito
---

# Requisito {#requirements}

## CPU {#cpu}

Para la instalación desde paquetes deb precompilados, utilice una CPU con arquitectura x86_64 y soporte para las instrucciones de SSE 4.2. Para ejecutar ClickHouse con procesadores que no admiten SSE 4.2 o tienen arquitectura AArch64 o PowerPC64LE, debe compilar ClickHouse a partir de fuentes.

ClickHouse implementa el procesamiento de datos paralelo y utiliza todos los recursos de hardware disponibles. Al elegir un procesador, tenga en cuenta que ClickHouse funciona de manera más eficiente en configuraciones con un gran número de núcleos pero con una velocidad de reloj más baja que en configuraciones con menos núcleos y una velocidad de reloj más alta. Por ejemplo, 16 núcleos con 2600 MHz es preferible a 8 núcleos con 3600 MHz.

Se recomienda usar **Impulso de Turbo** y **hiper-threading** tecnología. Mejora significativamente el rendimiento con una carga de trabajo típica.

## RAM {#ram}

Recomendamos utilizar un mínimo de 4 GB de RAM para realizar consultas no triviales. El servidor ClickHouse puede ejecutarse con una cantidad mucho menor de RAM, pero requiere memoria para procesar consultas.

El volumen requerido de RAM depende de:

-   La complejidad de las consultas.
-   La cantidad de datos que se procesan en las consultas.

Para calcular el volumen requerido de RAM, debe estimar el tamaño de los datos temporales para [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) y otras operaciones que utilice.

ClickHouse puede usar memoria externa para datos temporales. Ver [GROUP BY en memoria externa](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) para más detalles.

## Archivo de intercambio {#swap-file}

Deshabilite el archivo de intercambio para entornos de producción.

## Subsistema de almacenamiento {#storage-subsystem}

Necesita tener 2 GB de espacio libre en disco para instalar ClickHouse.

El volumen de almacenamiento requerido para sus datos debe calcularse por separado. La evaluación debe incluir:

-   Estimación del volumen de datos.

    Puede tomar una muestra de los datos y obtener el tamaño promedio de una fila de ella. Luego multiplique el valor por el número de filas que planea almacenar.

-   El coeficiente de compresión de datos.

    Para estimar el coeficiente de compresión de datos, cargue una muestra de sus datos en ClickHouse y compare el tamaño real de los datos con el tamaño de la tabla almacenada. Por ejemplo, los datos de clickstream generalmente se comprimen de 6 a 10 veces.

Para calcular el volumen final de datos que se almacenarán, aplique el coeficiente de compresión al volumen de datos estimado. Si planea almacenar datos en varias réplicas, multiplique el volumen estimado por el número de réplicas.

## Red {#network}

Si es posible, use redes de 10G o clase superior.

El ancho de banda de la red es fundamental para procesar consultas distribuidas con una gran cantidad de datos intermedios. Además, la velocidad de la red afecta a los procesos de replicación.

## Software {#software}

ClickHouse está desarrollado principalmente para la familia de sistemas operativos Linux. La distribución de Linux recomendada es Ubuntu. El `tzdata` paquete debe ser instalado en el sistema.

ClickHouse también puede funcionar en otras familias de sistemas operativos. Ver detalles en el [Primeros pasos](../getting-started/index.md) sección de la documentación.
