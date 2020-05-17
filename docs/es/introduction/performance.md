---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 6
toc_title: Rendimiento
---

# Rendimiento {#performance}

De acuerdo con los resultados de las pruebas internas en Yandex, ClickHouse muestra el mejor rendimiento (tanto el mayor rendimiento para consultas largas como la menor latencia en consultas cortas) para escenarios operativos comparables entre los sistemas de su clase que estaban disponibles para pruebas. Puede ver los resultados de la prueba en un [página separada](https://clickhouse.tech/benchmark/dbms/).

Numerosos puntos de referencia independientes llegaron a conclusiones similares. No son difíciles de encontrar mediante una búsqueda en Internet, o se puede ver [nuestra pequeña colección de enlaces relacionados](https://clickhouse.tech/#independent-benchmarks).

## Rendimiento para una única consulta grande {#throughput-for-a-single-large-query}

El rendimiento se puede medir en filas por segundo o megabytes por segundo. Si los datos se colocan en la caché de la página, una consulta que no es demasiado compleja se procesa en hardware moderno a una velocidad de aproximadamente 2-10 GB / s de datos sin comprimir en un solo servidor (para los casos más sencillos, la velocidad puede alcanzar 30 GB / s). Si los datos no se colocan en la memoria caché de la página, la velocidad depende del subsistema de disco y la velocidad de compresión de datos. Por ejemplo, si el subsistema de disco permite leer datos a 400 MB/s y la tasa de compresión de datos es 3, se espera que la velocidad sea de alrededor de 1,2 GB/s. Para obtener la velocidad en filas por segundo, divida la velocidad en bytes por segundo por el tamaño total de las columnas utilizadas en la consulta. Por ejemplo, si se extraen 10 bytes de columnas, se espera que la velocidad sea de alrededor de 100-200 millones de filas por segundo.

La velocidad de procesamiento aumenta casi linealmente para el procesamiento distribuido, pero solo si el número de filas resultantes de la agregación o la clasificación no es demasiado grande.

## Latencia al procesar consultas cortas {#latency-when-processing-short-queries}

Si una consulta usa una clave principal y no selecciona demasiadas columnas y filas para procesar (cientos de miles), puede esperar menos de 50 milisegundos de latencia (dígitos individuales de milisegundos en el mejor de los casos) si los datos se colocan en la memoria caché de la página. De lo contrario, la latencia está dominada principalmente por el número de búsquedas. Si utiliza unidades de disco giratorias, para un sistema que no está sobrecargado, la latencia se puede estimar con esta fórmula: `seek time (10 ms) * count of columns queried * count of data parts`.

## Rendimiento al procesar una gran cantidad de consultas cortas {#throughput-when-processing-a-large-quantity-of-short-queries}

En las mismas condiciones, ClickHouse puede manejar varios cientos de consultas por segundo en un solo servidor (hasta varios miles en el mejor de los casos). Dado que este escenario no es típico para DBMS analíticos, se recomienda esperar un máximo de 100 consultas por segundo.

## Rendimiento al insertar datos {#performance-when-inserting-data}

Recomendamos insertar datos en paquetes de al menos 1000 filas o no más de una sola solicitud por segundo. Al insertar en una tabla MergeTree desde un volcado separado por tabuladores, la velocidad de inserción puede ser de 50 a 200 MB/s. Si las filas insertadas tienen alrededor de 1 Kb de tamaño, la velocidad será de 50,000 a 200,000 filas por segundo. Si las filas son pequeñas, el rendimiento puede ser mayor en filas por segundo (en los datos del sistema Banner -`>` 500.000 filas por segundo; en datos de grafito -`>` 1.000.000 de filas por segundo). Para mejorar el rendimiento, puede realizar varias consultas INSERT en paralelo, que se escala linealmente.

[Artículo Original](https://clickhouse.tech/docs/en/introduction/performance/) <!--hide-->
