---
machine_translated: true
---

# Rendimiento {#performance}

De acuerdo con los resultados de las pruebas internas en Yandex, ClickHouse muestra el mejor rendimiento (tanto el mayor rendimiento para consultas largas como la menor latencia en consultas cortas) para escenarios operativos comparables entre los sistemas de su clase que estaban disponibles para pruebas. Puede ver los resultados de la prueba en un [página separada](https://clickhouse.tech/benchmark.html).

Esto también ha sido confirmado por numerosos puntos de referencia independientes. No son difíciles de encontrar mediante una búsqueda en Internet, o se puede ver [nuestra pequeña colección de enlaces relacionados](https://clickhouse.tech/#independent-benchmarks).

## Rendimiento para una única consulta grande {#throughput-for-a-single-large-query}

El rendimiento se puede medir en filas por segundo o en megabytes por segundo. Si los datos se colocan en la caché de la página, una consulta que no es demasiado compleja se procesa en hardware moderno a una velocidad de aproximadamente 2-10 GB / s de datos sin comprimir en un solo servidor (para los casos más simples, la velocidad puede alcanzar los 30 GB / s). Si los datos no se colocan en la memoria caché de la página, la velocidad depende del subsistema de disco y la velocidad de compresión de datos. Por ejemplo, si el subsistema de disco permite leer datos a 400 MB/s y la tasa de compresión de datos es de 3, la velocidad será de aproximadamente 1,2 GB/s. Para obtener la velocidad en filas por segundo, divida la velocidad en bytes por segundo por el tamaño total de las columnas utilizadas en la consulta. Por ejemplo, si se extraen 10 bytes de columnas, la velocidad será de alrededor de 100-200 millones de filas por segundo.

La velocidad de procesamiento aumenta casi linealmente para el procesamiento distribuido, pero solo si el número de filas resultantes de la agregación o la clasificación no es demasiado grande.

## Latencia al procesar consultas cortas {#latency-when-processing-short-queries}

Si una consulta usa una clave principal y no selecciona demasiadas filas para procesar (cientos de miles) y no usa demasiadas columnas, podemos esperar menos de 50 milisegundos de latencia (dígitos individuales de milisegundos en el mejor de los casos) si los datos se colocan en la caché de la página. De lo contrario, la latencia se calcula a partir del número de búsquedas. Si utiliza unidades giratorias, para un sistema que no está sobrecargado, la latencia se calcula mediante esta fórmula: tiempo de búsqueda (10 ms) \* número de columnas consultadas \* número de partes de datos.

## Rendimiento al procesar una gran cantidad de consultas cortas {#throughput-when-processing-a-large-quantity-of-short-queries}

En las mismas condiciones, ClickHouse puede manejar varios cientos de consultas por segundo en un solo servidor (hasta varios miles en el mejor de los casos). Dado que este escenario no es típico para DBMS analíticos, se recomienda esperar un máximo de 100 consultas por segundo.

## Rendimiento al insertar datos {#performance-when-inserting-data}

Recomendamos insertar datos en paquetes de al menos 1000 filas o no más de una sola solicitud por segundo. Al insertar en una tabla MergeTree desde un volcado separado por tabuladores, la velocidad de inserción será de 50 a 200 MB / s. Si las filas insertadas tienen un tamaño de aproximadamente 1 Kb, la velocidad será de 50,000 a 200,000 filas por segundo. Si las filas son pequeñas, el rendimiento será mayor en filas por segundo (en los datos del sistema Banner -`>` 500.000 filas por segundo; en datos de grafito -`>` 1.000.000 de filas por segundo). Para mejorar el rendimiento, puede realizar varias consultas INSERT en paralelo, y el rendimiento aumentará linealmente.

[Artículo Original](https://clickhouse.tech/docs/es/introduction/performance/) <!--hide-->
