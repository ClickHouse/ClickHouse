---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 4
toc_title: "Caracter\xEDsticas distintivas"
---

# Características distintivas de ClickHouse {#distinctive-features-of-clickhouse}

## DBMS orientado a columnas verdaderas {#true-column-oriented-dbms}

En un verdadero DBMS orientado a columnas, no se almacenan datos adicionales con los valores. Entre otras cosas, esto significa que los valores de longitud constante deben ser compatibles, para evitar almacenar su longitud “number” al lado de los valores. Como ejemplo, mil millones de valores de tipo UInt8 deberían consumir alrededor de 1 GB sin comprimir, o esto afecta fuertemente el uso de la CPU. Es esencial almacenar los datos de forma compacta (sin “garbage”) incluso sin comprimir, ya que la velocidad de descompresión (uso de CPU) depende principalmente del volumen de datos sin comprimir.

Vale la pena señalar porque hay sistemas que pueden almacenar valores de diferentes columnas por separado, pero que no pueden procesar efectivamente las consultas analíticas debido a su optimización para otros escenarios. Los ejemplos son HBase, BigTable, Cassandra e HyperTable. En estos sistemas, obtendría un rendimiento de alrededor de cien mil filas por segundo, pero no cientos de millones de filas por segundo.

También vale la pena señalar que ClickHouse es un sistema de administración de bases de datos, no una sola base de datos. ClickHouse permite crear tablas y bases de datos en tiempo de ejecución, cargar datos y ejecutar consultas sin volver a configurar y reiniciar el servidor.

## Compresión de datos {#data-compression}

Algunos DBMS orientados a columnas (InfiniDB CE y MonetDB) no utilizan la compresión de datos. Sin embargo, la compresión de datos juega un papel clave para lograr un rendimiento excelente.

## Almacenamiento en disco de datos {#disk-storage-of-data}

Mantener los datos físicamente ordenados por clave principal permite extraer datos para sus valores específicos o rangos de valores con baja latencia, menos de unas pocas docenas de milisegundos. Algunos DBMS orientados a columnas (como SAP HANA y Google PowerDrill) solo pueden funcionar en RAM. Este enfoque fomenta la asignación de un presupuesto de hardware más grande que el necesario para el análisis en tiempo real. ClickHouse está diseñado para funcionar en discos duros normales, lo que significa que el costo por GB de almacenamiento de datos es bajo, pero SSD y RAM adicional también se utilizan completamente si están disponibles.

## Procesamiento paralelo en varios núcleos {#parallel-processing-on-multiple-cores}

Las consultas grandes se paralelizan naturalmente, tomando todos los recursos necesarios disponibles en el servidor actual.

## Procesamiento distribuido en varios servidores {#distributed-processing-on-multiple-servers}

Casi ninguno de los DBMS columnar mencionados anteriormente tiene soporte para el procesamiento de consultas distribuidas.
En ClickHouse, los datos pueden residir en diferentes fragmentos. Cada fragmento puede ser un grupo de réplicas utilizadas para la tolerancia a errores. Todos los fragmentos se utilizan para ejecutar una consulta en paralelo, de forma transparente para el usuario.

## Soporte SQL {#sql-support}

ClickHouse admite un lenguaje de consulta declarativo basado en SQL que es idéntico al estándar SQL en muchos casos.
Las consultas admitidas incluyen GROUP BY, ORDER BY, subconsultas en cláusulas FROM, IN y JOIN y subconsultas escalares.
No se admiten subconsultas y funciones de ventana dependientes.

## Motor del vector {#vector-engine}

Los datos no solo se almacenan mediante columnas, sino que se procesan mediante vectores (partes de columnas), lo que permite lograr una alta eficiencia de CPU.

## Actualizaciones de datos en tiempo real {#real-time-data-updates}

ClickHouse admite tablas con una clave principal. Para realizar consultas rápidamente en el rango de la clave principal, los datos se ordenan de forma incremental utilizando el árbol de combinación. Debido a esto, los datos se pueden agregar continuamente a la tabla. No se toman bloqueos cuando se ingieren nuevos datos.

## Indice {#index}

Tener un dato ordenado físicamente por clave principal permite extraer datos para sus valores específicos o rangos de valores con baja latencia, menos de unas pocas docenas de milisegundos.

## Adecuado para consultas en línea {#suitable-for-online-queries}

La baja latencia significa que las consultas se pueden procesar sin demora y sin intentar preparar una respuesta por adelantado, justo en el mismo momento mientras se carga la página de la interfaz de usuario. En otras palabras, en línea.

## Soporte para cálculos aproximados {#support-for-approximated-calculations}

ClickHouse proporciona varias formas de intercambiar precisión por rendimiento:

1.  Funciones agregadas para el cálculo aproximado del número de valores distintos, medianas y cuantiles.
2.  Ejecutar una consulta basada en una parte (muestra) de datos y obtener un resultado aproximado. En este caso, se recuperan proporcionalmente menos datos del disco.
3.  Ejecutar una agregación para un número limitado de claves aleatorias, en lugar de para todas las claves. Bajo ciertas condiciones para la distribución de claves en los datos, esto proporciona un resultado razonablemente preciso mientras se utilizan menos recursos.

## Replicación de datos e integridad de datos {#data-replication-and-data-integrity-support}

ClickHouse utiliza la replicación multi-maestro asincrónica. Después de escribir en cualquier réplica disponible, todas las réplicas restantes recuperan su copia en segundo plano. El sistema mantiene datos idénticos en diferentes réplicas. La recuperación después de la mayoría de las fallas se realiza automáticamente, o semiautomáticamente en casos complejos.

Para obtener más información, consulte la sección [Replicación de datos](../engines/table-engines/mergetree-family/replication.md).

## Características que pueden considerarse desventajas {#clickhouse-features-that-can-be-considered-disadvantages}

1.  No hay transacciones completas.
2.  Falta de capacidad para modificar o eliminar datos ya insertados con alta tasa y baja latencia. Hay eliminaciones y actualizaciones por lotes disponibles para limpiar o modificar datos, por ejemplo, para cumplir con [GDPR](https://gdpr-info.eu).
3.  El índice disperso hace que ClickHouse no sea tan adecuado para consultas de puntos que recuperan filas individuales por sus claves.

[Artículo Original](https://clickhouse.tech/docs/en/introduction/distinctive_features/) <!--hide-->
