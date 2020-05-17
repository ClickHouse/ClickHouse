---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 78
toc_title: Preguntas generales
---

# Preguntas generales {#general-questions}

## ¿Por qué no usar algo como MapReduce? {#why-not-use-something-like-mapreduce}

Podemos referirnos a sistemas como MapReduce como sistemas informáticos distribuidos en los que la operación de reducción se basa en la clasificación distribuida. La solución de código abierto más común en esta clase es [Acerca de nosotros](http://hadoop.apache.org). Yandex utiliza su solución interna, YT.

Estos sistemas no son apropiados para consultas en línea debido a su alta latencia. En otras palabras, no se pueden usar como back-end para una interfaz web. Estos tipos de sistemas no son útiles para actualizaciones de datos en tiempo real. La clasificación distribuida no es la mejor manera de realizar operaciones de reducción si el resultado de la operación y todos los resultados intermedios (si los hay) se encuentran en la RAM de un único servidor, que generalmente es el caso de las consultas en línea. En tal caso, una tabla hash es una forma óptima de realizar operaciones de reducción. Un enfoque común para optimizar las tareas de reducción de mapas es la preagregación (reducción parcial) utilizando una tabla hash en RAM. El usuario realiza esta optimización manualmente. La clasificación distribuida es una de las principales causas de un rendimiento reducido cuando se ejecutan tareas simples de reducción de mapas.

La mayoría de las implementaciones de MapReduce le permiten ejecutar código arbitrario en un clúster. Pero un lenguaje de consulta declarativo es más adecuado para OLAP para ejecutar experimentos rápidamente. Por ejemplo, Hadoop tiene Hive y Pig. También considere Cloudera Impala o Shark (obsoleto) para Spark, así como Spark SQL, Presto y Apache Drill. El rendimiento cuando se ejecutan tales tareas es muy subóptimo en comparación con los sistemas especializados, pero la latencia relativamente alta hace que sea poco realista utilizar estos sistemas como back-end para una interfaz web.

## ¿Qué sucede si tengo un problema con las codificaciones al usar Oracle a través de ODBC? {#oracle-odbc-encodings}

Si utiliza Oracle a través del controlador ODBC como fuente de diccionarios externos, debe establecer el valor `NLS_LANG` variable de entorno en `/etc/default/clickhouse`. Para obtener más información, consulte [Oracle NLS\_LANG Preguntas frecuentes](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Ejemplo**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## Cómo exporto datos de ClickHouse a un archivo? {#how-to-export-to-file}

### Uso de la cláusula INTO OUTFILE {#using-into-outfile-clause}

Añadir un [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) cláusula a su consulta.

Por ejemplo:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

De forma predeterminada, ClickHouse usa el [TabSeparated](../interfaces/formats.md#tabseparated) formato de datos de salida. Para seleccionar el [formato de datos](../interfaces/formats.md), utilizar el [Cláusula FORMAT](../sql-reference/statements/select/format.md#format-clause).

Por ejemplo:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Uso de una tabla de motor de archivo {#using-a-file-engine-table}

Ver [File](../engines/table-engines/special/file.md).

### Uso de la redirección de línea de comandos {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Ver [Casa de clics-cliente](../interfaces/cli.md).

{## [Artículo Original](https://clickhouse.tech/docs/en/faq/general/) ##}
