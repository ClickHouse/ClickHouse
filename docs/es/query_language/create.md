# CREATE Consultas {#create-queries}

## CREAR BASE DE DATOS {#query-language-create-database}

Crea una base de datos.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Clausula {#clauses}

-   `IF NOT EXISTS`

        If the `db_name` database already exists, then ClickHouse doesn't create a new database and:

        - Doesn't throw an exception if clause is specified.
        - Throws an exception if clause isn't specified.

-   `ON CLUSTER`

        ClickHouse creates the `db_name` database on all the servers of a specified cluster.

-   `ENGINE`

        - [MySQL](../database_engines/mysql.md)

            Allows you to retrieve data from the remote MySQL server.

        By default, ClickHouse uses its own [database engine](../database_engines/index.md).

## CREAR TABLA {#create-table-query}

El `CREATE TABLE` consulta puede tener varias formas.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

Crea una tabla llamada ‘name’ en el ‘db’ base de datos o la base de datos actual si ‘db’ no está establecida, con la estructura especificada entre paréntesis y ‘engine’ motor.
La estructura de la tabla es una lista de descripciones de columnas. Si los índices son compatibles con el motor, se indican como parámetros para el motor de tablas.

Una descripción de columna es `name type` en el caso más simple. Ejemplo: `RegionID UInt32`.
Las expresiones también se pueden definir para los valores predeterminados (ver más abajo).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Crea una tabla con la misma estructura que otra tabla. Puede especificar un motor diferente para la tabla. Si no se especifica el motor, se utilizará el mismo motor que para el `db2.name2` tabla.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Crea una tabla con la estructura y los datos [función de la tabla](table_functions/index.md).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Crea una tabla con una estructura como el resultado de la `SELECT` consulta, con el ‘engine’ motor, y lo llena con datos de SELECT.

En todos los casos, si `IF NOT EXISTS` Si se especifica la tabla, la consulta no devolverá un error si la tabla ya existe. En este caso, la consulta no hará nada.

Puede haber otras cláusulas después del `ENGINE` cláusula en la consulta. Consulte la documentación detallada sobre cómo crear tablas en las descripciones de [motores de mesa](../operations/table_engines/index.md#table_engines).

### Valores predeterminados {#create-default-values}

La descripción de la columna puede especificar una expresión para un valor predeterminado, de una de las siguientes maneras:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Ejemplo: `URLDomain String DEFAULT domain(URL)`.

Si no se define una expresión para el valor predeterminado, los valores predeterminados se establecerán en ceros para números, cadenas vacías para cadenas, matrices vacías para matrices y `0000-00-00` para fechas o `0000-00-00 00:00:00` para las fechas con el tiempo. Los NULL no son compatibles.

Si se define la expresión predeterminada, el tipo de columna es opcional. Si no hay un tipo definido explícitamente, se utiliza el tipo de expresión predeterminado. Ejemplo: `EventDate DEFAULT toDate(EventTime)` – el ‘Date’ tipo será utilizado para el ‘EventDate’ columna.

Si el tipo de datos y la expresión predeterminada se definen explícitamente, esta expresión se convertirá al tipo especificado utilizando funciones de conversión de tipos. Ejemplo: `Hits UInt32 DEFAULT 0` significa lo mismo que `Hits UInt32 DEFAULT toUInt32(0)`.

Las expresiones predeterminadas se pueden definir como una expresión arbitraria de las constantes y columnas de la tabla. Al crear y cambiar la estructura de la tabla, comprueba que las expresiones no contengan bucles. Para INSERT, comprueba que las expresiones se puedan resolver, que se hayan pasado todas las columnas a partir de las que se pueden calcular.

`DEFAULT expr`

Valor predeterminado Normal. Si la consulta INSERT no especifica la columna correspondiente, se completará calculando la expresión correspondiente.

`MATERIALIZED expr`

Expresión materializada. Dicha columna no se puede especificar para INSERT, porque siempre se calcula.
Para un INSERT sin una lista de columnas, estas columnas no se consideran.
Además, esta columna no se sustituye cuando se utiliza un asterisco en una consulta SELECT. Esto es para preservar el invariante que el volcado obtuvo usando `SELECT *` se puede volver a insertar en la tabla usando INSERT sin especificar la lista de columnas.

`ALIAS expr`

Sinónimo. Dicha columna no se almacena en la tabla en absoluto.
Sus valores no se pueden insertar en una tabla y no se sustituyen cuando se utiliza un asterisco en una consulta SELECT.
Se puede usar en SELECT si el alias se expande durante el análisis de consultas.

Cuando se utiliza la consulta ALTER para agregar nuevas columnas, no se escriben datos antiguos para estas columnas. En su lugar, al leer datos antiguos que no tienen valores para las nuevas columnas, las expresiones se calculan sobre la marcha de forma predeterminada. Sin embargo, si la ejecución de las expresiones requiere diferentes columnas que no están indicadas en la consulta, estas columnas se leerán adicionalmente, pero solo para los bloques de datos que lo necesitan.

Si agrega una nueva columna a una tabla pero luego cambia su expresión predeterminada, los valores utilizados para los datos antiguos cambiarán (para los datos donde los valores no se almacenaron en el disco). Tenga en cuenta que cuando se ejecutan combinaciones en segundo plano, los datos de las columnas que faltan en una de las partes de combinación se escriben en la parte combinada.

No es posible establecer valores predeterminados para elementos en estructuras de datos anidadas.

### Limitación {#constraints}

Junto con las descripciones de columnas, se podrían definir restricciones:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` podría por cualquier expresión booleana. Si se definen restricciones para la tabla, cada una de ellas se verificará para cada fila en `INSERT` consulta. Si no se cumple alguna restricción, el servidor generará una excepción con el nombre de la restricción y la expresión de comprobación.

Agregar una gran cantidad de restricciones puede afectar negativamente el rendimiento de grandes `INSERT` consulta.

### Expresión TTL {#ttl-expression}

Define el tiempo de almacenamiento de los valores. Solo se puede especificar para tablas de la familia MergeTree. Para la descripción detallada, ver [TTL para columnas y tablas](../operations/table_engines/mergetree.md#table_engine-mergetree-ttl).

### Códecs de compresión de columna {#codecs}

De forma predeterminada, ClickHouse aplica el `lz4` método de compresión. Para `MergeTree`- familia de motor puede cambiar el método de compresión predeterminado en el [compresión](../operations/server_settings/settings.md#server-settings-compression) sección de una configuración de servidor. También puede definir el método de compresión para cada columna `CREATE TABLE` consulta.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Si se especifica un códec, el códec predeterminado no se aplica. Los códecs se pueden combinar en una tubería, por ejemplo, `CODEC(Delta, ZSTD)`. Para seleccionar la mejor combinación de códecs para su proyecto, pase puntos de referencia similares a los descritos en Altinity [Nuevas codificaciones para mejorar la eficiencia de ClickHouse](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) artículo.

!!! warning "Advertencia"
    No puede descomprimir archivos de base de datos ClickHouse con utilidades externas como `lz4`. En su lugar, use el especial [Compresor de clickhouse](https://github.com/ClickHouse/ClickHouse/tree/master/dbms/programs/compressor) utilidad.

La compresión es compatible con los siguientes motores de tablas:

-   [Método de codificación de datos:](../operations/table_engines/mergetree.md) familia. Admite códecs de compresión de columnas y selecciona el método de compresión predeterminado mediante [compresión](../operations/server_settings/settings.md#server-settings-compression) configuración.
-   [Registro](../operations/table_engines/log_family.md) familia. Utiliza el `lz4` método de compresión por defecto y soporta códecs de compresión de columna.
-   [Establecer](../operations/table_engines/set.md). Solo admite la compresión predeterminada.
-   [Unir](../operations/table_engines/join.md). Solo admite la compresión predeterminada.

ClickHouse admite códecs de propósito común y códecs especializados.

#### Especializados Codecs {#create-query-specialized-codecs}

Estos códecs están diseñados para hacer que la compresión sea más efectiva mediante el uso de características específicas de los datos. Algunos de estos códecs no comprimen los propios datos. En su lugar, preparan los datos para un códec de propósito común, que lo comprime mejor que sin esta preparación.

Especializados codecs:

-   `Delta(delta_bytes)` — Enfoque de compresión en el que los valores brutos se sustituyen por la diferencia de dos valores vecinos, excepto el primer valor que permanece sin cambios. Hasta `delta_bytes` se utilizan para almacenar valores delta, por lo que `delta_bytes` es el tamaño máximo de los valores brutos. Posible `delta_bytes` valores: 1, 2, 4, 8. El valor predeterminado para `delta_bytes` ser `sizeof(type)` si es igual a 1, 2, 4 u 8. En todos los demás casos, es 1.
-   `DoubleDelta` — Calcula delta de deltas y lo escribe en forma binaria compacta. Las tasas de compresión óptimas se logran para secuencias monotónicas con una zancada constante, como los datos de series de tiempo. Se puede utilizar con cualquier tipo de ancho fijo. Implementa el algoritmo utilizado en Gorilla TSDB, extendiéndolo para admitir tipos de 64 bits. Utiliza 1 bit adicional para deltas de 32 bytes: prefijos de 5 bits en lugar de prefijos de 4 bits. Para obtener información adicional, consulte Compresión de sellos de tiempo en [Gorila: Una base de datos de series temporales rápida, escalable y en memoria](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` - Calcula XOR entre el valor actual y el anterior y lo escribe en forma binaria compacta. Eficiente al almacenar una serie de valores de coma flotante que cambian lentamente, porque la mejor tasa de compresión se logra cuando los valores vecinos son binarios iguales. Implementa el algoritmo utilizado en Gorilla TSDB, extendiéndolo para admitir tipos de 64 bits. Para obtener información adicional, consulte Comprimir valores en [Gorila: Una base de datos de series temporales rápida, escalable y en memoria](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Enfoque de compresión que recorta bits altos no utilizados de valores en tipos de datos enteros (incluidos `Enum`, `Date` y `DateTime`). En cada paso de su algoritmo, el códec toma un bloque de 64 valores, los coloca en una matriz de 64x64 bits, lo transpone, recorta los bits de valores no utilizados y devuelve el resto como una secuencia. Los bits no utilizados son los bits, que no difieren entre los valores máximo y mínimo en toda la parte de datos para la que se utiliza la compresión.

`DoubleDelta` y `Gorilla` códecs se utilizan en Gorilla TSDB como los componentes de su algoritmo de compresión. El enfoque de gorila es efectivo en escenarios en los que hay una secuencia de valores que cambian lentamente con sus marcas de tiempo. Las marcas de tiempo se comprimen efectivamente por el `DoubleDelta` códec, y los valores son efectivamente comprimidos por el `Gorilla` códec. Por ejemplo, para obtener una tabla almacenada efectivamente, puede crearla en la siguiente configuración:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### Propósito común codecs {#create-query-common-purpose-codecs}

Códecs:

-   `NONE` — Sin compresión.
-   `LZ4` — Lossless [algoritmo de compresión de datos](https://github.com/lz4/lz4) utilizado por defecto. Aplica compresión rápida LZ4.
-   `LZ4HC[(level)]` — Algoritmo LZ4 HC (alta compresión) con nivel configurable. Nivel predeterminado: 9. Configuración `level <= 0` aplica el nivel predeterminado. Niveles posibles: \[1, 12\]. Rango de nivel recomendado: \[4, 9\].
-   `ZSTD[(level)]` — [Algoritmo de compresión ZSTD](https://en.wikipedia.org/wiki/Zstandard) con configurable `level`. Niveles posibles: \[1, 22\]. Valor predeterminado: 1.

Los altos niveles de compresión son útiles para escenarios asimétricos, como comprimir una vez, descomprimir repetidamente. Los niveles más altos significan una mejor compresión y un mayor uso de la CPU.

## Tablas temporales {#temporary-tables}

ClickHouse admite tablas temporales que tienen las siguientes características:

-   Las tablas temporales desaparecen cuando finaliza la sesión, incluso si se pierde la conexión.
-   Una tabla temporal solo utiliza el motor de memoria.
-   No se puede especificar la base de datos para una tabla temporal. Se crea fuera de las bases de datos.
-   Imposible crear una tabla temporal con consulta DDL distribuida en todos los servidores de clúster (mediante `ON CLUSTER`): esta tabla sólo existe en la sesión actual.
-   Si una tabla temporal tiene el mismo nombre que otra y una consulta especifica el nombre de la tabla sin especificar la base de datos, se utilizará la tabla temporal.
-   Para el procesamiento de consultas distribuidas, las tablas temporales utilizadas en una consulta se pasan a servidores remotos.

Para crear una tabla temporal, utilice la siguiente sintaxis:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

En la mayoría de los casos, las tablas temporales no se crean manualmente, sino cuando se utilizan datos externos para una consulta o para `(GLOBAL) IN`. Para obtener más información, consulte las secciones correspondientes

Es posible utilizar tablas con [MOTOR = Memoria](../operations/table_engines/memory.md) en lugar de tablas temporales.

## Consultas DDL distribuidas (cláusula ON CLUSTER) {#distributed-ddl-queries-on-cluster-clause}

El `CREATE`, `DROP`, `ALTER`, y `RENAME` las consultas admiten la ejecución distribuida en un clúster.
Por ejemplo, la siguiente consulta crea el `all_hits` `Distributed` la tabla en cada host `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Para ejecutar estas consultas correctamente, cada host debe tener la misma definición de clúster (para simplificar la sincronización de configuraciones, puede usar sustituciones de ZooKeeper). También deben conectarse a los servidores ZooKeeper.
La versión local de la consulta finalmente se implementará en cada host del clúster, incluso si algunos hosts no están disponibles actualmente. El orden para ejecutar consultas dentro de un único host está garantizado.

## CREAR VISTA {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Crea una vista. Hay dos tipos de vistas: normal y MATERIALIZADO.

Las vistas normales no almacenan ningún dato, sino que solo realizan una lectura desde otra tabla. En otras palabras, una vista normal no es más que una consulta guardada. Al leer desde una vista, esta consulta guardada se utiliza como una subconsulta en la cláusula FROM.

Como ejemplo, suponga que ha creado una vista:

``` sql
CREATE VIEW view AS SELECT ...
```

y escribió una consulta:

``` sql
SELECT a, b, c FROM view
```

Esta consulta es totalmente equivalente a usar la subconsulta:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

Las vistas materializadas almacenan datos transformados por la consulta SELECT correspondiente.

Al crear una vista materializada sin `TO [db].[table]`, debe especificar ENGINE – el motor de tabla para almacenar datos.

Al crear una vista materializada con `TO [db].[table]` usted no debe usar `POPULATE`.

Una vista materializada se organiza de la siguiente manera: al insertar datos en la tabla especificada en SELECT, parte de los datos insertados se convierte mediante esta consulta SELECT y el resultado se inserta en la vista.

Si especifica POPULATE, los datos de tabla existentes se insertan en la vista al crearlos, como si `CREATE TABLE ... AS SELECT ...` . De lo contrario, la consulta solo contiene los datos insertados en la tabla después de crear la vista. No recomendamos usar POPULATE, ya que los datos insertados en la tabla durante la creación de la vista no se insertarán en ella.

Un `SELECT` consulta puede contener `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Tenga en cuenta que las conversiones correspondientes se realizan de forma independiente en cada bloque de datos insertados. Por ejemplo, si `GROUP BY` se establece, los datos se agregan durante la inserción, pero solo dentro de un solo paquete de datos insertados. Los datos no se agregarán más. La excepción es cuando se utiliza un ENGINE que realiza de forma independiente la agregación de datos, como `SummingMergeTree`.

La ejecución de `ALTER` las consultas sobre vistas materializadas no se han desarrollado completamente, por lo que podrían ser inconvenientes. Si la vista materializada utiliza la construcción `TO [db.]name` puede `DETACH` la vista, ejecutar `ALTER` para la tabla de destino, y luego `ATTACH` el previamente separado (`DETACH`) vista.

Las vistas tienen el mismo aspecto que las tablas normales. Por ejemplo, se enumeran en el resultado de la `SHOW TABLES` consulta.

No hay una consulta independiente para eliminar vistas. Para eliminar una vista, utilice `DROP TABLE`.

## CREAR DICCIONARIO {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

Crear [diccionario externo](dicts/external_dicts.md) con dado [estructura](dicts/external_dicts_dict_structure.md), [fuente](dicts/external_dicts_dict_sources.md), [diseño](dicts/external_dicts_dict_layout.md) y [vida](dicts/external_dicts_dict_lifetime.md).

La estructura del diccionario externo consta de atributos. Los atributos de diccionario se especifican de manera similar a las columnas de la tabla. La única propiedad de atributo requerida es su tipo, todas las demás propiedades pueden tener valores predeterminados.

Dependiendo del diccionario [diseño](dicts/external_dicts_dict_layout.md) se pueden especificar uno o más atributos como claves de diccionario.

Para obtener más información, consulte [Diccionarios externos](dicts/external_dicts.md) apartado.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/create/) <!--hide-->
