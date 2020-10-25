---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: CREATE
---

# CREATE Consultas {#create-queries}

## CREATE DATABASE {#query-language-create-database}

Crea una base de datos.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Clausula {#clauses}

-   `IF NOT EXISTS`
    Si el `db_name` base de datos ya existe, entonces ClickHouse no crea una nueva base de datos y:

    -   No lanza una excepción si se especifica una cláusula.
    -   Lanza una excepción si no se especifica la cláusula.

-   `ON CLUSTER`
    ClickHouse crea el `db_name` base de datos en todos los servidores de un clúster especificado.

-   `ENGINE`

    -   [MySQL](../../engines/database-engines/mysql.md)
        Le permite recuperar datos del servidor MySQL remoto.
        De forma predeterminada, ClickHouse usa su propio [motor de base de datos](../../engines/database-engines/index.md).

## CREATE TABLE {#create-table-query}

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

Crea una tabla con la estructura y los datos [función de la tabla](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Crea una tabla con una estructura como el resultado de la `SELECT` consulta, con el ‘engine’ motor, y lo llena con datos de SELECT.

En todos los casos, si `IF NOT EXISTS` se especifica, la consulta no devolverá un error si la tabla ya existe. En este caso, la consulta no hará nada.

Puede haber otras cláusulas después del `ENGINE` cláusula en la consulta. Consulte la documentación detallada sobre cómo crear tablas en las descripciones de [motores de mesa](../../engines/table-engines/index.md#table_engines).

### Valores predeterminados {#create-default-values}

La descripción de la columna puede especificar una expresión para un valor predeterminado, de una de las siguientes maneras:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Ejemplo: `URLDomain String DEFAULT domain(URL)`.

Si no se define una expresión para el valor predeterminado, los valores predeterminados se establecerán en ceros para números, cadenas vacías para cadenas, matrices vacías para matrices y `1970-01-01` para fechas o zero unix timestamp para las fechas con el tiempo. Los NULL no son compatibles.

Si se define la expresión predeterminada, el tipo de columna es opcional. Si no hay un tipo definido explícitamente, se utiliza el tipo de expresión predeterminado. Ejemplo: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ tipo será utilizado para el ‘EventDate’ columna.

Si el tipo de datos y la expresión predeterminada se definen explícitamente, esta expresión se convertirá al tipo especificado utilizando funciones de conversión de tipos. Ejemplo: `Hits UInt32 DEFAULT 0` significa lo mismo que `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don't contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

Valor predeterminado Normal. Si la consulta INSERT no especifica la columna correspondiente, se completará calculando la expresión correspondiente.

`MATERIALIZED expr`

Expresión materializada. Dicha columna no se puede especificar para INSERT, porque siempre se calcula.
Para un INSERT sin una lista de columnas, estas columnas no se consideran.
Además, esta columna no se sustituye cuando se utiliza un asterisco en una consulta SELECT. Esto es para preservar el invariante que el volcado obtuvo usando `SELECT *` se puede volver a insertar en la tabla usando INSERT sin especificar la lista de columnas.

`ALIAS expr`

Sinónimo. Tal columna no se almacena en la tabla en absoluto.
Sus valores no se pueden insertar en una tabla, y no se sustituye cuando se usa un asterisco en una consulta SELECT.
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

`boolean_expr_1` podría por cualquier expresión booleana. Si se definen restricciones para la tabla, cada una de ellas se verificará para cada fila en `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Agregar una gran cantidad de restricciones puede afectar negativamente el rendimiento de grandes `INSERT` consulta.

### Expresión TTL {#ttl-expression}

Define el tiempo de almacenamiento de los valores. Solo se puede especificar para tablas de la familia MergeTree. Para la descripción detallada, ver [TTL para columnas y tablas](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### Códecs de compresión de columna {#codecs}

De forma predeterminada, ClickHouse aplica el `lz4` método de compresión. Para `MergeTree`- familia de motor puede cambiar el método de compresión predeterminado en el [compresión](../../operations/server-configuration-parameters/settings.md#server-settings-compression) sección de una configuración de servidor. También puede definir el método de compresión para cada columna `CREATE TABLE` consulta.

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
    No puede descomprimir archivos de base de datos ClickHouse con utilidades externas como `lz4`. En su lugar, use el especial [Compresor de clickhouse](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) utilidad.

La compresión es compatible con los siguientes motores de tablas:

-   [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) familia. Admite códecs de compresión de columnas y selecciona el método de compresión predeterminado mediante [compresión](../../operations/server-configuration-parameters/settings.md#server-settings-compression) configuración.
-   [Registro](../../engines/table-engines/log-family/index.md) familia. Utiliza el `lz4` método de compresión por defecto y soporta códecs de compresión de columna.
-   [Establecer](../../engines/table-engines/special/set.md). Solo admite la compresión predeterminada.
-   [Unir](../../engines/table-engines/special/join.md). Solo admite la compresión predeterminada.

ClickHouse admite códecs de propósito común y códecs especializados.

#### Especializados Codecs {#create-query-specialized-codecs}

Estos códecs están diseñados para hacer que la compresión sea más efectiva mediante el uso de características específicas de los datos. Algunos de estos códecs no comprimen los datos por sí mismos. En su lugar, preparan los datos para un códec de propósito común, que lo comprime mejor que sin esta preparación.

Especializados codecs:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` se utilizan para almacenar valores delta, por lo que `delta_bytes` es el tamaño máximo de los valores brutos. Posible `delta_bytes` valores: 1, 2, 4, 8. El valor predeterminado para `delta_bytes` ser `sizeof(type)` si es igual a 1, 2, 4 u 8. En todos los demás casos, es 1.
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorila: Una base de datos de series temporales rápida, escalable y en memoria](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorila: Una base de datos de series temporales rápida, escalable y en memoria](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` y `DateTime`). En cada paso de su algoritmo, el códec toma un bloque de 64 valores, los coloca en una matriz de 64x64 bits, lo transpone, recorta los bits de valores no utilizados y devuelve el resto como una secuencia. Los bits no utilizados son los bits, que no difieren entre los valores máximo y mínimo en toda la parte de datos para la que se utiliza la compresión.

`DoubleDelta` y `Gorilla` códecs se utilizan en Gorilla TSDB como los componentes de su algoritmo de compresión. El enfoque de gorila es efectivo en escenarios en los que hay una secuencia de valores que cambian lentamente con sus marcas de tiempo. Las marcas de tiempo se comprimen efectivamente por el `DoubleDelta` códec, y los valores son efectivamente comprimidos por el `Gorilla` códec. Por ejemplo, para obtener una tabla almacenada efectivamente, puede crearla en la siguiente configuración:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### Códecs de uso general {#create-query-general-purpose-codecs}

Códecs:

-   `NONE` — No compression.
-   `LZ4` — Lossless [algoritmo de compresión de datos](https://github.com/lz4/lz4) utilizado por defecto. Aplica compresión rápida LZ4.
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` aplica el nivel predeterminado. Niveles posibles: \[1, 12\]. Rango de nivel recomendado: \[4, 9\].
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

Es posible usar tablas con [MOTOR = Memoria](../../engines/table-engines/special/memory.md) en lugar de tablas temporales.

## Consultas DDL distribuidas (cláusula ON CLUSTER) {#distributed-ddl-queries-on-cluster-clause}

El `CREATE`, `DROP`, `ALTER`, y `RENAME` las consultas admiten la ejecución distribuida en un clúster.
Por ejemplo, la siguiente consulta crea el `all_hits` `Distributed` la tabla en cada host `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Para ejecutar estas consultas correctamente, cada host debe tener la misma definición de clúster (para simplificar la sincronización de configuraciones, puede usar sustituciones de ZooKeeper). También deben conectarse a los servidores ZooKeeper.
La versión local de la consulta finalmente se implementará en cada host del clúster, incluso si algunos hosts no están disponibles actualmente. El orden para ejecutar consultas dentro de un único host está garantizado.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Crea una vista. Hay dos tipos de vistas: normal y MATERIALIZADO.

Las vistas normales no almacenan ningún dato, solo realizan una lectura desde otra tabla. En otras palabras, una vista normal no es más que una consulta guardada. Al leer desde una vista, esta consulta guardada se utiliza como una subconsulta en la cláusula FROM.

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

Al crear una vista materializada sin `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

Al crear una vista materializada con `TO [db].[table]` usted no debe usar `POPULATE`.

Una vista materializada se organiza de la siguiente manera: al insertar datos en la tabla especificada en SELECT, parte de los datos insertados se convierte mediante esta consulta SELECT y el resultado se inserta en la vista.

Si especifica POPULATE, los datos de tabla existentes se insertan en la vista al crearlos, como si `CREATE TABLE ... AS SELECT ...` . De lo contrario, la consulta solo contiene los datos insertados en la tabla después de crear la vista. No recomendamos usar POPULATE, ya que los datos insertados en la tabla durante la creación de la vista no se insertarán en ella.

A `SELECT` consulta puede contener `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` se establece, los datos se agregan durante la inserción, pero solo dentro de un solo paquete de datos insertados. Los datos no se agregarán más. La excepción es cuando se utiliza un ENGINE que realiza de forma independiente la agregación de datos, como `SummingMergeTree`.

La ejecución de `ALTER` las consultas sobre vistas materializadas no se han desarrollado completamente, por lo que podrían ser inconvenientes. Si la vista materializada utiliza la construcción `TO [db.]name` puede `DETACH` la vista, ejecutar `ALTER` para la tabla de destino, y luego `ATTACH` el previamente separado (`DETACH`) vista.

Las vistas tienen el mismo aspecto que las tablas normales. Por ejemplo, se enumeran en el resultado de la `SHOW TABLES` consulta.

No hay una consulta separada para eliminar vistas. Para eliminar una vista, utilice `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
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

Crear [diccionario externo](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) con dado [estructura](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [fuente](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [diseño](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) y [vida](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

La estructura del diccionario externo consta de atributos. Los atributos de diccionario se especifican de manera similar a las columnas de la tabla. La única propiedad de atributo requerida es su tipo, todas las demás propiedades pueden tener valores predeterminados.

Dependiendo del diccionario [diseño](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) se pueden especificar uno o más atributos como claves de diccionario.

Para obtener más información, consulte [Diccionarios externos](../dictionaries/external-dictionaries/external-dicts.md) apartado.

## CREATE USER {#create-user-statement}

Crea un [cuenta de usuario](../../operations/access-rights.md#user-account-management).

### Sintaxis {#create-user-syntax}

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Identificación {#identification}

Hay múltiples formas de identificación del usuario:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` o `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### Anfitrión del usuario {#user-host}

El host de usuario es un host desde el que se podría establecer una conexión con el servidor ClickHouse. El host se puede especificar en el `HOST` sección de consulta de las siguientes maneras:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subred](https://en.wikipedia.org/wiki/Subnetwork). Ejemplos: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. Para su uso en producción, sólo especifique `HOST IP` elementos (direcciones IP y sus máscaras), ya que usan `host` y `host_regexp` podría causar latencia adicional.
-   `HOST ANY` — User can connect from any location. This is default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) expresiones regulares al especificar hosts de usuario. Por ejemplo, `HOST NAME REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you use the [LIKE](../functions/string-search-functions.md#function-like) operador para filtrar los hosts de usuario. Por ejemplo, `HOST LIKE '%'` es equivalente a `HOST ANY`, `HOST LIKE '%.mysite.com'` filtros todos los anfitriones en el `mysite.com` dominio.

Otra forma de especificar el host es usar `@` sintaxis con el nombre de usuario. Ejemplos:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` sintaxis.
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` sintaxis.
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` sintaxis.

!!! info "Advertencia"
    ClickHouse trata `user_name@'address'` como un nombre de usuario en su conjunto. Por lo tanto, técnicamente puede crear múltiples usuarios con `user_name` y diferentes construcciones después `@`. No recomendamos hacerlo.

### Ejemplos {#create-user-examples}

Crear la cuenta de usuario `mira` protegido por la contraseña `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` debe iniciar la aplicación cliente en el host donde se ejecuta el servidor ClickHouse.

Crear la cuenta de usuario `john`, asignarle roles y hacer que estos roles sean predeterminados:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Crear la cuenta de usuario `john` y hacer todos sus roles futuros por defecto:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Cuando se asignará algún rol a `john` en el futuro se convertirá en predeterminado automáticamente.

Crear la cuenta de usuario `john` y hacer todos sus futuros roles por defecto excepto `role1` y `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```

## CREATE ROLE {#create-role-statement}

Crea un [rol](../../operations/access-rights.md#role-management).

### Sintaxis {#create-role-syntax}

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Descripci {#create-role-description}

El rol es un conjunto de [privilegio](grant.md#grant-privileges). Un usuario concedido con un rol obtiene todos los privilegios de este rol.

A un usuario se le pueden asignar varios roles. Los usuarios pueden aplicar sus roles otorgados en combinaciones arbitrarias por el [SET ROLE](misc.md#set-role-statement) instrucción. El ámbito final de los privilegios es un conjunto combinado de todos los privilegios de todos los roles aplicados. Si un usuario tiene privilegios otorgados directamente a su cuenta de usuario, también se combinan con los privilegios otorgados por roles.

El usuario puede tener roles predeterminados que se aplican al iniciar sesión del usuario. Para establecer roles predeterminados, utilice el [SET DEFAULT ROLE](misc.md#set-default-role-statement) declaración o el [ALTER USER](alter.md#alter-user-statement) instrucción.

Para revocar un rol, utilice el [REVOKE](revoke.md) instrucción.

Para eliminar el rol, utilice el [DROP ROLE](misc.md#drop-role-statement) instrucción. El rol eliminado se revoca automáticamente de todos los usuarios y roles a los que se concedió.

### Ejemplos {#create-role-examples}

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Esta secuencia de consultas crea el rol `accountant` que tiene el privilegio de leer datos del `accounting` base.

Conceder el rol al usuario `mira`:

``` sql
GRANT accountant TO mira;
```

Después de conceder el rol, el usuario puede usarlo y realizar las consultas permitidas. Por ejemplo:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

Crea un [filtro para filas](../../operations/access-rights.md#row-policy-management) que un usuario puede leer de una tabla.

### Sintaxis {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Sección AS {#create-row-policy-as}

Con esta sección puede crear políticas permisivas o restrictivas.

La política permisiva concede acceso a las filas. Las políticas permisivas que se aplican a la misma tabla se combinan usando el valor booleano `OR` operador. Las políticas son permisivas de forma predeterminada.

La política restrictiva restringe el acceso a la fila. Las políticas restrictivas que se aplican a la misma tabla se combinan usando el valor booleano `AND` operador.

Las políticas restrictivas se aplican a las filas que pasaron los filtros permisivos. Si establece directivas restrictivas pero no directivas permisivas, el usuario no puede obtener ninguna fila de la tabla.

#### Sección A {#create-row-policy-to}

En la sección `TO` puede dar una lista mixta de roles y usuarios, por ejemplo, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Palabra clave `ALL` significa todos los usuarios de ClickHouse, incluido el usuario actual. Palabras clave `ALL EXCEPT` permitir excluir a algunos usuarios de la lista de todos los usuarios, por ejemplo `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Ejemplos {#examples}

-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`

## CREATE QUOTA {#create-quota-statement}

Crea un [cuota](../../operations/access-rights.md#quotas-management) que se puede asignar a un usuario o a un rol.

### Sintaxis {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Ejemplo {#create-quota-example}

Limite el número máximo de consultas para el usuario actual con 123 consultas en una restricción de 15 meses:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```

## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Crea un [perfil de configuración](../../operations/access-rights.md#settings-profiles-management) que se puede asignar a un usuario o a un rol.

### Sintaxis {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

# Ejemplo {#create-settings-profile-syntax}

Crear el `max_memory_usage_profile` perfil de configuración con valor y restricciones para el `max_memory_usage` configuración. Asignarlo a `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
