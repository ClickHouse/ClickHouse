---
description: 'Documentación de la tabla'
keywords: ['compresión', 'códec', 'esquema', 'DDL']
sidebar_label: 'TABLE'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Crea una tabla nueva. Esta consulta puede tener varias formas sintácticas según el caso de uso.

De forma predeterminada, las tablas se crean solo en el servidor actual. Las consultas DDL distribuidas se implementan mediante la cláusula `ON CLUSTER`, que se [describe por separado](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## Formas sintácticas
</div>

<div id="with-explicit-schema">
  ### Con esquema explícito
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Crea una tabla llamada `table_name` en la base de datos `db` o en la base de datos actual si no se ha establecido `db`, con la estructura especificada entre corchetes y el motor `engine`.
La estructura de la tabla es una lista de descripciones de columnas, índices secundarios, proyecciones y restricciones. Si el motor admite una [clave primaria](#primary-key), esta se indicará como parámetro del motor de tabla.

Una descripción de columna es `name type` en el caso más simple. Ejemplo: `RegionID UInt32`.

También se pueden definir expresiones para valores predeterminados (véase más abajo).

Si es necesario, se puede especificar la clave primaria, con una o más expresiones de clave.

Se pueden añadir comentarios a las columnas y a la tabla.

<div id="with-a-schema-similar-to-other-table">
  ### Con el esquema de una tabla existente
</div>

ClickHouse permite copiar el esquema y los datos de una tabla existente.

Para replicar el esquema de una tabla existente:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

Esto crea una tabla con la misma estructura que otra tabla.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### Con el esquema y los datos de una tabla existente
</div>

Para replicar el esquema y los datos de una tabla existente:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

Esto crea una tabla con el mismo esquema y los mismos datos que una tabla existente.  Una vez creada la nueva tabla, se le adjuntan todas las particiones de `db.table`. En otras palabras, al crearse, los datos de `db.table` se clonan en `db2.table_clone`. Esta consulta es equivalente a la siguiente:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

Para ambas funcionalidades, puede especificar un motor diferente para la tabla. Si no se especifica el motor, se usará el mismo que para la tabla original (`db.table`).

<div id="from-a-table-function">
  ### A partir de una función de tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Crea una tabla con el mismo resultado que la [función de tabla](/es/sql-reference/table-functions) especificada. La tabla creada también funcionará de la misma manera que la función de tabla correspondiente especificada.

<div id="from-select-query">
  ### A partir de una consulta SELECT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Crea una tabla con una estructura similar a la del resultado de la consulta `SELECT`, con el motor `engine`, y la rellena con datos de `SELECT`. También puede especificar explícitamente la definición de las columnas.

Si la tabla ya existe y se especifica `IF NOT EXISTS`, la consulta no realizará ninguna acción.

Puede haber otras cláusulas después de la cláusula `ENGINE` en la consulta. Consulte la documentación detallada sobre cómo crear tablas en las descripciones de [motores de tabla](/es/engines/table-engines).

**Ejemplo**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## Modificadores NULL o NOT NULL
</div>

Los modificadores `NULL` y `NOT NULL` después del tipo de dato en la definición de una columna permiten o impiden que sea [Nullable](/es/sql-reference/data-types/nullable).

Si el tipo no es `Nullable` y se especifica `NULL`, se tratará como `Nullable`; si se especifica `NOT NULL`, no. Por ejemplo, `INT NULL` es lo mismo que `Nullable(INT)`. Si el tipo es `Nullable` y se especifican los modificadores `NULL` o `NOT NULL`, se generará una excepción.

Véase también el ajuste [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable).

<div id="default_values">
  ## Valores predeterminados
</div>

La descripción de la columna puede especificar una expresión de valor predeterminado con la forma `DEFAULT expr`, `MATERIALIZED expr` o `ALIAS expr`. Ejemplo: `URLDomain String DEFAULT domain(URL)`.

La expresión `expr` es opcional. Si se omite, el tipo de la columna debe especificarse explícitamente y el valor predeterminado será `0` para las columnas numéricas, `''` (la cadena vacía) para las columnas String, `[]` (el array vacío) para las columnas array, `1970-01-01` para las columnas de fecha o `NULL` para las columnas Nullable.

El tipo de una columna con valor predeterminado puede omitirse; en ese caso, se infiere a partir del tipo de `expr`. Por ejemplo, el tipo de la columna `EventDate DEFAULT toDate(EventTime)` será Date.

Si se especifican tanto un tipo de dato como una expresión de valor predeterminado, se inserta una función implícita de conversión de tipos que convierte la expresión al tipo especificado. Ejemplo: `Hits UInt32 DEFAULT 0` se representa internamente como `Hits UInt32 DEFAULT toUInt32(0)`.

Una expresión de valor predeterminado `expr` puede hacer referencia a columnas de la tabla arbitrarias y a constantes. ClickHouse comprueba que los cambios en la estructura de la tabla no introduzcan bucles en el cálculo de la expresión. En el caso de INSERT, comprueba que las expresiones puedan resolverse; es decir, que se hayan pasado todas las columnas a partir de las cuales pueden calcularse.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

Valor predeterminado normal. Si no se especifica el valor de una columna de este tipo en una consulta `INSERT`, se calcula a partir de `expr`.

Ejemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

Expresión materializada. Los valores de estas columnas se calculan automáticamente según la expresión materializada especificada al insertar filas. Los valores no se pueden especificar explícitamente durante las operaciones `INSERT`.

Además, las columnas con valor predeterminado de este tipo no se incluyen en el resultado de `SELECT *`. Esto preserva el invariante de que el resultado de un `SELECT *` siempre puede volver a insertarse en la tabla mediante `INSERT`. Este comportamiento puede desactivarse con la configuración `asterisk_include_materialized_columns`.

Ejemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

Columna efímera. Las columnas de este tipo no se almacenan en la tabla y no es posible hacer `SELECT` sobre ellas. El único propósito de las columnas efímeras es servir para construir a partir de ellas expresiones de valor predeterminado para otras columnas.

En una inserción sin columnas especificadas explícitamente, se omitirán las columnas de este tipo. Esto preserva el invariante de que el resultado de un `SELECT *` siempre puede volver a insertarse en la tabla usando `INSERT`.

Ejemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

Columnas calculadas (sinónimo). Las columnas de este tipo no se almacenan en la tabla y no es posible hacer `INSERT` de valores en ellas.

Cuando las consultas `SELECT` hacen referencia explícita a columnas de este tipo, el valor se calcula en el momento de la consulta a partir de `expr`. De forma predeterminada, `SELECT *` excluye las columnas ALIAS. Este comportamiento puede desactivarse con la configuración `asterisk_include_alias_columns`.

Al usar la consulta `ALTER` para agregar columnas nuevas, no se escriben datos antiguos para estas columnas. En su lugar, al leer datos antiguos que no tienen valores para las columnas nuevas, las expresiones se calculan sobre la marcha de forma predeterminada. Sin embargo, si para ejecutar las expresiones se requieren otras columnas que no se indican en la consulta, esas columnas también se leerán, pero solo para los bloques de datos que las necesiten.

Si agrega una columna nueva a una tabla pero más adelante cambia su expresión predeterminada, los valores usados para los datos antiguos cambiarán (en los datos cuyos valores no se almacenaron en el disco). Tenga en cuenta que, durante las merges en segundo plano, los datos de las columnas que faltan en una de las partes que se están fusionando se escriben en la parte fusionada.

No es posible establecer valores predeterminados para elementos de estructuras de datos anidadas.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## Clave primaria
</div>

Puede definir una [clave primaria](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries) al crear una tabla. La clave primaria puede especificarse de dos maneras:

* Dentro de la lista de columnas

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* Fuera de la lista de columnas

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
No es posible combinar ambas formas en una sola consulta.
:::

<div id="constraints">
  ## Restricciones
</div>

Además de las descripciones de las columnas, también se pueden definir restricciones:

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` puede ser cualquier expresión booleana. Si se definen restricciones para la tabla, cada una de ellas se verificará para cada fila de la consulta `INSERT`. Si alguna restricción no se cumple, el servidor generará una excepción con el nombre de la restricción y la expresión de comprobación.

Añadir una gran cantidad de restricciones puede afectar negativamente al rendimiento de las consultas `INSERT` de gran tamaño.

Las restricciones existentes en todas las tablas pueden consultarse en la tabla [`system.constraints`](/es/operations/system-tables/constraints).

<div id="assume">
  ### ASSUME
</div>

La cláusula `ASSUME` se usa para definir una `CONSTRAINT` sobre una tabla que se asume como verdadera. Esta restricción puede ser utilizada posteriormente por el optimizador para mejorar el rendimiento de las consultas SQL.

Tomemos este ejemplo en el que `ASSUME CONSTRAINT` se usa al crear la tabla `users_a`:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

Aquí, `ASSUME CONSTRAINT` se usa para indicar que la función `length(name)` siempre es igual al valor de la columna `name_len`. Esto significa que, cada vez que se llama a `length(name)` en una consulta, ClickHouse puede reemplazarla por `name_len`, lo que debería ser más rápido porque evita llamar a la función `length()`.

Luego, al ejecutar la consulta `SELECT name FROM users_a WHERE length(name) < 5;`, ClickHouse puede optimizarla a `SELECT name FROM users_a WHERE name_len < 5`; gracias a `ASSUME CONSTRAINT`. Esto puede hacer que la consulta se ejecute más rápido porque evita calcular la longitud de `name` para cada fila.

`ASSUME CONSTRAINT` **no impone la restricción**, simplemente informa al optimizador de que la restricción se cumple. Si la restricción en realidad no se cumple, los resultados de las consultas pueden ser incorrectos. Por lo tanto, solo debe usar `ASSUME CONSTRAINT` si está seguro de que la restricción es verdadera.

<div id="ttl-expression">
  ## Expresión TTL
</div>

Define el tiempo de almacenamiento de los valores. Solo puede especificarse para tablas de la familia MergeTree. Para una descripción detallada, consulte [TTL para columnas y tablas](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

<div id="column_compression_codec">
  ## Códecs de compresión de columnas
</div>

De forma predeterminada, ClickHouse aplica compresión `lz4` en la versión autogestionada y `zstd` en ClickHouse Cloud.

En la familia de motores `MergeTree`, puede cambiar el método de compresión predeterminado en la sección [compression](/es/operations/server-configuration-parameters/settings#compression) de la configuración del servidor.

También puede definir el método de compresión para cada columna por separado en la consulta `CREATE TABLE`.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

El códec `Default` puede especificarse para hacer referencia a la compresión predeterminada, que en tiempo de ejecución puede depender de distintos ajustes (y de las propiedades de los datos).
Ejemplo: `value UInt64 CODEC(Default)` — es lo mismo que no especificar ningún códec.

También puede eliminar el CODEC actual de la columna y usar la compresión predeterminada de config.xml:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Los códecs se pueden combinar en una cadena; por ejemplo, `CODEC(Delta, Default)`.

:::tip
No puedes descomprimir archivos de la base de datos de ClickHouse con utilidades externas como `lz4`. En su lugar, usa la utilidad especial [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).
:::

La compresión es compatible con los siguientes motores de tabla:

* Familia [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md). Admite códecs de compresión de columnas y la selección del método de compresión predeterminado mediante la configuración de [compression](/es/operations/server-configuration-parameters/settings#compression).
* Familia [Log](../../../engines/table-engines/log-family/index.md). Usa el método de compresión `lz4` de forma predeterminada y admite códecs de compresión de columnas.
* [Set](../../../engines/table-engines/special/set.md). Solo admite la compresión predeterminada.
* [Join](../../../engines/table-engines/special/join.md). Solo admite la compresión predeterminada.

ClickHouse admite códecs de uso general y códecs especializados.

<div id="general-purpose-codecs">
  ### Códecs de uso general
</div>

<div id="none">
  #### NONE
</div>

`NONE` — Sin compresión.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — Algoritmo de [compresión de datos](https://github.com/lz4/lz4) sin pérdida que se utiliza de forma predeterminada. Aplica compresión rápida LZ4.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — algoritmo LZ4 HC (alta compresión) con nivel ajustable. Nivel predeterminado: 9. La configuración `level <= 0` aplica el nivel predeterminado. Niveles posibles: [1, 12]. Rango de niveles recomendado: [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [algoritmo de compresión ZSTD](https://en.wikipedia.org/wiki/Zstandard) con `level` configurable. Niveles posibles: [1, 22]. Nivel predeterminado: 1.

Los niveles altos de compresión son útiles en escenarios asimétricos, como comprimir una vez y descomprimir repetidamente. Cuanto mayor sea el nivel, mejor será la compresión y mayor el uso de CPU.

<div id="zstd_qat">
  #### Obsoleto: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### Obsoleto: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### Códecs especializados
</div>

Estos códecs están diseñados para hacer la compresión más eficaz aprovechando características específicas de los datos. Algunos de estos códecs no comprimen los datos por sí mismos, sino que los preprocesan para que una segunda etapa de compresión con un códec de propósito general pueda lograr una mayor tasa de compresión.

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — Método de compresión en el que los valores originales se sustituyen por la diferencia entre dos valores adyacentes, salvo el primero, que permanece sin cambios. `delta_bytes` es el tamaño máximo de los valores originales; el valor predeterminado es `sizeof(type)`. Especificar `delta_bytes` como argumento está obsoleto y su compatibilidad se eliminará en una versión futura. Delta es un códec de preparación de datos; es decir, no puede usarse de forma independiente.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — Calcula delta de deltas y lo escribe en forma binaria compacta. `bytes_size` tiene un significado similar al de `delta_bytes` en el códec [Delta](#delta). Especificar `bytes_size` como argumento está en desuso y su compatibilidad se eliminará en una versión futura. Las tasas de compresión óptimas se consiguen con secuencias monótonas con un incremento constante, como los datos de series temporales. Puede usarse con cualquier tipo numérico. Implementa el algoritmo utilizado en Gorilla TSDB y lo amplía para admitir tipos de 64 bits. Usa 1 bit adicional para deltas de 32 bits: prefijos de 5 bits en lugar de prefijos de 4 bits. Para obtener más información, consulte Compressing Time Stamps en [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta es un códec de preparación de datos; es decir, no puede usarse de forma independiente.

<div id="gcd">
  #### GCD
</div>

`GCD()` - - Calcula el máximo común divisor (GCD) de los valores de la columna y luego divide cada valor por el GCD. Puede usarse con columnas enteras, decimales y de fecha/hora. Este códec es especialmente adecuado para columnas con valores que cambian (aumentan o disminuyen) en múltiplos del GCD, por ejemplo: 24, 28, 16, 24, 8, 24 (GCD = 4). GCD es un códec de preparación de datos; es decir, no puede usarse por sí solo.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — Calcula el XOR entre el valor actual y el valor de coma flotante anterior, y lo escribe en forma binaria compacta. Cuanto menor sea la diferencia entre valores consecutivos, es decir, cuanto más lentamente cambien los valores de la serie, mejor será la tasa de compresión. Implementa el algoritmo utilizado en Gorilla TSDB y lo amplía para admitir tipos de 64 bits. Los valores posibles de `bytes_size` son 1, 2, 4 y 8; el valor predeterminado es `sizeof(type)` si es igual a 1, 2, 4 u 8. En todos los demás casos, es 1. Para obtener más información, consulte la sección 4.1 de [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — Compresión adaptativa sin pérdida para datos de coma flotante. Compatible con `Float32` y `Float64`. Para obtener más información, consulte [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

El códec acepta un argumento opcional `variant`:

* `ALP()` o `ALP(AUTO)` (predeterminado) — Usa STD y recurre a RD en función del tamaño comprimido estimado.
* `ALP(STD)` — Variante estándar de ALP. Representa cada valor como un entero escalado exacto usando potencias de diez y, a continuación, comprime los enteros resultantes con Frame-of-Reference y empaquetado de bits. Los valores que no pueden representarse se almacenan como excepciones en bruto. Funciona mejor con números procedentes de decimales (por ejemplo, mediciones o precios).
* `ALP(RD)` — Variante Real Doubles. Reinterpreta el patrón de bits de cada valor y lo divide en una parte alta (signo + exponente + bits superiores de la mantisa) y una parte baja. Las partes altas se codifican mediante diccionario (hasta 8 entradas) y las partes bajas se empaquetan en bits. Funciona mejor cuando muchos valores comparten los mismos bits altos.

:::note
Este códec es experimental y requiere `SET allow_experimental_codecs = 1` para poder usarse.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - Predice repetidamente el siguiente valor de coma flotante de la secuencia usando el mejor de dos predictores; luego aplica XOR entre el valor real y el valor predicho, y comprime el resultado mediante supresión de ceros iniciales. Al igual que Gorilla, es eficiente para almacenar una serie de valores de coma flotante que cambian lentamente. Para valores de 64 bits (double), FPC es más rápido que Gorilla; para valores de 32 bits, el rendimiento puede variar. Valores posibles de `level`: 1-28; el valor predeterminado es 12. Valores posibles de `float_size`: 4, 8; el valor predeterminado es `sizeof(type)` si el tipo es Float. En todos los demás casos, es 4. Para una descripción detallada del Algorithm, consulte [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

<div id="t64">
  #### T64
</div>

`T64` — Método de compresión que recorta los bits altos no utilizados de los valores en tipos de datos enteros (incluidos `Enum`, `Date` y `DateTime`). En cada paso de su algoritmo, el códec toma un bloque de 64 valores, los coloca en una matriz de bits de 64x64, la transpone, recorta los bits no utilizados de los valores y devuelve el resto en forma de secuencia. Los bits no utilizados son aquellos que no varían entre los valores máximo y mínimo en toda la parte de datos para la que se usa la compresión.

Los códecs `DoubleDelta` y `Gorilla` se usan en Gorilla TSDB como componentes de su algoritmo de compresión. El enfoque de Gorilla es eficaz en escenarios en los que hay una secuencia de valores que cambian lentamente junto con sus marcas de tiempo. Las marcas de tiempo se comprimen eficazmente con el códec `DoubleDelta`, y los valores se comprimen eficazmente con el códec `Gorilla`. Por ejemplo, para obtener una tabla almacenada de forma eficiente, puede crearla con la siguiente configuración:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### Códecs de cifrado
</div>

Estos códecs en realidad no comprimen los datos, sino que los cifran en disco. Solo están disponibles cuando se especifica una clave de cifrado mediante la configuración de [encryption](/es/operations/server-configuration-parameters/settings#encryption). Tenga en cuenta que el cifrado solo tiene sentido al final de las canalizaciones de códecs, porque los datos cifrados normalmente no pueden comprimirse de forma útil.

Códecs de cifrado:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — Cifra los datos con AES-128 en el modo GCM-SIV definido en [RFC 8452](https://tools.ietf.org/html/rfc8452).

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — Cifra los datos con AES-256 en modo GCM-SIV.

Estos códec usan un nonce fijo y, por lo tanto, el cifrado es determinista. Esto los hace compatibles con motores con deduplicación como [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md), pero tienen una debilidad: cuando el mismo bloque de datos se cifra dos veces, el texto cifrado resultante será exactamente el mismo, por lo que un adversario que pueda leer el disco podrá detectar esta equivalencia (aunque solo la equivalencia, sin obtener su contenido).

:::note
La mayoría de los motores, incluida la familia &quot;*MergeTree&quot;, crean archivos de índice en disco sin aplicar códec. Esto significa que aparecerá texto sin cifrar en el disco si se indexa una columna cifrada.
:::

:::note
Si realiza una consulta SELECT que menciona un valor específico en una columna cifrada (por ejemplo, en su cláusula WHERE), el valor puede aparecer en [system.query&#95;log](../../../operations/system-tables/query_log.md). Puede que desee desactivar el registro.
:::

**Ejemplo**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
Si es necesario aplicar compresión, debe especificarse explícitamente. De lo contrario, los datos solo se cifrarán.
:::

**Ejemplo**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## Tablas temporales
</div>

:::note
Ten en cuenta que las tablas temporales no se replican. Como resultado, no hay garantía de que los datos insertados en una tabla temporal estén disponibles en otras réplicas. El principal caso de uso en el que las tablas temporales pueden resultar útiles es para consultar o hacer join con pequeños conjuntos de datos externos durante una sola sesión.
:::

ClickHouse admite tablas temporales, que tienen las siguientes características:

* Las tablas temporales desaparecen cuando termina la sesión, incluso si se pierde la conexión.
* Una tabla temporal usa el motor de tabla Memory cuando no se especifica ningún motor, y puede usar cualquier motor de tabla excepto los motores Replicated y `KeeperMap`.
* No se puede especificar la DB para una tabla temporal. Se crea fuera de las bases de datos.
* Es imposible crear una tabla temporal con una consulta DDL distribuida en todos los servidores del clúster (usando `ON CLUSTER`): esta tabla existe solo en la sesión actual.
* Si una tabla temporal tiene el mismo nombre que otra y una consulta especifica el nombre de la tabla sin especificar la DB, se usará la tabla temporal.
* Para el procesamiento distribuido de consultas, las tablas temporales con motor Memory que se usan en una consulta se envían a los servidores remotos.

Para crear una tabla temporal, usa la siguiente sintaxis:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

En la mayoría de los casos, las tablas temporales no se crean manualmente, sino cuando se usan datos externos en una consulta o para un `(GLOBAL) IN` distribuido. Para más información, consulte las secciones correspondientes

Es posible usar tablas con [ENGINE = Memory](../../../engines/table-engines/special/memory.md) en lugar de tablas temporales.

<div id="replace-table">
  ## REPLACE TABLE
</div>

La instrucción `REPLACE` le permite actualizar una tabla [de forma atómica](/es/concepts/glossary#atomicity).

:::note
Esta instrucción es compatible con los motores de base de datos [`Atomic`](../../../engines/database-engines/atomic.md) y [`Replicated`](../../../engines/database-engines/replicated.md),
que son los motores de base de datos predeterminados de ClickHouse y ClickHouse Cloud, respectivamente.
:::

Normalmente, si necesita eliminar algunos datos de una tabla,
puede crear una tabla nueva y llenarla con una instrucción `SELECT` que omita los datos no deseados;
después, puede eliminar la tabla anterior y cambiar el nombre de la nueva.
Este enfoque se muestra en el ejemplo siguiente:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

En lugar del enfoque anterior, también puedes usar `REPLACE` (si usas los motores de base de datos predeterminados) para lograr el mismo resultado:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### Sintaxis
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
Todas las formas de sintaxis de la sentencia `CREATE` también sirven para esta sentencia. Invocar `REPLACE` sobre una tabla que no existe provocará un error.
:::

<div id="examples">
  ### Ejemplos:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="Local" default>
    Considere la siguiente tabla:

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    Podemos usar la sentencia `REPLACE` para eliminar todos los datos:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    O bien podemos usar la sentencia `REPLACE` para cambiar la estructura de la tabla:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    Considere la siguiente tabla en ClickHouse Cloud:

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    Podemos usar la sentencia `REPLACE` para eliminar todos los datos:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    O bien podemos usar la sentencia `REPLACE` para cambiar la estructura de la tabla:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## Cláusula COMMENT
</div>

Puede añadir un comentario a la tabla al crearla.

**Sintaxis**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
La cláusula `COMMENT` debe especificarse **después** de cualquier cláusula específica del almacenamiento, como `PARTITION BY`, `ORDER BY` y `SETTINGS` específicos del almacenamiento.

Después de la cláusula `COMMENT`, solo se procesarán los `SETTINGS` específicos de la consulta (como `max_threads`, etc.), no la configuración relacionada con el almacenamiento.

Esto significa que el orden correcto de las cláusulas es:

* `ENGINE`
* cláusulas de almacenamiento
* `COMMENT`
* configuración de la consulta (si la hay)
  :::

**Ejemplo**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Optimizar ClickHouse con esquemas y códecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog: [Trabajar con datos de series temporales en ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)