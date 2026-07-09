---
description: 'Documentación de la sintaxis'
sidebar_label: 'Sintaxis'
sidebar_position: 2
slug: /sql-reference/syntax
title: 'Sintaxis'
doc_type: 'reference'
---

En esta sección, examinaremos la sintaxis SQL de ClickHouse.
ClickHouse utiliza una sintaxis basada en SQL, pero ofrece diversas extensiones y optimizaciones.

<div id="query-parsing">
  ## Análisis sintáctico de consultas
</div>

Hay dos tipos de analizadores en ClickHouse:

* *Un analizador de SQL completo* (un analizador descendente recursivo).
* *Un analizador de formato de datos* (un analizador rápido de flujo).

El analizador de SQL completo se utiliza en todos los casos, excepto en la consulta `INSERT`, que usa ambos analizadores.

Veamos la consulta siguiente:

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

Como ya se mencionó, la consulta `INSERT` utiliza ambos analizadores.
El fragmento `INSERT INTO t VALUES` se analiza con el analizador completo,
y los datos `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` se analizan con el analizador de formato de datos, o analizador rápido de flujo.

<details>
  <summary>Activación del analizador completo</summary>

  También se puede activar el analizador completo para los datos
  mediante la configuración [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions).

  Cuando la configuración mencionada anteriormente se establece en `1`,
  ClickHouse primero intenta analizar los valores con el analizador rápido de flujo.
  Si falla, ClickHouse intenta usar el analizador completo para los datos, tratándolos como una [expresión](#expressions) SQL.
</details>

Los datos pueden tener cualquier formato.
Cuando se recibe una consulta, el servidor mantiene en RAM como máximo [max&#95;query&#95;size](../operations/settings/settings.md#max_query_size) bytes de la solicitud
(de forma predeterminada, 1 MB), y el resto se analiza en flujo.
Esto permite evitar problemas con consultas `INSERT` grandes, que es la forma recomendada de insertar datos en ClickHouse.

Al usar el formato [`Values`](/es/interfaces/formats/Values) en una consulta `INSERT`,
puede parecer que los datos se analizan igual que las expresiones de una consulta `SELECT`, pero no es así.
El formato `Values` es mucho más limitado.

El resto de esta sección trata sobre el analizador completo.

:::note
Para obtener más información sobre los analizadores de formatos, consulta la sección [Formats](../interfaces/formats.md).
:::

<div id="spaces">
  ## Espacios
</div>

* Puede haber cualquier cantidad de caracteres de espacio en blanco entre construcciones sintácticas (incluidos el inicio y el final de una consulta).
* Los caracteres de espacio en blanco incluyen el espacio, la tabulación, el salto de línea, CR y el salto de página.

<div id="comments">
  ## Comentarios
</div>

ClickHouse admite comentarios de estilo SQL y de estilo C:

* Los comentarios de estilo SQL comienzan con `--`, `#!` o `# ` y continúan hasta el final de la línea. El espacio después de `--` y `#!` puede omitirse.
* Comentarios de estilo C:
  * `//` (o más de 2 caracteres `/`) seguidos de texto hasta el final de la línea. No es necesario dejar espacios después de `/`.
  * Pueden extenderse desde `/*` hasta `*/` en comentarios multilínea. Tampoco es necesario dejar espacios.
  * Los comentarios de estilo C pueden anidarse.

Por ejemplo:

```sql
/*
 * Compute the number of days between two dates.
 * /* Returns NULL if either argument is NULL */
 */
SELECT
    dateDiff('day', toDate('2024-01-01'), toDate('2024-12-31')) AS days_in_year, -- 365
    dateDiff('day', toDate('2020-01-01'), today()) AS days_since  #! since 2020
    ///////////////////////////////////////////////////////////////////
    # TODO: add hour/minute variants
```

<div id="keywords">
  ## Palabras clave
</div>

Las palabras clave de ClickHouse pueden ser *sensibles a mayúsculas y minúsculas* o *no sensibles a mayúsculas y minúsculas*, según el contexto.

Las palabras clave **no sensibles a mayúsculas y minúsculas** son las que corresponden a:

* El estándar SQL. Por ejemplo, `SELECT`, `select` y `SeLeCt` son válidos.
* La implementación de algunos SGBD populares (MySQL o Postgres). Por ejemplo, `DateTime` es lo mismo que `datetime`.

:::note
Puede comprobar si un nombre de tipo de dato es sensible a mayúsculas y minúsculas en la tabla [system.data&#95;type&#95;families](/es/operations/system-tables/data_type_families).
:::

A diferencia del SQL estándar, todas las demás palabras clave (incluidos los nombres de las funciones) son **sensibles a mayúsculas y minúsculas**.

Además, las palabras clave no están reservadas.
Solo se tratan como tales en el contexto correspondiente.
Si usa [identificadores](#identifiers) con el mismo nombre que las palabras clave, enciérrelos entre comillas dobles o acentos graves.

Por ejemplo, la siguiente consulta es válida si la tabla `table_name` tiene una columna con el nombre `"FROM"`:

```sql
SELECT "FROM" FROM table_name
```

<div id="identifiers">
  ## Identificadores
</div>

Los identificadores son:

* Nombres de clúster, base de datos, tabla, partición y columna.
* [Funciones](#functions).
* [Tipos de datos](../sql-reference/data-types/index.md).
* [Alias de expresiones](#expression-aliases).

Los identificadores pueden ir entre comillas o sin comillas, aunque se prefieren estos últimos.

Los identificadores sin comillas deben coincidir con la regex `^[a-zA-Z_][0-9a-zA-Z_]*$` y no pueden ser iguales a las [palabras clave](#keywords).
Consulte la siguiente tabla para ver ejemplos de identificadores válidos y no válidos:

| Identificadores válidos                        | Identificadores no válidos             |
| ---------------------------------------------- | -------------------------------------- |
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

Si desea usar identificadores iguales a las palabras clave, o si quiere usar otros símbolos en los identificadores, póngalos entre comillas dobles o acentos graves, por ejemplo, `"id"`, `` `id` ``.

:::note
Las mismas reglas que se aplican al escaping en los identificadores entre comillas también se aplican a los literales de cadena. Consulte [String](#string) para más detalles.
:::

:::tip[Evite usar puntos en los nombres de columna]
Los nombres de columna que contienen puntos, las columnas que comparten un prefijo común separado por puntos y las columnas con el tipo `Array` pueden interpretarse como parte de una estructura `Nested` aplanada cuando `flatten_nested = 1` (el valor predeterminado). Esto puede provocar una validación inesperada de la longitud de los arrays en las inserciones y restricciones al cambiarles el nombre.

Evite usar puntos en los nombres de columna siempre que sea posible.
Use guiones bajos (`_`) u otro separador en lugar de puntos en los nombres de columna, a menos que necesite intencionadamente la semántica de `Nested`.
:::

<div id="literals">
  ## Literales
</div>

En ClickHouse, un literal es un valor representado directamente en una consulta.
En otras palabras, es un valor fijo que no cambia durante la ejecución de la consulta.

Los literales pueden ser:

* [String](#string)
* [Numéricos](#numeric)
* [Compuestos](#compound)
* [`NULL`](#null)
* [Heredocs](#heredoc) (literales de cadena personalizados)

A continuación, veremos cada uno de ellos con más detalle en las secciones siguientes.

<div id="string">
  ### String
</div>

Los literales de cadena deben ir entre comillas simples. No se admiten comillas dobles.

El escape funciona de una de estas dos formas:

* usando una comilla simple delante, donde el carácter de comilla simple `'` (y solo este carácter) puede escaparse como `''`, o
* usando una barra invertida delante con las siguientes secuencias de escape admitidas, que se enumeran en la tabla de abajo.

:::note
La barra invertida pierde su significado especial; es decir, se interpreta literalmente si precede a caracteres distintos de los que se enumeran a continuación.
:::

| Escape compatible                         | Descripción                                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `\xHH`                                    | Especificación de un carácter de 8 bits seguida de cualquier número de dígitos hexadecimales (H). |
| `\N`                                      | reservado, no hace nada (p. ej., `SELECT 'a\Nb'` devuelve `ab`)                                   |
| `\a`                                      | alerta                                                                                            |
| `\b`                                      | retroceso                                                                                         |
| `\e`                                      | carácter de escape                                                                                |
| `\f`                                      | salto de página                                                                                   |
| `\n`                                      | salto de línea                                                                                    |
| `\r`                                      | retorno de carro                                                                                  |
| `\t`                                      | tabulación horizontal                                                                             |
| `\v`                                      | tabulación vertical                                                                               |
| `\0`                                      | carácter nulo                                                                                     |
| `\\`                                      | barra invertida                                                                                   |
| `\'` (o `''`)                             | comilla simple                                                                                    |
| `\"`                                      | comilla doble                                                                                     |
| `` ` ``                                   | acento grave                                                                                      |
| `\/`                                      | barra inclinada                                                                                   |
| `\=`                                      | signo igual                                                                                       |
| Caracteres de control ASCII (c &lt;= 31). |                                                                                                   |

:::note
En los literales de cadena, debes escapar al menos `'` y `\` usando los códigos de escape `\'` (o: `''`) y `\\`.
:::

<div id="numeric">
  ### Numérico
</div>

Los literales numéricos se analizan de la siguiente manera:

* Si el literal lleva el prefijo de signo menos `-`, el token se omite y el resultado se niega después del análisis.
* El literal numérico primero se analiza como un entero sin signo de 64 bits mediante la función [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul).
  * Si el valor tiene el prefijo `0b` o `0x`/`0X`, el número se analiza como binario o hexadecimal, respectivamente.
  * Si el valor es negativo y su magnitud absoluta es mayor que 2<sup>63</sup>, se devuelve un error.
* Si no se puede, el valor se analiza a continuación como un número de coma flotante mediante la función [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof).
* En caso contrario, se devuelve un error.

Los valores literales se convierten al tipo más pequeño en el que quepan.
Por ejemplo:

* `1` se analiza como `UInt8`
* `256` se analiza como `UInt16`.

:::note Importante
Los valores enteros de más de 64 bits (`UInt128`, `Int128`, `UInt256`, `Int256`) deben convertirse a un tipo mayor para poder analizarse correctamente:

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

Esto omite el algoritmo anterior e interpreta el entero con una rutina que admite precisión arbitraria.

De lo contrario, el literal se interpretará como un número de coma flotante y, por lo tanto, puede perder precisión debido al truncamiento.
:::

Para obtener más información, consulta [Tipos de datos](../sql-reference/data-types/index.md).

Los guiones bajos `_` dentro de los literales numéricos se ignoran y pueden usarse para mejorar la legibilidad.

Se admiten los siguientes literales numéricos:

| Literal numérico                                      | Ejemplos                                        |
| ----------------------------------------------------- | ----------------------------------------------- |
| **Enteros**                                           | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **Decimales**                                         | `0.1`                                           |
| **Notación exponencial**                              | `1e100`, `-1e-100`                              |
| **Números de coma flotante**                          | `123.456`, `inf`, `nan`                         |
| **Hexadecimal**                                       | `0xc0fe`                                        |
| **Cadena hexadecimal compatible con el estándar SQL** | `x'c0fe'`                                       |
| **Binario**                                           | `0b1101`                                        |
| **Cadena binaria compatible con el estándar SQL**     | `b'1101'`                                       |

:::note
No se admiten literales octales para evitar errores accidentales de interpretación.
:::

<div id="compound">
  ### Compuestos
</div>

Los arrays se construyen con `[]`: `[1, 2, 3]`. Las tuplas se construyen con `()`: `(1, 'Hello, world!', 2)`.
Técnicamente, no se trata de literales, sino de expresiones con el operador de creación de arrays y el operador de creación de tuplas, respectivamente.
Un array debe constar de al menos un elemento, y una tupla debe tener al menos dos elementos.

:::note
Hay un caso aparte en el que las tuplas aparecen en la cláusula `IN` de una consulta `SELECT`.
Los resultados de una consulta pueden incluir tuplas, pero las tuplas no se pueden guardar en una base de datos (excepto en tablas que usan el motor [Memory](../engines/table-engines/special/memory.md)).
:::

<div id="null">
  ### NULL
</div>

`NULL` se usa para indicar que falta un valor.
Para almacenar `NULL` en un campo de una tabla, este debe ser del tipo [Nullable](../sql-reference/data-types/nullable.md).

:::note
Debe tenerse en cuenta lo siguiente sobre `NULL`:

* Según el formato de datos (de entrada o de salida), `NULL` puede tener una representación distinta. Para más información, consulte [formatos de datos](/es/interfaces/formats).
* El tratamiento de `NULL` tiene ciertos matices. Por ejemplo, si al menos uno de los argumentos de una operación de comparación es `NULL`, el resultado de esa operación también es `NULL`. Lo mismo ocurre con la multiplicación, la suma y otras operaciones. Recomendamos leer la documentación de cada operación.
* En las consultas, puede comprobar si un valor es `NULL` mediante los operadores [`IS NULL`](/es/sql-reference/functions/functions-for-nulls#isNull) y [`IS NOT NULL`](/es/sql-reference/functions/functions-for-nulls#isNotNull), así como las funciones relacionadas `isNull` e `isNotNull`.
  :::

<div id="heredoc">
  ### Heredoc
</div>

Un [heredoc](https://en.wikipedia.org/wiki/Here_document) es una forma de definir una cadena (a menudo multilínea) manteniendo el formato original.
Un heredoc se define como un literal de cadena personalizado, colocado entre dos símbolos `$`.

Por ejemplo:

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note

* Un valor entre dos heredocs se procesa &quot;tal cual&quot;.
  :::

:::tip

* Puedes usar un heredoc para incluir fragmentos de código SQL, HTML o XML, etc.
  :::

<div id="defining-and-using-query-parameters">
  ## Definición y uso de parámetros de consulta
</div>

Los parámetros de consulta le permiten escribir consultas genéricas que contienen marcadores de posición abstractos en lugar de identificadores concretos.
Cuando se ejecuta una consulta con parámetros de consulta,
todos los marcadores de posición se resuelven y se sustituyen por los valores reales de los parámetros.

Los parámetros de consulta pueden definirse de varias maneras:

* `SET param_<name>=<value>` — mediante un comando `SET` en una consulta.
* `--param_<name>='<value>'` — como argumento de `clickhouse-client` en la línea de comandos.
* `param_<name>=<value>` — como parámetro de la query string de la URL para la interfaz HTTP.

Se puede hacer referencia a un parámetro de consulta dentro de una consulta mediante `{<name>: <datatype>}`, donde `<name>` es el nombre del parámetro de consulta y `<datatype>` es el tipo de dato al que se convierte.

<details>
  <summary>Ejemplo con el comando SET</summary>

  Por ejemplo, el siguiente SQL define parámetros llamados `a`, `b`, `c` y `d`, cada uno con un tipo de dato distinto:

  ```sql
  SET param_a = 13;
  SET param_b = 'str';
  SET param_c = '2022-08-04 18:30:53';
  SET param_d = {'10': [11, 12], '13': [14, 15]};

  SELECT
     {a: UInt32},
     {b: String},
     {c: DateTime},
     {d: Map(String, Array(UInt8))};

  13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
  ```
</details>

<details>
  <summary>Ejemplo con clickhouse-client</summary>

  Si está usando `clickhouse-client`, los parámetros se especifican como `--param_name=value`. Por ejemplo, el siguiente parámetro se llama `message` y se recupera como `String`:

  ```bash
  clickhouse-client --param_message='hello' --query="SELECT {message: String}"

  hello
  ```

  Si el parámetro de consulta representa el nombre de una base de datos, tabla, función u otro identificador, use `Identifier` como tipo. Por ejemplo, la siguiente consulta devuelve filas de una tabla llamada `uk_price_paid`:

  ```sql
  SET param_mytablename = "uk_price_paid";
  SELECT * FROM {mytablename:Identifier};
  ```
</details>

<details>
  <summary>Ejemplo con la interfaz HTTP</summary>

  Los parámetros de consulta pueden pasarse como parámetros de la query string de la URL con el prefijo `param_`. Por ejemplo:

  ```bash
  curl -s "http://localhost:8123/?param_message=hello" --data-binary "SELECT {message: String}"

  hello
  ```
</details>

<details>
  <summary>Ejemplo con la interfaz web</summary>

  La interfaz web integrada (`play.html`) detecta automáticamente los marcadores de posición de parámetros `{name:Type}` en la consulta y muestra campos de entrada etiquetados para cada parámetro. Los valores de los parámetros se incluyen en la solicitud HTTP y también se conservan en la URL de la página para poder guardarla en marcadores y compartirla.
</details>

:::note
Los parámetros de consulta no son sustituciones de texto generales que puedan usarse en lugares arbitrarios de consultas SQL arbitrarias.
Están diseñados principalmente para funcionar en sentencias `SELECT` en lugar de identificadores o literales.
:::

<div id="functions">
  ## Funciones
</div>

Las llamadas a funciones se escriben como un identificador seguido de una lista de argumentos (posiblemente vacía) entre `()`.
A diferencia del SQL estándar, los paréntesis son obligatorios, incluso cuando la lista de argumentos está vacía.
Por ejemplo:

```sql
now()
```

También existen:

* [Funciones regulares](/es/sql-reference/functions/overview).
* [Funciones de agregación](/es/sql-reference/aggregate-functions).

Algunas funciones de agregación pueden incluir dos listas de argumentos entre paréntesis. Por ejemplo:

```sql
quantile (0.9)(x) 
```

Estas funciones de agregación se denominan funciones &quot;paramétricas&quot;,
y los argumentos de la primera lista se denominan &quot;parámetros&quot;.

:::note
La sintaxis de las funciones de agregación sin parámetros es la misma que la de las funciones normales.
:::

<div id="operators">
  ## Operadores
</div>

Los operadores se transforman en las funciones correspondientes al analizar la consulta, teniendo en cuenta su prioridad y asociatividad.

Por ejemplo, la expresión

```text
1 + 2 * 3 + 4
```

se transforma en

```text
plus(plus(1, multiply(2, 3)), 4)`
```

<div id="data-types-and-database-table-engines">
  ## Tipos de datos y motores de tabla de la base de datos
</div>

Los tipos de datos y los motores de tabla en la consulta `CREATE` se escriben igual que los identificadores o las funciones.
En otras palabras, pueden incluir o no una lista de argumentos entre paréntesis.

Para obtener más información, consulte las secciones:

* [Tipos de datos](/es/sql-reference/data-types/index.md)
* [Motores de tabla](/es/engines/table-engines/index.md)
* [CREATE](/es/sql-reference/statements/create/index.md).

<div id="expressions">
  ## Expresiones
</div>

Una expresión puede ser cualquiera de las siguientes:

* una función
* un identificador
* un literal
* la aplicación de un operador
* una expresión entre paréntesis
* una subconsulta
* un asterisco

También puede contener un [alias](#expression-aliases).

Una lista de expresiones consta de una o más expresiones separadas por comas.
Las funciones y los operadores, a su vez, pueden tener expresiones como argumentos.

Una expresión constante es una expresión cuyo resultado se conoce durante el análisis de la consulta, es decir, antes de la ejecución.
Por ejemplo, las expresiones formadas por literales son expresiones constantes.

<div id="expression-aliases">
  ## Alias de expresiones
</div>

Un alias es un nombre definido por el usuario para una [expresión](#expressions) en una consulta.

```sql
expr AS alias
```

Las partes de la sintaxis anterior se explican a continuación.

| Parte de la sintaxis | Descripción                                                                                                                                                 | Ejemplo                                                                 | Notas                                                                                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AS`                 | La palabra clave para definir alias. Puede definir el alias de un nombre de tabla o de una columna en una cláusula `SELECT` sin usar la palabra clave `AS`. | `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | En la función [CAST](/es/sql-reference/functions/type-conversion-functions#CAST), la palabra clave `AS` tiene otro significado. Consulte la descripción de la función. |
| `expr`               | Cualquier expresión admitida por ClickHouse.                                                                                                                | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                                                                                     |
| `alias`              | Nombre de `expr`. Los alias deben ajustarse a la sintaxis de los [identificadores](#identifiers).                                                           | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                                                                                     |

<div id="notes-on-usage">
  ### Notas sobre el uso
</div>

* Los alias son globales para una consulta o subconsulta, y se puede definir un alias en cualquier parte de una consulta para cualquier expresión. Por ejemplo:

```sql
SELECT (1 AS n) + 2, n`.
```

* Los alias no son visibles en las subconsultas ni entre ellas. Por ejemplo, al ejecutar la siguiente consulta, ClickHouse genera la excepción `Unknown identifier: num`:

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

* Si se define un alias para las columnas de resultados en la cláusula `SELECT` de una subconsulta, estas columnas serán visibles en la consulta externa. Por ejemplo:

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

* Tenga cuidado con los alias que coincidan con nombres de columnas o tablas. Consideremos el siguiente ejemplo:

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

En el ejemplo anterior, declaramos la tabla `t` con la columna `b`.
Luego, al seleccionar datos, definimos el alias `sum(b) AS b`.
Como los alias son globales,
ClickHouse sustituyó el literal `b` en la expresión `argMax(a, b)` por la expresión `sum(b)`.
Esta sustitución provocó la excepción.

:::note
Puede cambiar este comportamiento predeterminado estableciendo [prefer&#95;column&#95;name&#95;to&#95;alias](/es/operations/settings/settings#prefer_column_name_to_alias) en `1`.
:::

<div id="asterisk">
  ## Asterisco
</div>

En una consulta `SELECT`, un asterisco puede sustituir a la expresión.
Para obtener más información, consulte la sección [SELECT](/es/sql-reference/statements/select/index.md#asterisk).