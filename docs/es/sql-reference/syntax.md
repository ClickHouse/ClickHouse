---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: Sintaxis
---

# Sintaxis {#syntax}

Hay dos tipos de analizadores en el sistema: el analizador SQL completo (un analizador de descenso recursivo) y el analizador de formato de datos (un analizador de flujo rápido).
En todos los casos, excepto el `INSERT` consulta, sólo se utiliza el analizador SQL completo.
El `INSERT` consulta utiliza ambos analizadores:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

El `INSERT INTO t VALUES` fragmento es analizado por el analizador completo, y los datos `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` es analizado por el analizador de flujo rápido. También puede activar el analizador completo de los datos mediante el [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) configuración. Cuando `input_format_values_interpret_expressions = 1`, ClickHouse primero intenta analizar valores con el analizador de flujo rápido. Si falla, ClickHouse intenta usar el analizador completo para los datos, tratándolo como un SQL [expresion](#syntax-expressions).

Los datos pueden tener cualquier formato. Cuando se recibe una consulta, el servidor no calcula más de [max_query_size](../operations/settings/settings.md#settings-max_query_size) bytes de la solicitud en RAM (por defecto, 1 MB), y el resto se analiza la secuencia.
Permite evitar problemas con grandes `INSERT` consulta.

Cuando se utiliza el `Values` formato en un `INSERT` consulta, puede parecer que los datos se analizan igual que las expresiones en un `SELECT` consulta, pero esto no es cierto. El `Values` formato es mucho más limitado.

El resto de este artículo cubre el analizador completo. Para obtener más información sobre los analizadores de formato, consulte [Formato](../interfaces/formats.md) apartado.

## Espacio {#spaces}

Puede haber cualquier número de símbolos de espacio entre las construcciones sintácticas (incluidos el principio y el final de una consulta). Los símbolos de espacio incluyen el espacio, tabulación, avance de línea, CR y avance de formulario.

## Comentario {#comments}

ClickHouse admite comentarios de estilo SQL y de estilo C.
Los comentarios de estilo SQL comienzan con `--` y continuar hasta el final de la línea, un espacio después `--` se puede omitir.
C-estilo son de `/*` a `*/`y puede ser multilínea, tampoco se requieren espacios.

## Palabras clave {#syntax-keywords}

Las palabras clave no distinguen entre mayúsculas y minúsculas cuando corresponden a:

-   Estándar SQL. Por ejemplo, `SELECT`, `select` y `SeLeCt` son todos válidos.
-   Implementación en algunos DBMS populares (MySQL o Postgres). Por ejemplo, `DateTime` es lo mismo que `datetime`.

Si el nombre del tipo de datos distingue entre mayúsculas y minúsculas `system.data_type_families` tabla.

A diferencia de SQL estándar, todas las demás palabras clave (incluidos los nombres de funciones) son **minúsculas**.

Las palabras clave no están reservadas; se tratan como tales solo en el contexto correspondiente. Si usted usa [identificador](#syntax-identifiers) con el mismo nombre que las palabras clave, encerrarlas en comillas dobles o backticks. Por ejemplo, la consulta `SELECT "FROM" FROM table_name` es válido si la tabla `table_name` tiene columna con el nombre `"FROM"`.

## Identificador {#syntax-identifiers}

Los identificadores son:

-   Nombres de clúster, base de datos, tabla, partición y columna.
-   Función.
-   Tipos de datos.
-   [Alias de expresión](#syntax-expression_aliases).

Los identificadores pueden ser citados o no citados. Este último es preferido.

Los identificadores no citados deben coincidir con la expresión regular `^[a-zA-Z_][0-9a-zA-Z_]*$` y no puede ser igual a [Palabras clave](#syntax-keywords). Ejemplos: `x, _1, X_y__Z123_.`

Si desea utilizar identificadores iguales a las palabras clave o si desea utilizar otros símbolos en los identificadores, cítelo con comillas dobles o retrocesos, por ejemplo, `"id"`, `` `id` ``.

## Literal {#literals}

Hay numérico, cadena, compuesto, y `NULL` literal.

### Numérico {#numeric}

Literal numérico, intenta ser analizado:

-   Primero, como un número firmado de 64 bits, usando el [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) función.
-   Si no tiene éxito, como un número de 64 bits sin signo, [Sistema abierto.](https://en.cppreference.com/w/cpp/string/byte/strtol) función.
-   Si no tiene éxito, como un número de punto flotante [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) función.
-   De lo contrario, devuelve un error.

El valor literal tiene el tipo más pequeño en el que se ajusta el valor.
Por ejemplo, 1 se analiza como `UInt8` pero 256 se analiza como `UInt16`. Para obtener más información, consulte [Tipos de datos](../sql-reference/data-types/index.md).

Ejemplos: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### Cadena {#syntax-string-literal}

Solo se admiten literales de cadena entre comillas simples. Los caracteres incluidos se pueden escapar de barra invertida. Las siguientes secuencias de escape tienen un valor especial correspondiente: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. En todos los demás casos, secuencias de escape en el formato `\c`, donde `c` cualquier carácter, se convierten a `c`. Significa que puedes usar las secuencias `\'`y`\\`. El valor tendrá el [Cadena](../sql-reference/data-types/string.md) tipo.

En literales de cadena, necesitas escapar al menos `'` y `\`. Las comillas simples se pueden escapar con las comillas simples, literales `'It\'s'` y `'It''s'` son iguales.

### Compuesto {#compound}

Las matrices se construyen con corchetes `[1, 2, 3]`. Nuples están construidos con paréntesis `(1, 'Hello, world!', 2)`.
Técnicamente, estos no son literales, sino expresiones con el operador de creación de matriz y el operador de creación de tuplas, respectivamente.
Una matriz debe constar de al menos un elemento y una tupla debe tener al menos dos elementos.
Hay un caso separado cuando aparecen tuplas en el `IN` cláusula de un `SELECT` consulta. Los resultados de la consulta pueden incluir tuplas, pero las tuplas no se pueden guardar en una base de datos (excepto las tablas con [Memoria](../engines/table-engines/special/memory.md) motor).

### NULL {#null-literal}

Indica que falta el valor.

Para almacenar `NULL` en un campo de tabla, debe ser del [NULL](../sql-reference/data-types/nullable.md) tipo.

Dependiendo del formato de datos (entrada o salida), `NULL` puede tener una representación diferente. Para obtener más información, consulte la documentación de [Formatos de datos](../interfaces/formats.md#formats).

Hay muchos matices para el procesamiento `NULL`. Por ejemplo, si al menos uno de los argumentos de una operación de comparación es `NULL`, el resultado de esta operación también es `NULL`. Lo mismo es cierto para la multiplicación, la suma y otras operaciones. Para obtener más información, lea la documentación de cada operación.

En las consultas, puede verificar `NULL` utilizando el [IS NULL](operators/index.md#operator-is-null) y [IS NOT NULL](operators/index.md) operadores y las funciones relacionadas `isNull` y `isNotNull`.

## Función {#functions}

Las llamadas a funciones se escriben como un identificador con una lista de argumentos (posiblemente vacíos) entre corchetes redondos. A diferencia de SQL estándar, los corchetes son necesarios, incluso para una lista de argumentos vacía. Ejemplo: `now()`.
Hay funciones regulares y agregadas (ver la sección “Aggregate functions”). Algunas funciones agregadas pueden contener dos listas de argumentos entre paréntesis. Ejemplo: `quantile (0.9) (x)`. Estas funciones agregadas se llaman “parametric” funciones, y los argumentos en la primera lista se llaman “parameters”. La sintaxis de las funciones agregadas sin parámetros es la misma que para las funciones regulares.

## Operador {#operators}

Los operadores se convierten a sus funciones correspondientes durante el análisis de consultas, teniendo en cuenta su prioridad y asociatividad.
Por ejemplo, la expresión `1 + 2 * 3 + 4` se transforma a `plus(plus(1, multiply(2, 3)), 4)`.

## Tipos de datos y motores de tabla de base de datos {#data_types-and-database-table-engines}

Tipos de datos y motores de tablas en el `CREATE` las consultas se escriben de la misma manera que los identificadores o funciones. En otras palabras, pueden o no contener una lista de argumentos entre corchetes. Para obtener más información, consulte las secciones “Data types,” “Table engines,” y “CREATE”.

## Alias de expresión {#syntax-expression_aliases}

Un alias es un nombre definido por el usuario para la expresión en una consulta.

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` cláusula sin usar el `AS` palabra clave.

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. Los alias deben cumplir con el [identificador](#syntax-identifiers) sintaxis.

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### Notas sobre el uso {#notes-on-usage}

Los alias son globales para una consulta o subconsulta, y puede definir un alias en cualquier parte de una consulta para cualquier expresión. Por ejemplo, `SELECT (1 AS n) + 2, n`.

Los alias no son visibles en subconsultas y entre subconsultas. Por ejemplo, al ejecutar la consulta `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouse genera la excepción `Unknown identifier: num`.

Si se define un alias para las columnas de resultados `SELECT` cláusula de una subconsulta, estas columnas son visibles en la consulta externa. Por ejemplo, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

Tenga cuidado con los alias que son iguales a los nombres de columna o tabla. Consideremos el siguiente ejemplo:

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

En este ejemplo, declaramos tabla `t` con columna `b`. Luego, al seleccionar los datos, definimos el `sum(b) AS b` apodo. Como los alias son globales, ClickHouse sustituyó el literal `b` en la expresión `argMax(a, b)` con la expresión `sum(b)`. Esta sustitución causó la excepción.

## Asterisco {#asterisk}

En un `SELECT` consulta, un asterisco puede reemplazar la expresión. Para obtener más información, consulte la sección “SELECT”.

## Expresiones {#syntax-expressions}

Una expresión es una función, identificador, literal, aplicación de un operador, expresión entre paréntesis, subconsulta o asterisco. También puede contener un alias.
Una lista de expresiones es una o más expresiones separadas por comas.
Las funciones y los operadores, a su vez, pueden tener expresiones como argumentos.

[Artículo Original](https://clickhouse.tech/docs/en/sql_reference/syntax/) <!--hide-->
