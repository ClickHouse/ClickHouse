---
description: 'Documentación de las funciones regulares'
sidebar_label: 'Descripción general'
sidebar_position: 1
slug: /sql-reference/functions/overview
title: 'Funciones regulares'
doc_type: 'reference'
---

Hay al menos* dos tipos de funciones: las funciones regulares (a las que simplemente se llama &quot;funciones&quot;) y las funciones de agregación. Son conceptos completamente distintos. Las funciones regulares actúan como si se aplicaran a cada fila por separado (para cada fila, el resultado de la función no depende de las demás filas). Las funciones de agregación acumulan un conjunto de valores de varias filas (es decir, dependen del conjunto completo de filas).

En esta sección se tratan las funciones regulares. Para las funciones de agregación, consulte la sección &quot;Funciones de agregación&quot;.

:::note
Existe un tercer tipo de función, al que pertenece la función [&#39;arrayJoin&#39;](../functions/array-join.md). Además, las [funciones de tabla](../table-functions/index.md) también pueden mencionarse por separado.
:::

<div id="strong-typing">
  ## Tipado fuerte
</div>

A diferencia del SQL estándar, ClickHouse tiene tipado fuerte. En otras palabras, no realiza conversiones implícitas entre tipos. Cada función opera sobre un conjunto específico de tipos. Esto significa que a veces es necesario usar funciones de conversión de tipos.

<div id="common-subexpression-elimination">
  ## Eliminación de subexpresiones comunes
</div>

Se considera que todas las expresiones de una consulta que tienen el mismo AST (el mismo registro o el mismo resultado del análisis sintáctico) tienen valores idénticos. Estas expresiones se agrupan y se ejecutan una sola vez. Las subconsultas idénticas también se eliminan de esta manera.

<div id="types-of-results">
  ## Tipos de resultados
</div>

Todas las funciones devuelven un único valor como resultado (no varios ni ninguno). El tipo del resultado normalmente se define solo por los tipos de los argumentos, no por los valores. Las excepciones son la función tupleElement (el operador a.N) y la función toFixedString.

<div id="constants">
  ## Constantes
</div>

Por simplicidad, algunas funciones solo pueden trabajar con constantes en determinados argumentos. Por ejemplo, el argumento derecho del operador LIKE debe ser una constante.
Casi todas las funciones devuelven una constante cuando reciben argumentos constantes. La excepción son las funciones que generan números aleatorios.
La función &#39;now&#39; devuelve valores distintos para consultas ejecutadas en momentos diferentes, pero el resultado se considera una constante, ya que la constancia solo es importante dentro de una misma consulta.
Una expresión constante también se considera una constante (por ejemplo, la parte derecha del operador LIKE puede construirse a partir de varias constantes).

Las funciones pueden implementarse de distintas maneras para argumentos constantes y no constantes (se ejecuta código diferente). Sin embargo, los resultados de una constante y de una columna real que contiene únicamente ese mismo valor deben coincidir.

<div id="null-processing">
  ## Procesamiento de NULL
</div>

Las funciones presentan los siguientes comportamientos:

* Si al menos uno de los argumentos de la función es `NULL`, el resultado de la función también es `NULL`.
* Un comportamiento especial que se especifica de forma individual en la descripción de cada función. En el código fuente de ClickHouse, estas funciones tienen `UseDefaultImplementationForNulls=false`.

<div id="constancy">
  ## Constancia
</div>

Las funciones no pueden cambiar los valores de sus argumentos; cualquier cambio se devuelve como resultado. Por lo tanto, el resultado de calcular funciones por separado no depende del orden en que estas se escriban en la consulta.

<div id="higher-order-functions">
  ## Funciones de orden superior
</div>

<div id="arrow-operator-and-lambda">
  ### Operador `->` y funciones lambda(params, expr)
</div>

Las funciones de orden superior solo pueden aceptar funciones lambda como argumento de función. Para pasar una función lambda a una función de orden superior, use el operador `->`. En el lado izquierdo de la flecha va un parámetro formal, que puede ser cualquier ID, o varios parámetros formales: cualquier ID en una tupla. En el lado derecho de la flecha va una expresión que puede usar estos parámetros formales, así como cualquier columna de la tabla.

Ejemplos:

```python
x -> 2 * x
str -> str != Referer
```

Una función lambda que acepta varios argumentos también puede pasarse a una función de orden superior. En este caso, a la función de orden superior se le pasan varios arrays de la misma longitud, con los que se corresponderán esos argumentos.

En algunas funciones, el primer argumento (la función lambda) puede omitirse. En este caso, se asume un mapeo identidad.

<div id="bare-function-names-as-lambdas">
  ### Nombres de funciones simples como expresiones `lambda`
</div>

En lugar de escribir una expresión `lambda` completa, puedes pasar directamente el nombre de una función a una función de orden superior. El nombre de la función se convierte automáticamente en una expresión `lambda` equivalente.

Por ejemplo, los siguientes pares son equivalentes:

```sql
SELECT arrayMap(negate, [1, 2, 3]);            -- [-1, -2, -3]
SELECT arrayMap(x -> negate(x), [1, 2, 3]);    -- [-1, -2, -3]

SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);            -- [11, 22, 33]
SELECT arrayMap((x, y) -> plus(x, y), [1, 2, 3], [10, 20, 30]); -- [11, 22, 33]

SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);            -- [1, 3, 5]
SELECT arrayFilter(x -> isNotNull(x), [1, NULL, 3, NULL, 5]);    -- [1, 3, 5]

SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));                      -- 15
SELECT arrayFold((acc, x) -> plus(acc, x), [1, 2, 3, 4, 5], toUInt64(0));  -- 15
```

Esto funciona con funciones integradas, UDF de SQL, UDF ejecutables y UDF de WebAssembly. Los nombres de columna y los alias tienen prioridad sobre los nombres de función cuando hay ambigüedad.

La aridad de la lambda se toma de la función interna. Por ejemplo, `arrayMap(plus, ...)` usa aridad 2 porque `plus` toma dos argumentos, así que también funciona con entradas de tupla como `arrayMap(plus, [(1, 10), (2, 20)])`, donde los elementos de la tupla se desempaquetan en los argumentos de la lambda.

En las funciones internas variádicas (como `concat`, que acepta cualquier cantidad de argumentos), la aridad de la lambda pasa a depender del número de argumentos de array. Esto es correcto para funciones de orden superior como `arrayMap`, `arrayFilter` y `arrayFold`. En funciones de orden superior que aceptan parámetros fijos no array además de arrays —por ejemplo, `arrayPartialSort(f, limit, arr)`— los nombres simples de funciones variádicas pueden dar una aridad incorrecta, en cuyo caso se requiere una lambda explícita.

Las funciones internas variádicas tampoco desempaquetan automáticamente las entradas de tupla. Por ejemplo, `arrayMap(concat, [('a', 'b'), ('c', 'd')])` se reescribe como una lambda unaria y no es equivalente a `arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')])`. Usa una lambda explícita cuando quieras desestructurar los elementos de una tupla en una llamada variádica.

<div id="user-defined-functions-udfs">
  ## Funciones definidas por el usuario (UDFs)
</div>

ClickHouse admite funciones definidas por el usuario. Consulte [UDFs](../functions/udf.md).