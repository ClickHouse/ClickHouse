---
machine_translated: true
---

# Función {#functions}

Hay al menos \* dos tipos de funciones: funciones regulares (simplemente se llaman “functions”) y funciones agregadas. Estos son conceptos completamente diferentes. Las funciones regulares funcionan como si se aplicaran a cada fila por separado (para cada fila, el resultado de la función no depende de las otras filas). Las funciones agregadas acumulan un conjunto de valores de varias filas (es decir,dependen de todo el conjunto de filas).

En esta sección discutimos las funciones regulares. Para las funciones agregadas, consulte la sección “Aggregate functions”.

\* - Existe un tercer tipo de función que el ‘arrayJoin’ la función pertenece a; las funciones de la tabla también se pueden mencionar por separado.\*

## Tipeo fuerte {#strong-typing}

A diferencia del SQL estándar, ClickHouse tiene una tipificación fuerte. En otras palabras, no hace conversiones implícitas entre tipos. Cada función funciona para un conjunto específico de tipos. Esto significa que a veces necesita usar funciones de conversión de tipos.

## Eliminación de subexpresiones comunes {#common-subexpression-elimination}

Se considera que todas las expresiones de una consulta que tienen el mismo AST (el mismo registro o el mismo resultado del análisis sintáctico) tienen valores idénticos. Tales expresiones se concatenan y se ejecutan una vez. Las subconsultas idénticas también se eliminan de esta manera.

## Tipos de resultados {#types-of-results}

Todas las funciones devuelven un único retorno como resultado (no varios valores, y no valores cero). El tipo de resultado generalmente se define solo por los tipos de argumentos, no por los valores. Las excepciones son la función tupleElement (el operador a.N) y la función toFixedString.

## Constante {#constants}

Para simplificar, ciertas funciones solo pueden funcionar con constantes para algunos argumentos. Por ejemplo, el argumento correcto del operador LIKE debe ser una constante.
Casi todas las funciones devuelven una constante para argumentos constantes. La excepción son las funciones que generan números aleatorios.
El ‘now’ función devuelve valores diferentes para las consultas que se ejecutaron en diferentes momentos, pero el resultado se considera una constante, ya que la constancia solo es importante dentro de una sola consulta.
Una expresión constante también se considera una constante (por ejemplo, la mitad derecha del operador LIKE se puede construir a partir de múltiples constantes).

Las funciones se pueden implementar de diferentes maneras para argumentos constantes y no constantes (se ejecuta un código diferente). Pero los resultados para una constante y para una columna verdadera que contiene solo el mismo valor deben coincidir entre sí.

## Procesamiento NULL {#null-processing}

Las funciones tienen los siguientes comportamientos:

-   Si al menos uno de los argumentos de la función es `NULL` el resultado de la función es también `NULL`.
-   Comportamiento especial que se especifica individualmente en la descripción de cada función. En el código fuente de ClickHouse, estas funciones tienen `UseDefaultImplementationForNulls=false`.

## Constancia {#constancy}

Las funciones no pueden cambiar los valores de sus argumentos; cualquier cambio se devuelve como resultado. Por lo tanto, el resultado del cálculo de funciones separadas no depende del orden en que se escriban las funciones en la consulta.

## Manejo de errores {#error-handling}

Algunas funciones pueden producir una excepción si los datos no son válidos. En este caso, la consulta se cancela y se devuelve un texto de error al cliente. Para el procesamiento distribuido, cuando se produce una excepción en uno de los servidores, los otros servidores también intentan anular la consulta.

## Evaluación de expresiones de argumento {#evaluation-of-argument-expressions}

En casi todos los lenguajes de programación, uno de los argumentos puede no evaluarse para ciertos operadores. Esto suele ser los operadores `&&`, `||`, y `?:`.
Pero en ClickHouse, los argumentos de las funciones (operadores) siempre se evalúan. Esto se debe a que partes enteras de columnas se evalúan a la vez, en lugar de calcular cada fila por separado.

## Realización de funciones para el procesamiento de consultas distribuidas {#performing-functions-for-distributed-query-processing}

Para el procesamiento de consultas distribuidas, se realizan tantas etapas de procesamiento de consultas como sea posible en servidores remotos, y el resto de las etapas (fusionando resultados intermedios y todo lo posterior) se realizan en el servidor solicitante.

Esto significa que las funciones se pueden realizar en diferentes servidores.
Por ejemplo, en la consulta `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   Más información `distributed_table` tiene al menos dos fragmentos, las funciones ‘g’ y ‘h’ se realizan en servidores remotos, y la función ‘f’ se realiza en el servidor solicitante.
-   Más información `distributed_table` tiene sólo un fragmento, todos los ‘f’, ‘g’, y ‘h’ funciones se realizan en el servidor de este fragmento.

El resultado de una función generalmente no depende del servidor en el que se realice. Sin embargo, a veces esto es importante.
Por ejemplo, las funciones que funcionan con diccionarios utilizan el diccionario que existe en el servidor en el que se están ejecutando.
Otro ejemplo es el `hostName` función, que devuelve el nombre del servidor en el que se está ejecutando para `GROUP BY` por servidores en un `SELECT` consulta.

Si se realiza una función en una consulta en el servidor solicitante, pero debe realizarla en servidores remotos, puede envolverla en un ‘any’ agregar o agregarlo a una clave en `GROUP BY`.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/) <!--hide-->
