---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Cláusula SAMPLE {#select-sample-clause}

El `SAMPLE` cláusula permite aproximadamente `SELECT` procesamiento de consultas.

Cuando se habilita el muestreo de datos, la consulta no se realiza en todos los datos, sino solo en una cierta fracción de datos (muestra). Por ejemplo, si necesita calcular estadísticas para todas las visitas, es suficiente ejecutar la consulta en la fracción 1/10 de todas las visitas y luego multiplicar el resultado por 10.

El procesamiento de consultas aproximado puede ser útil en los siguientes casos:

-   Cuando tiene requisitos de temporización estrictos (como \<100 ms) pero no puede justificar el costo de recursos de hardware adicionales para cumplirlos.
-   Cuando sus datos sin procesar no son precisos, la aproximación no degrada notablemente la calidad.
-   Los requisitos comerciales se centran en los resultados aproximados (por rentabilidad o para comercializar los resultados exactos a los usuarios premium).

!!! note "Nota"
    Sólo puede utilizar el muestreo con las tablas en el [Método de codificación de datos:](../../../engines/table-engines/mergetree-family/mergetree.md) familia, y sólo si la expresión de muestreo se especificó durante la creación de la tabla (ver [Motor MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Las características del muestreo de datos se enumeran a continuación:

-   El muestreo de datos es un mecanismo determinista. El resultado de la misma `SELECT .. SAMPLE` la consulta es siempre la misma.
-   El muestreo funciona consistentemente para diferentes tablas. Para tablas con una sola clave de muestreo, una muestra con el mismo coeficiente siempre selecciona el mismo subconjunto de datos posibles. Por ejemplo, una muestra de ID de usuario toma filas con el mismo subconjunto de todos los ID de usuario posibles de diferentes tablas. Esto significa que puede utilizar el ejemplo en subconsultas [IN](../../operators/in.md) clausula. Además, puede unir muestras usando el [JOIN](join.md) clausula.
-   El muestreo permite leer menos datos de un disco. Tenga en cuenta que debe especificar la clave de muestreo correctamente. Para obtener más información, consulte [Creación de una tabla MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Para el `SAMPLE` cláusula se admite la siguiente sintaxis:

| SAMPLE Clause Syntax | Descripci                                                                                                                                                                                                                                                                     |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Aqui `k` es el número de 0 a 1.</br>La consulta se ejecuta en `k` de datos. Por ejemplo, `SAMPLE 0.1` ejecuta la consulta en el 10% de los datos. [Leer más](#select-sample-k)                                                                                                |
| `SAMPLE n`           | Aqui `n` es un entero suficientemente grande.</br>La consulta se ejecuta en una muestra de al menos `n` filas (pero no significativamente más que esto). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta en un mínimo de 10.000.000 de filas. [Leer más](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Aqui `k` y `m` son los números del 0 al 1.</br>La consulta se ejecuta en una muestra de `k` de los datos. Los datos utilizados para el ejemplo se compensan por `m` fracción. [Leer más](#select-sample-offset)                                                               |

## SAMPLE K {#select-sample-k}

Aqui `k` es el número de 0 a 1 (se admiten notaciones fraccionarias y decimales). Por ejemplo, `SAMPLE 1/2` o `SAMPLE 0.5`.

En un `SAMPLE k` cláusula, la muestra se toma de la `k` de datos. El ejemplo se muestra a continuación:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

En este ejemplo, la consulta se ejecuta en una muestra de 0,1 (10%) de datos. Los valores de las funciones agregadas no se corrigen automáticamente, por lo que para obtener un resultado aproximado, el valor `count()` se multiplica manualmente por 10.

## SAMPLE N {#select-sample-n}

Aqui `n` es un entero suficientemente grande. Por ejemplo, `SAMPLE 10000000`.

En este caso, la consulta se ejecuta en una muestra de al menos `n` filas (pero no significativamente más que esto). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta en un mínimo de 10.000.000 de filas.

Dado que la unidad mínima para la lectura de datos es un gránulo (su tamaño se establece mediante el `index_granularity` ajuste), tiene sentido establecer una muestra que es mucho más grande que el tamaño del gránulo.

Cuando se utiliza el `SAMPLE n` cláusula, no sabe qué porcentaje relativo de datos se procesó. Por lo tanto, no sabe el coeficiente por el que se deben multiplicar las funciones agregadas. Utilice el `_sample_factor` columna virtual para obtener el resultado aproximado.

El `_sample_factor` columna contiene coeficientes relativos que se calculan dinámicamente. Esta columna se crea automáticamente cuando [crear](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) una tabla con la clave de muestreo especificada. Los ejemplos de uso del `_sample_factor` columna se muestran a continuación.

Consideremos la tabla `visits`, que contiene las estadísticas sobre las visitas al sitio. El primer ejemplo muestra cómo calcular el número de páginas vistas:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

El siguiente ejemplo muestra cómo calcular el número total de visitas:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

El siguiente ejemplo muestra cómo calcular la duración media de la sesión. Tenga en cuenta que no necesita usar el coeficiente relativo para calcular los valores promedio.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

## SAMPLE K OFFSET M {#select-sample-offset}

Aqui `k` y `m` son números del 0 al 1. Los ejemplos se muestran a continuación.

**Ejemplo 1**

``` sql
SAMPLE 1/10
```

En este ejemplo, la muestra es 1/10 de todos los datos:

`[++------------]`

**Ejemplo 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Aquí, se toma una muestra del 10% de la segunda mitad de los datos.

`[------++------]`
