---
description: 'Documentación de la cláusula SAMPLE'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'Cláusula SAMPLE'
doc_type: 'reference'
---

La cláusula `SAMPLE` permite el procesamiento aproximado de consultas `SELECT`.

Cuando el muestreo de datos está habilitado, la consulta no se ejecuta sobre todos los datos, sino solo sobre una fracción determinada de ellos (muestra). Por ejemplo, si necesita calcular estadísticas para todas las visitas, basta con ejecutar la consulta sobre 1/10 del total de visitas y luego multiplicar el resultado por 10.

El procesamiento aproximado de consultas puede ser útil en los siguientes casos:

* Cuando tiene requisitos estrictos de latencia (por ejemplo, por debajo de 100 ms), pero no puede justificar el costo de recursos de hardware adicionales para cumplirlos.
* Cuando sus datos sin procesar no son precisos, por lo que la aproximación no degrada la calidad de forma apreciable.
* Cuando los requisitos del negocio apuntan a resultados aproximados (por rentabilidad o para ofrecer resultados exactos a usuarios prémium).

:::note
Solo puede usar muestreo con tablas de la familia [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), y únicamente si la expresión de muestreo se especificó durante la creación de la tabla (consulte [motor MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).
:::

A continuación se enumeran las características del muestreo de datos:

* El muestreo de datos es un mecanismo determinista. El resultado de la misma consulta `SELECT .. SAMPLE` siempre es el mismo.
* El muestreo funciona de forma coherente entre distintas tablas. En tablas con una única clave de muestreo, una muestra con el mismo coeficiente siempre selecciona el mismo subconjunto de datos posibles. Por ejemplo, una muestra de ID de usuario toma filas con el mismo subconjunto de todos los ID de usuario posibles de distintas tablas. Esto significa que puede usar la muestra en subconsultas dentro de la cláusula [IN](../../../sql-reference/operators/in.md). Además, puede combinar muestras mediante la cláusula [JOIN](../../../sql-reference/statements/select/join.md).
* El muestreo permite leer menos datos desde disco. Tenga en cuenta que debe especificar correctamente la clave de muestreo. Para obtener más información, consulte [Creación de una tabla MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Para la cláusula `SAMPLE` se admite la siguiente sintaxis:

| Sintaxis de la cláusula SAMPLE | Descripción                                                                                                                                                                                                                                                                             |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SAMPLE k`                     | Aquí `k` es un número de 0 a 1. La consulta se ejecuta sobre una fracción `k` de los datos. Por ejemplo, `SAMPLE 0.1` ejecuta la consulta sobre el 10 % de los datos. [Más información](#sample-k)                                                                                      |
| `SAMPLE n`                     | Aquí `n` es un entero suficientemente grande. La consulta se ejecuta sobre una muestra de al menos `n` filas (pero no significativamente superior a esa cantidad). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta sobre un mínimo de 10 000 000 filas. [Más información](#sample-n) |
| `SAMPLE k OFFSET m`            | Aquí `k` y `m` son números de 0 a 1. La consulta se ejecuta sobre una muestra de una fracción `k` de los datos. Los datos usados para la muestra se desplazan en una fracción `m`. [Más información](#sample-k-offset-m)                                                                |

<div id="sample-k">
  ## SAMPLE K
</div>

Aquí, `k` es un número entre 0 y 1 (se admiten tanto las notaciones fraccionarias como las decimales). Por ejemplo, `SAMPLE 1/2` o `SAMPLE 0.5`.

En una cláusula `SAMPLE k`, la muestra se toma de la fracción `k` de los datos. A continuación se muestra el ejemplo:

```sql
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

En este ejemplo, la consulta se ejecuta sobre una muestra del 0,1 (10 %) de los datos. Los valores de las funciones de agregación no se ajustan automáticamente, por lo que, para obtener un resultado aproximado, el valor `count()` se multiplica manualmente por 10.

<div id="sample-n">
  ## SAMPLE N
</div>

Aquí, `n` es un entero suficientemente grande. Por ejemplo, `SAMPLE 10000000`.

En este caso, la consulta se ejecuta sobre una muestra de al menos `n` filas (pero no muchas más). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta sobre un mínimo de 10,000,000 filas.

Dado que la unidad mínima de lectura de datos es un gránulo (su tamaño se establece mediante la configuración `index_granularity`), tiene sentido usar una muestra mucho mayor que el tamaño del gránulo.

Al usar la cláusula `SAMPLE n`, no se sabe qué porcentaje relativo de los datos se ha procesado. Por lo tanto, no se sabe qué coeficiente debe aplicarse a las funciones de agregación. Use la columna virtual `_sample_factor` para obtener un resultado aproximado.

La columna `_sample_factor` contiene coeficientes relativos que se calculan dinámicamente. Esta columna se crea automáticamente cuando [crea](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) una tabla con la clave de muestreo especificada. A continuación se muestran ejemplos de uso de la columna `_sample_factor`.

Consideremos la tabla `visits`, que contiene estadísticas sobre las visitas al sitio. El primer ejemplo muestra cómo calcular el número de vistas de página:

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

El siguiente ejemplo muestra cómo calcular el número total de visitas:

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

El ejemplo siguiente muestra cómo calcular la duración media de la sesión. Tenga en cuenta que no es necesario usar el coeficiente relativo para calcular los valores medios.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

<div id="sample-k-offset-m">
  ## SAMPLE K OFFSET M
</div>

Aquí, `k` y `m` son números entre 0 y 1. A continuación se muestran ejemplos.

**Ejemplo 1**

```sql
SAMPLE 1/10
```

En este ejemplo, la muestra representa 1/10 del total de los datos:

`[++------------]`

**Ejemplo 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Aquí se toma una muestra del 10 % de la segunda mitad de los datos.

`[------++------]`