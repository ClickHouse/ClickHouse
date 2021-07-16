---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# ORDEN POR CLÁUSULA {#select-order-by}

El `ORDER BY` cláusula contiene una lista de expresiones, cada una de las cuales puede atribuirse con `DESC` (descendente) o `ASC` modificador (ascendente) que determina la dirección de clasificación. Si no se especifica la dirección, `ASC` se supone, por lo que generalmente se omite. La dirección de ordenación se aplica a una sola expresión, no a toda la lista. Ejemplo: `ORDER BY Visits DESC, SearchPhrase`

Las filas que tienen valores idénticos para la lista de expresiones de clasificación se generan en un orden arbitrario, que también puede ser no determinista (diferente cada vez).
Si se omite la cláusula ORDER BY, el orden de las filas tampoco está definido, y también puede ser no determinista.

## Clasificación de valores especiales {#sorting-of-special-values}

Hay dos enfoques para `NaN` y `NULL` orden de clasificación:

-   Por defecto o con el `NULLS LAST` modificador: primero los valores, luego `NaN`, entonces `NULL`.
-   Con el `NULLS FIRST` modificador: primero `NULL`, entonces `NaN`, luego otros valores.

### Ejemplo {#example}

Para la mesa

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Ejecute la consulta `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` conseguir:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Cuando se ordenan los números de coma flotante, los NaN están separados de los otros valores. Independientemente del orden de clasificación, los NaN vienen al final. En otras palabras, para la clasificación ascendente se colocan como si fueran más grandes que todos los demás números, mientras que para la clasificación descendente se colocan como si fueran más pequeños que el resto.

## Soporte de colación {#collation-support}

Para ordenar por valores de cadena, puede especificar la intercalación (comparación). Ejemplo: `ORDER BY SearchPhrase COLLATE 'tr'` - para ordenar por palabra clave en orden ascendente, utilizando el alfabeto turco, insensible a mayúsculas y minúsculas, suponiendo que las cadenas están codificadas en UTF-8. COLLATE se puede especificar o no para cada expresión en ORDER BY de forma independiente. Si se especifica ASC o DESC, se especifica COLLATE después de él. Cuando se usa COLLATE, la clasificación siempre distingue entre mayúsculas y minúsculas.

Solo recomendamos usar COLLATE para la clasificación final de un pequeño número de filas, ya que la clasificación con COLLATE es menos eficiente que la clasificación normal por bytes.

## Detalles de implementación {#implementation-details}

Menos RAM se utiliza si un pequeño suficiente [LIMIT](limit.md) se especifica además `ORDER BY`. De lo contrario, la cantidad de memoria gastada es proporcional al volumen de datos para clasificar. Para el procesamiento de consultas distribuidas, si [GROUP BY](group-by.md) se omite, la clasificación se realiza parcialmente en servidores remotos y los resultados se fusionan en el servidor solicitante. Esto significa que para la ordenación distribuida, el volumen de datos a ordenar puede ser mayor que la cantidad de memoria en un único servidor.

Si no hay suficiente RAM, es posible realizar la clasificación en la memoria externa (creando archivos temporales en un disco). Utilice el ajuste `max_bytes_before_external_sort` para este propósito. Si se establece en 0 (el valor predeterminado), la ordenación externa está deshabilitada. Si está habilitada, cuando el volumen de datos a ordenar alcanza el número especificado de bytes, los datos recopilados se ordenan y se vuelcan en un archivo temporal. Después de leer todos los datos, todos los archivos ordenados se fusionan y se generan los resultados. Los archivos se escriben en el `/var/lib/clickhouse/tmp/` directorio en la configuración (de forma predeterminada, pero puede usar el `tmp_path` parámetro para cambiar esta configuración).

La ejecución de una consulta puede usar más memoria que `max_bytes_before_external_sort`. Por este motivo, esta configuración debe tener un valor significativamente menor que `max_memory_usage`. Como ejemplo, si su servidor tiene 128 GB de RAM y necesita ejecutar una sola consulta, establezca `max_memory_usage` de hasta 100 GB, y `max_bytes_before_external_sort` para 80 GB.

La clasificación externa funciona con mucha menos eficacia que la clasificación en RAM.
