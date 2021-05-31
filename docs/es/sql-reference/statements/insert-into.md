---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: INSERT INTO
---

## INSERT {#insert}

Adición de datos.

Formato de consulta básico:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

La consulta puede especificar una lista de columnas para insertar `[(c1, c2, c3)]`. En este caso, el resto de las columnas se llenan con:

-   Los valores calculados a partir del `DEFAULT` expresiones especificadas en la definición de la tabla.
-   Ceros y cadenas vacías, si `DEFAULT` expresiones no están definidas.

Si [strict_insert_defaults=1](../../operations/settings/settings.md), columnas que no tienen `DEFAULT` definido debe figurar en la consulta.

Los datos se pueden pasar al INSERT en cualquier [formato](../../interfaces/formats.md#formats) con el apoyo de ClickHouse. El formato debe especificarse explícitamente en la consulta:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse elimina todos los espacios y un avance de línea (si hay uno) antes de los datos. Al formar una consulta, recomendamos colocar los datos en una nueva línea después de los operadores de consulta (esto es importante si los datos comienzan con espacios).

Ejemplo:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Puede insertar datos por separado de la consulta mediante el cliente de línea de comandos o la interfaz HTTP. Para obtener más información, consulte la sección “[Interfaz](../../interfaces/index.md#interfaces)”.

### Limitación {#constraints}

Si la tabla tiene [limitación](create.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### Insertar los resultados de `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Las columnas se asignan de acuerdo con su posición en la cláusula SELECT. Sin embargo, sus nombres en la expresión SELECT y la tabla para INSERT pueden diferir. Si es necesario, se realiza la fundición de tipo.

Ninguno de los formatos de datos, excepto Valores, permite establecer valores para expresiones como `now()`, `1 + 2` y así sucesivamente. El formato Values permite el uso limitado de expresiones, pero esto no se recomienda, porque en este caso se usa código ineficiente para su ejecución.

No se admiten otras consultas para modificar partes de datos: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Sin embargo, puede eliminar datos antiguos usando `ALTER TABLE ... DROP PARTITION`.

`FORMAT` cláusula debe especificarse al final de la consulta si `SELECT` cláusula contiene la función de tabla [entrada()](../table-functions/input.md).

### Consideraciones de rendimiento {#performance-considerations}

`INSERT` ordena los datos de entrada por clave principal y los divide en particiones por una clave de partición. Si inserta datos en varias particiones a la vez, puede reducir significativamente el rendimiento del `INSERT` consulta. Para evitar esto:

-   Agregue datos en lotes bastante grandes, como 100.000 filas a la vez.
-   Agrupe los datos por una clave de partición antes de cargarlos en ClickHouse.

El rendimiento no disminuirá si:

-   Los datos se agregan en tiempo real.
-   Carga datos que normalmente están ordenados por tiempo.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
