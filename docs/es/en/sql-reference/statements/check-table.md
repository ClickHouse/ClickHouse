---
description: 'Documentación de CHECK TABLE'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'Sentencia CHECK TABLE'
doc_type: 'reference'
---

La consulta `CHECK TABLE` en ClickHouse se utiliza para realizar una comprobación de validación en una tabla específica o en sus particiones. Garantiza la integridad de los datos al verificar las sumas de comprobación y otras estructuras de datos internas.

En particular, compara los tamaños reales de los archivos con los valores esperados almacenados en el servidor. Si los tamaños de los archivos no coinciden con los valores almacenados, significa que los datos están corruptos. Esto puede deberse, por ejemplo, a un fallo del sistema durante la ejecución de una consulta.

:::warning
La consulta `CHECK TABLE` puede leer todos los datos de la tabla y mantener ocupados ciertos recursos, por lo que puede consumir muchos recursos.
Considere el posible impacto en el rendimiento y el uso de recursos antes de ejecutar esta consulta.
Esta consulta no mejorará el rendimiento del sistema y no debe ejecutarla si no está seguro de lo que está haciendo.
:::

<div id="syntax">
  ## Sintaxis
</div>

La sintaxis básica de la consulta es la siguiente:

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name`: Especifica el nombre de la tabla que desea verificar.
* `partition_expression`: (Opcional) Si desea verificar una partición específica de la tabla, puede usar esta expresión para indicarla.
* `part_name`: (Opcional) Si desea verificar una parte específica de la tabla, puede añadir un literal de cadena para indicar el nombre de la parte.
* `FORMAT format`: (Opcional) Le permite especificar el formato de salida del resultado.
* `SETTINGS`: (Opcional) Permite ajustes adicionales.
  * (Opcional): [check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result): Este ajuste controla si la salida es detallada (`0`) o resumida (`1`).
  * También se pueden aplicar otros ajustes. Si no necesita un orden determinista en los resultados, puede establecer max&#95;threads en un valor mayor que uno para acelerar la consulta.

La respuesta de la consulta depende del valor del ajuste `check_query_single_value_result`.
En caso de `check_query_single_value_result = 1`, solo se devuelve la columna `result` con una única fila. El valor de esta fila es `1` si la comprobación de integridad se supera y `0` si los datos están corruptos.

Con `check_query_single_value_result = 0`, la consulta devuelve las siguientes columnas:

* `part_path`: Indica la ruta a la parte de datos o el nombre del archivo.
  * `is_passed`: Devuelve 1 si la comprobación de esta parte se realiza correctamente; en caso contrario, 0.
  * `message`: Cualquier mensaje adicional relacionado con la comprobación, como errores o mensajes de éxito.

La consulta `CHECK TABLE` admite los siguientes motores de tabla:

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [familia MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

Ejecutarla sobre tablas con otros motores de tabla produce una excepción `NOT_IMPLEMENTED`.

Los motores de la familia `*Log` no proporcionan recuperación automática de datos en caso de fallo. Utilice la consulta `CHECK TABLE` para detectar la pérdida de datos a tiempo.

<div id="examples">
  ## Ejemplos
</div>

Por defecto, la consulta `CHECK TABLE` muestra el estado general de la comprobación de la tabla:

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

Si desea ver el estado de la comprobación de cada parte de datos por separado, puede usar la configuración `check_query_single_value_result`.

Además, para comprobar una partición específica de la tabla, puede usar la palabra clave `PARTITION`.

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

De manera similar, puedes comprobar una parte específica de la tabla usando la palabra clave `PART`.

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

Tenga en cuenta que, cuando la parte no existe, la consulta devuelve un error:

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### Cuando se obtiene un resultado &#39;corrupto&#39;
</div>

:::warning
Descargo de responsabilidad: El procedimiento que se describe aquí, incluida la manipulación manual o la eliminación de archivos directamente en el directorio de datos, es solo para entornos experimentales o de desarrollo. **No** intente esto en un servidor de producción, ya que puede provocar la pérdida de datos u otras consecuencias no deseadas.
:::

Elimine el archivo de suma de comprobación existente:

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

Si falta el archivo checksums.txt, se puede restaurar. Se volverá a calcular y a escribir durante la ejecución del comando CHECK TABLE para la partición específica, y el estado seguirá mostrándose como &#39;is&#95;passed = 1&#39;.

Puede comprobar todas las tablas `(Replicated)MergeTree` existentes a la vez mediante la consulta `CHECK ALL TABLES`.

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## Si los datos están corruptos
</div>

Si la tabla está corrupta, puede copiar los datos no corruptos a otra tabla. Para ello:

1. Cree una tabla nueva con la misma estructura que la tabla dañada. Para ello, ejecute la consulta `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2. Establezca `max_threads` en 1 para procesar la siguiente consulta en un solo hilo. Para ello, ejecute la consulta `SET max_threads = 1`.
3. Ejecute la consulta `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Esta operación copia los datos no corruptos de la tabla dañada a otra tabla. Solo se copiarán los datos anteriores a la parte corrupta.
4. Reinicie `clickhouse-client` para restablecer el valor de `max_threads`.