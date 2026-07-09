---
alias: []
description: 'Documentación del formato ArrowStream'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

`ArrowStream` es el formato en &quot;modo de flujo&quot; de Apache Arrow. Está diseñado para el procesamiento de flujo en memoria.

<div id="example-usage">
  ## Uso de ejemplo
</div>

En el ejemplo siguiente usamos el conjunto de datos `forex`, que está disponible en el
[Playground de SQL de ClickHouse](https://sql.clickhouse.com). Puede conectarse
remotamente con `clickhouse-client` al host `sql-clickhouse.clickhouse.com`
y con el usuario `demo` (que no tiene contraseña). La tabla `forex` está en la
base de datos `forex`, por lo que la seleccionamos como base de datos predeterminada:

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

La tabla `forex` almacena tipos de cambio de divisas. Podemos inspeccionar su tamaño y
el nivel de compresión en disco consultando [`system.columns`](/es/operations/system-tables/columns):

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

A diferencia del formato [`Arrow`](/es/interfaces/formats/Arrow) en &quot;modo archivo&quot;, que
requiere disponer del resultado completo antes de poder leerse, `ArrowStream` se entrega como una
secuencia de lotes de registros que un consumidor puede leer de forma incremental a medida que
llegan. Esto lo hace muy adecuado para transmitir un resultado de consulta directamente a una
herramienta de visualización o analítica sin tener que materializar antes todo el dataset.

Para transmitir el resultado, envíe la consulta a través de la interfaz HTTP de ClickHouse con una
solicitud `POST` y lea la respuesta como un flujo Arrow. Desactivamos la compresión
de la salida Arrow mediante la opción
[`output_format_arrow_compression_method`](/es/operations/settings/formats#output_format_arrow_compression_method)
para que los consumidores puedan decodificar los lotes directamente a medida que se reciben.

La salida de `ArrowStream` es binaria sin procesar, así que, en lugar de imprimirla en la
terminal, la canalizamos a un consumidor. El flujo es autodescriptivo (incluye
su propio esquema), por lo que aquí lo canalizamos directamente a
[`clickhouse-local`](/es/operations/utilities/clickhouse-local), que lee los
lotes entrantes con `--input-format ArrowStream` y los consulta como una tabla.
La tabla `forex` es grande, así que acotamos la consulta remota con un predicado `WHERE`
y un `LIMIT` para que este ejemplo siga siendo pequeño:

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

El mismo flujo puede ser consumido de forma incremental por cualquier client compatible con Arrow, que
lo lee lote a lote en lugar de almacenar el resultado completo en el búfer. Por ejemplo,
usando la [biblioteca JavaScript de Apache Arrow](https://arrow.apache.org/docs/js/), un
`RecordBatchReader` produce cada lote de registros en cuanto se transmite desde el
servidor:

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

Para consultar una guía completa sobre cómo transmitir datos `ArrowStream` en flujo desde ClickHouse a una
visualización en tiempo real con [Perspective](https://perspective.finos.org/), consulta
la entrada del blog
[Streaming real-time visualizations with ClickHouse, Apache Arrow and Perspective](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective).

<div id="format-settings">
  ## Configuración del formato
</div>

`ArrowStream` comparte la misma configuración que el formato [`Arrow`](/es/interfaces/formats/Arrow).

| Configuración                                                                | Descripción                                                                                                                                                                        | Predeterminado |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `input_format_arrow_allow_missing_columns`                                   | Permite columnas ausentes al leer formatos de entrada Arrow                                                                                                                        | `1`            |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignora mayúsculas y minúsculas al hacer coincidir columnas Arrow con columnas de CH.                                                                                               | `0`            |
| `input_format_arrow_import_nested`                                           | Configuración obsoleta, no hace nada.                                                                                                                                              | `0`            |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Omite columnas con tipos no compatibles durante la inferencia de esquema del formato Arrow                                                                                         | `0`            |
| `output_format_arrow_compression_method`                                     | Método de compresión para el formato de salida Arrow. Códecs compatibles: lz4&#95;frame, zstd, none (sin comprimir)                                                                | `lz4_frame`    |
| `output_format_arrow_date_as_uint16`                                         | Escribe valores Date como números simples de 16 bits (se vuelven a leer como UInt16), en lugar de convertirlos en un tipo Arrow DATE32 de 32 bits (se vuelven a leer como Date32). | `0`            |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Usa el tipo Arrow FIXED&#95;SIZE&#95;BINARY en lugar de Binary para columnas FixedString.                                                                                          | `1`            |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Habilita la salida del tipo LowCardinality como tipo Arrow Dictionary                                                                                                              | `0`            |
| `output_format_arrow_string_as_string`                                       | Usa el tipo Arrow String en lugar de Binary para columnas String                                                                                                                   | `1`            |
| `output_format_arrow_unsupported_types_as_binary`                            | Genera como datos binarios sin procesar los tipos que no tengan conversión. Si es false, esos tipos generarían una excepción UNKNOWN&#95;TYPE.                                     | `1`            |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Usa siempre enteros de 64 bits para los índices de diccionario en el formato Arrow                                                                                                 | `0`            |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Usa enteros con signo para los índices de diccionario en el formato Arrow                                                                                                          | `1`            |