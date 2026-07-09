---
description: 'Descripción general de los formatos de datos compatibles para la entrada y salida en ClickHouse'
sidebar_label: 'Ver todos los formatos...'
sidebar_position: 21
slug: /interfaces/formats
title: 'Formatos de datos de entrada y salida'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # Formatos para datos de entrada y salida
</div>

ClickHouse admite la mayoría de los formatos conocidos de texto y datos binarios. Esto permite una integración sencilla en casi cualquier
pipeline de datos para aprovechar las ventajas de ClickHouse.

<div id="input-formats">
  ## Formatos de entrada
</div>

Los formatos de entrada se utilizan para:

* Analizar los datos proporcionados a las sentencias `INSERT`
* Ejecutar consultas `SELECT` sobre tablas basadas en archivos, como `File`, `URL` o `HDFS`
* Leer diccionarios

Elegir el formato de entrada adecuado es crucial para lograr una ingestión de datos eficiente en ClickHouse. Con más de 70 formatos compatibles,
seleccionar la opción con mejor rendimiento puede afectar significativamente la velocidad de inserción, el uso de CPU y memoria, y la eficiencia
general del sistema. Para ayudarle a orientarse entre estas opciones, evaluamos mediante un benchmark el rendimiento de ingestión de distintos formatos, lo que permitió extraer estas conclusiones clave:

* **El formato [Native](formats/Native.md) es el formato de entrada más eficiente**, ya que ofrece la mejor compresión, el menor
  uso de recursos y una sobrecarga mínima de procesamiento del lado del servidor.
* **La compresión es esencial**: LZ4 reduce el tamaño de los datos con un coste mínimo de CPU, mientras que ZSTD ofrece una compresión mayor a
  costa de un mayor uso de CPU.
* **La preordenación tiene un impacto moderado**, ya que ClickHouse ya ordena de forma eficiente.
* **El procesamiento por lotes mejora significativamente la eficiencia**: los lotes más grandes reducen la sobrecarga de inserción y mejoran el rendimiento.

Para profundizar en los resultados y las buenas prácticas,
lea el [análisis completo del benchmark](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient).
Para ver los resultados completos de las pruebas, explore el dashboard en línea de [FastFormats](https://fastformats.clickhouse.com/).

<div id="output-formats">
  ## Formatos de salida
</div>

Los formatos de salida admitidos se utilizan para:

* Organizar los resultados de una consulta `SELECT`
* Realizar operaciones `INSERT` en tablas basadas en archivos

<div id="formats-overview">
  ## Descripción general de los formatos
</div>

Los formatos admitidos son:

| Formato                                                                                                    | Entrada | Salida |
| ---------------------------------------------------------------------------------------------------------- | ------- | ------ |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔       | ✔      |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔       | ✔      |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔       | ✔      |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔       | ✔      |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔       | ✔      |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔       | ✔      |
| [Template](./formats/Template/Template.md)                                                                 | ✔       | ✔      |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔       | ✗      |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔       | ✔      |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔       | ✔      |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔       | ✔      |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔       | ✔      |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔       | ✔      |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔       | ✔      |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗       | ✔      |
| [Values](./formats/Values.md)                                                                              | ✔       | ✔      |
| [Vertical](./formats/Vertical.md)                                                                          | ✗       | ✔      |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔       | ✔      |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔       | ✗      |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔       | ✗      |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗       | ✔      |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔       | ✔      |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔       | ✔      |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔       | ✔      |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗       | ✔      |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔       | ✔      |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔       | ✔      |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗       | ✔      |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗       | ✔      |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔       | ✔      |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗       | ✔      |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔       | ✔      |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔       | ✔      |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔       | ✔      |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗       | ✔      |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔       | ✔      |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔       | ✔      |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔       | ✔      |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗       | ✔      |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔       | ✔      |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔       | ✔      |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔       | ✔      |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗       | ✔      |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗       | ✔      |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗       | ✔      |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗       | ✔      |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗       | ✔      |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗       | ✔      |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗       | ✔      |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗       | ✔      |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗       | ✔      |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗       | ✔      |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗       | ✔      |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗       | ✔      |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗       | ✔      |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔       | ✔      |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔       | ✔      |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔       | ✔      |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔       | ✔      |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔       | ✔      |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔       | ✔      |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔       | ✗      |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔       | ✔      |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔       | ✔      |
| [ORC](./formats/ORC.md)                                                                                    | ✔       | ✔      |
| [One](./formats/One.md)                                                                                    | ✔       | ✗      |
| [Npy](./formats/Npy.md)                                                                                    | ✔       | ✔      |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔       | ✔      |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔       | ✔      |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔       | ✔      |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔       | ✗      |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔       | ✗      |
| [Native](./formats/Native.md)                                                                              | ✔       | ✔      |
| [Buffers](./formats/Buffers.md)                                                                            | ✔       | ✔      |
| [Null](./formats/Null.md)                                                                                  | ✗       | ✔      |
| [Hash](./formats/Hash.md)                                                                                  | ✗       | ✔      |
| [XML](./formats/XML.md)                                                                                    | ✗       | ✔      |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔       | ✔      |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔       | ✔      |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗       | ✔      |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗       | ✔      |
| [Regexp](./formats/Regexp.md)                                                                              | ✔       | ✗      |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔       | ✔      |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔       | ✔      |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔       | ✗      |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔       | ✔      |
| [DWARF](./formats/DWARF.md)                                                                                | ✔       | ✗      |
| [Markdown](./formats/Markdown.md)                                                                          | ✗       | ✔      |
| [Form](./formats/Form.md)                                                                                  | ✔       | ✗      |

Puede controlar algunos parámetros de procesamiento de formatos mediante la configuración de ClickHouse. Para obtener más información, consulte la sección [Settings](/es/operations/settings/settings-formats.md).

<div id="formatschema">
  ## Esquema de formato
</div>

El nombre del archivo que contiene el esquema de formato se establece mediante el ajuste `format_schema`.
Es necesario establecer este ajuste cuando se utiliza uno de los formatos `Cap'n Proto` y `Protobuf`.
El esquema de formato es una combinación del nombre de un archivo y el nombre de un tipo de mensaje dentro de ese archivo, separados por dos puntos,
por ejemplo, `schemafile.proto:MessageType`.
Si el archivo tiene la extensión estándar del formato (por ejemplo, `.proto` para `Protobuf`),
puede omitirse y, en ese caso, el esquema de formato tendrá este aspecto: `schemafile:MessageType`.

Si introduce o exporta datos mediante el [client](/es/interfaces/client.md) en modo interactivo, el nombre de archivo especificado en el esquema de formato
puede contener una ruta absoluta o una ruta relativa al directorio actual del client.
Si utiliza el client en [modo por lotes](/es/interfaces/client.md/#batch-mode), la ruta al esquema debe ser relativa por motivos de seguridad.

Si introduce o exporta datos mediante la [interfaz HTTP](/es/interfaces/http), el nombre de archivo especificado en el esquema de formato
debe estar ubicado en el directorio especificado en [format&#95;schema&#95;path](/es/operations/server-configuration-parameters/settings.md/#format_schema_path)
de la configuración del servidor.

<div id="skippingerrors">
  ## Omitir errores
</div>

Algunos formatos, como `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` y `Protobuf`, pueden omitir una fila incorrecta si se produce un error de análisis y continuar analizando desde el comienzo de la siguiente fila. Consulte la configuración de [input&#95;format&#95;allow&#95;errors&#95;num](/es/operations/settings/settings-formats.md/#input_format_allow_errors_num) e
[input&#95;format&#95;allow&#95;errors&#95;ratio](/es/operations/settings/settings-formats.md/#input_format_allow_errors_ratio).
Limitaciones:

* En caso de error de análisis, `JSONEachRow` omite todos los datos hasta la nueva línea (o EOF), por lo que las filas deben estar delimitadas por `\n` para que los errores se cuenten correctamente.
* `Template` y `CustomSeparated` usan el delimitador después de la última columna y el delimitador entre filas para encontrar el comienzo de la siguiente fila, por lo que la omisión de errores solo funciona si al menos uno de ellos no está vacío.