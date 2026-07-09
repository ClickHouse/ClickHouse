---
description: 'Обзор поддерживаемых форматов входных и выходных данных в ClickHouse'
sidebar_label: 'Все форматы...'
sidebar_position: 21
slug: /interfaces/formats
title: 'Форматы входных и выходных данных'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # Форматы входных и выходных данных
</div>

ClickHouse поддерживает большинство известных текстовых и бинарных форматов данных. Это позволяет легко интегрировать его практически в любой существующий
конвейер обработки данных и использовать преимущества ClickHouse.

<div id="input-formats">
  ## Форматы ввода
</div>

Форматы ввода используются для:

* Разбора данных, передаваемых в операторы `INSERT`
* Выполнения запросов `SELECT` к таблицам с файловой поддержкой, таким как `File`, `URL` или `HDFS`
* Чтения словарей

Выбор подходящего формата ввода критически важен для эффективной ингестии данных в ClickHouse. При более чем 70 поддерживаемых форматах
выбор самого производительного варианта может существенно повлиять на скорость вставки, использование CPU и памяти, а также на общую
эффективность системы. Чтобы помочь с этим выбором, мы провели бенчмарк производительности ингестии для разных форматов и выделили следующие ключевые выводы:

* **Формат [Native](formats/Native.md) — самый эффективный формат ввода**, обеспечивающий лучшее сжатие, минимальное
  потребление ресурсов и минимальные накладные расходы на обработку на стороне сервера.
* **Сжатие крайне важно** — LZ4 уменьшает размер данных с минимальными затратами CPU, тогда как ZSTD обеспечивает более высокую степень сжатия
  ценой дополнительного использования CPU.
* **Предварительная сортировка оказывает умеренное влияние**, поскольку ClickHouse и без того эффективно выполняет сортировку.
* **Батчинг значительно повышает эффективность** — более крупные батчи снижают накладные расходы на вставку и повышают пропускную способность.

Чтобы подробнее ознакомиться с результатами и лучшими практиками,
прочитайте полный [анализ бенчмарка](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient).
Полные результаты тестирования можно посмотреть в онлайн-панели мониторинга [FastFormats](https://fastformats.clickhouse.com/).

<div id="output-formats">
  ## Форматы вывода
</div>

Поддерживаемые форматы вывода используются для:

* Представления результатов запроса `SELECT`
* Выполнения операций `INSERT` в таблицы с файловой поддержкой

<div id="formats-overview">
  ## Обзор форматов
</div>

Поддерживаемые форматы:

| Формат                                                                                                     | Вход | Выход |
| ---------------------------------------------------------------------------------------------------------- | ---- | ----- |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔    | ✔     |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔    | ✔     |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔    | ✔     |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔    | ✔     |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔    | ✔     |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔    | ✔     |
| [Template](./formats/Template/Template.md)                                                                 | ✔    | ✔     |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔    | ✗     |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔    | ✔     |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔    | ✔     |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔    | ✔     |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔    | ✔     |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔    | ✔     |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔    | ✔     |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗    | ✔     |
| [Values](./formats/Values.md)                                                                              | ✔    | ✔     |
| [Vertical](./formats/Vertical.md)                                                                          | ✗    | ✔     |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔    | ✔     |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔    | ✗     |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔    | ✗     |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗    | ✔     |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔    | ✔     |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔    | ✔     |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔    | ✔     |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗    | ✔     |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔    | ✔     |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔    | ✔     |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗    | ✔     |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗    | ✔     |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔    | ✔     |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗    | ✔     |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔    | ✔     |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔    | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔    | ✔     |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗    | ✔     |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔    | ✔     |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗    | ✔     |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔    | ✔     |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔    | ✔     |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔    | ✔     |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗    | ✔     |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗    | ✔     |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗    | ✔     |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗    | ✔     |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗    | ✔     |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗    | ✔     |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗    | ✔     |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗    | ✔     |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗    | ✔     |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗    | ✔     |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗    | ✔     |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗    | ✔     |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗    | ✔     |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔    | ✔     |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔    | ✔     |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔    | ✔     |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔    | ✔     |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔    | ✔     |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔    | ✔     |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔    | ✗     |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔    | ✔     |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔    | ✔     |
| [ORC](./formats/ORC.md)                                                                                    | ✔    | ✔     |
| [One](./formats/One.md)                                                                                    | ✔    | ✗     |
| [Npy](./formats/Npy.md)                                                                                    | ✔    | ✔     |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔    | ✔     |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔    | ✔     |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔    | ✔     |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔    | ✗     |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔    | ✗     |
| [Native](./formats/Native.md)                                                                              | ✔    | ✔     |
| [Buffers](./formats/Buffers.md)                                                                            | ✔    | ✔     |
| [Null](./formats/Null.md)                                                                                  | ✗    | ✔     |
| [Hash](./formats/Hash.md)                                                                                  | ✗    | ✔     |
| [XML](./formats/XML.md)                                                                                    | ✗    | ✔     |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔    | ✔     |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔    | ✔     |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗    | ✔     |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗    | ✔     |
| [Regexp](./formats/Regexp.md)                                                                              | ✔    | ✗     |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔    | ✔     |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔    | ✔     |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔    | ✗     |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔    | ✔     |
| [DWARF](./formats/DWARF.md)                                                                                | ✔    | ✗     |
| [Markdown](./formats/Markdown.md)                                                                          | ✗    | ✔     |
| [Form](./formats/Form.md)                                                                                  | ✔    | ✗     |

Вы можете управлять некоторыми параметрами обработки форматов с помощью настроек ClickHouse. Подробнее см. в разделе [Settings](/ru/operations/settings/settings-formats.md).

<div id="formatschema">
  ## Схема формата
</div>

Имя файла, содержащего схему формата, задаётся настройкой `format_schema`.
Эту настройку необходимо задать при использовании одного из форматов `Cap'n Proto` или `Protobuf`.
Схема формата представляет собой сочетание имени файла и имени типа сообщения в этом файле, разделённых двоеточием,
например, `schemafile.proto:MessageType`.
Если файл имеет стандартное расширение для формата (например, `.proto` для `Protobuf`),
его можно опустить, и в этом случае схема формата будет выглядеть так: `schemafile:MessageType`.

Если вы вводите или выводите данные через [клиент](/ru/interfaces/client.md) в интерактивном режиме, имя файла, указанное в схеме формата,
может содержать абсолютный путь или путь относительно текущего каталога клиента.
Если вы используете клиент в [пакетном режиме](/ru/interfaces/client.md/#batch-mode), путь к схеме из соображений безопасности должен быть относительным.

Если вы вводите или выводите данные через [HTTP-интерфейс](/ru/interfaces/http), имя файла, указанное в схеме формата,
должно находиться в каталоге, указанном в [format&#95;schema&#95;path](/ru/operations/server-configuration-parameters/settings.md/#format_schema_path)
в конфигурации сервера.

<div id="skippingerrors">
  ## Пропуск ошибок
</div>

Некоторые форматы, такие как `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` и `Protobuf`, могут пропускать некорректную строку при возникновении ошибки разбора и продолжать разбор с начала следующей строки. См. настройки [input&#95;format&#95;allow&#95;errors&#95;num](/ru/operations/settings/settings-formats.md/#input_format_allow_errors_num) и
[input&#95;format&#95;allow&#95;errors&#95;ratio](/ru/operations/settings/settings-formats.md/#input_format_allow_errors_ratio).
Ограничения:

* При ошибке разбора `JSONEachRow` пропускает все данные до символа новой строки (или EOF), поэтому для корректного подсчета ошибок строки должны разделяться символом `\n`.
* `Template` и `CustomSeparated` используют разделитель после последнего столбца и разделитель между строками, чтобы определить начало следующей строки, поэтому пропуск ошибок работает только если хотя бы один из них не пуст.