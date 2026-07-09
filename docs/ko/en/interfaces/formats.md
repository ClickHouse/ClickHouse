---
description: 'ClickHouse에서 입력 및 출력에 지원되는 데이터 포맷 개요'
sidebar_label: '모든 포맷 보기...'
sidebar_position: 21
slug: /interfaces/formats
title: '입력 및 출력용 데이터 포맷'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # 입력 및 출력 데이터 포맷
</div>

ClickHouse는 널리 사용되는 대부분의 텍스트 및 바이너리 데이터 포맷을 지원합니다. 따라서 거의 모든 데이터 파이프라인에 손쉽게 통합하여
ClickHouse의 장점을 활용할 수 있습니다.

<div id="input-formats">
  ## 입력 형식
</div>

입력 형식은 다음과 같은 용도로 사용됩니다:

* `INSERT` SQL 문에 전달되는 데이터 파싱
* `File`, `URL`, `HDFS`와 같은 파일 기반 테이블에서 `SELECT` 쿼리 수행
* 딕셔너리 읽기

ClickHouse에서 효율적인 데이터 수집을 위해서는 적절한 입력 형식을 선택하는 것이 매우 중요합니다. 지원되는 포맷이 70개가 넘기 때문에,
가장 성능이 뛰어난 옵션을 선택하면 삽입 속도, CPU 및 메모리 사용량, 그리고 전체 시스템
효율성에 큰 영향을 줄 수 있습니다. 이러한 선택에 도움이 되도록 포맷별 수집 성능을 벤치마크했으며, 그 결과 다음과 같은 핵심 사항을 확인했습니다:

* **[Native](formats/Native.md) 포맷은 가장 효율적인 입력 형식입니다**. 최고의 압축률, 가장 낮은
  리소스 사용량, 그리고 최소한의 서버 측 처리 오버헤드를 제공합니다.
* **압축은 필수적입니다** - LZ4는 CPU 비용 증가를 최소화하면서 데이터 크기를 줄이고, ZSTD는 더 높은 압축률을 제공하는 대신
  CPU 사용량이 더 늘어납니다.
* **사전 정렬의 영향은 중간 정도입니다**, ClickHouse가 이미 효율적으로 정렬하기 때문입니다.
* **배칭은 효율성을 크게 높입니다** - 더 큰 배치는 삽입 오버헤드를 줄이고 처리량을 개선합니다.

결과와 모범 사례를 자세히 알아보려면
전체 [벤치마크 분석](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)을 읽어보십시오.
전체 테스트 결과는 [FastFormats](https://fastformats.clickhouse.com/) 온라인 대시보드에서 확인하십시오.

<div id="output-formats">
  ## 출력 형식
</div>

출력용으로 지원되는 포맷은 다음과 같은 용도로 사용됩니다:

* `SELECT` 쿼리 결과를 구성하는 데
* 파일 기반 테이블에 `INSERT` 작업을 수행하는 데

<div id="formats-overview">
  ## 포맷 개요
</div>

지원되는 포맷은 다음과 같습니다:

| 포맷                                                                                                         | 입력 | 출력 |
| ---------------------------------------------------------------------------------------------------------- | -- | -- |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔  | ✔  |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔  | ✔  |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔  | ✔  |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔  | ✔  |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔  | ✔  |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔  | ✔  |
| [Template](./formats/Template/Template.md)                                                                 | ✔  | ✔  |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔  | ✗  |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔  | ✔  |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔  | ✔  |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔  | ✔  |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔  | ✔  |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔  | ✔  |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔  | ✔  |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗  | ✔  |
| [Values](./formats/Values.md)                                                                              | ✔  | ✔  |
| [Vertical](./formats/Vertical.md)                                                                          | ✗  | ✔  |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔  | ✔  |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔  | ✗  |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔  | ✗  |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗  | ✔  |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔  | ✔  |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔  | ✔  |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔  | ✔  |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗  | ✔  |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔  | ✔  |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔  | ✔  |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗  | ✔  |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗  | ✔  |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔  | ✔  |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗  | ✔  |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔  | ✔  |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔  | ✔  |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔  | ✔  |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗  | ✔  |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔  | ✔  |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔  | ✔  |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔  | ✔  |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗  | ✔  |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔  | ✔  |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔  | ✔  |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔  | ✔  |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗  | ✔  |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗  | ✔  |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗  | ✔  |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗  | ✔  |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗  | ✔  |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗  | ✔  |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗  | ✔  |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗  | ✔  |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗  | ✔  |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗  | ✔  |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗  | ✔  |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗  | ✔  |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗  | ✔  |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔  | ✔  |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔  | ✔  |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔  | ✔  |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔  | ✔  |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔  | ✔  |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔  | ✔  |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔  | ✗  |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔  | ✔  |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔  | ✔  |
| [ORC](./formats/ORC.md)                                                                                    | ✔  | ✔  |
| [One](./formats/One.md)                                                                                    | ✔  | ✗  |
| [Npy](./formats/Npy.md)                                                                                    | ✔  | ✔  |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔  | ✔  |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔  | ✔  |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔  | ✔  |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔  | ✗  |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔  | ✗  |
| [Native](./formats/Native.md)                                                                              | ✔  | ✔  |
| [Buffers](./formats/Buffers.md)                                                                            | ✔  | ✔  |
| [Null](./formats/Null.md)                                                                                  | ✗  | ✔  |
| [Hash](./formats/Hash.md)                                                                                  | ✗  | ✔  |
| [XML](./formats/XML.md)                                                                                    | ✗  | ✔  |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔  | ✔  |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔  | ✔  |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗  | ✔  |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗  | ✔  |
| [Regexp](./formats/Regexp.md)                                                                              | ✔  | ✗  |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔  | ✔  |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔  | ✔  |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔  | ✗  |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔  | ✔  |
| [DWARF](./formats/DWARF.md)                                                                                | ✔  | ✗  |
| [Markdown](./formats/Markdown.md)                                                                          | ✗  | ✔  |
| [Form](./formats/Form.md)                                                                                  | ✔  | ✗  |

ClickHouse 설정을 사용하여 일부 포맷 처리 매개변수를 제어할 수 있습니다. 자세한 내용은 [설정](/ko/operations/settings/settings-formats.md) 섹션을 참조하십시오.

<div id="formatschema">
  ## 포맷 스키마
</div>

포맷 스키마가 들어 있는 파일 이름은 `format_schema` 설정으로 지정합니다.
`Cap'n Proto` 및 `Protobuf` 포맷 중 하나를 사용할 때는 이 설정을 반드시 지정해야 합니다.
포맷 스키마는 파일 이름과 해당 파일의 메시지 유형 이름을 콜론으로 구분해 결합한 형태이며,
예를 들면 `schemafile.proto:MessageType`입니다.
파일에 해당 포맷의 표준 확장자(예: `Protobuf`의 `.proto`)가 있으면
확장자를 생략할 수 있으며, 이 경우 포맷 스키마는 `schemafile:MessageType` 형태가 됩니다.

대화형 모드에서 [클라이언트](/ko/interfaces/client.md)를 통해 데이터를 입력하거나 출력하는 경우, 포맷 스키마에 지정된 파일 이름에는
절대 경로나 클라이언트의 현재 디렉터리를 기준으로 하는 상대 경로를 사용할 수 있습니다.
[배치 모드](/ko/interfaces/client.md/#batch-mode)에서 클라이언트를 사용하는 경우에는 보안상의 이유로 스키마 경로가 상대 경로여야 합니다.

[HTTP 인터페이스](/ko/interfaces/http)를 통해 데이터를 입력하거나 출력하는 경우, 포맷 스키마에 지정된 파일 이름은
서버 구성의 [format&#95;schema&#95;path](/ko/operations/server-configuration-parameters/settings.md/#format_schema_path)에서 지정한 디렉터리에
있어야 합니다.

<div id="skippingerrors">
  ## 오류 건너뛰기
</div>

`CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated`, `Protobuf`와 같은 일부 포맷은 파싱 오류가 발생하면 손상된 행을 건너뛰고 다음 행의 시작부터 파싱을 계속할 수 있습니다. [input&#95;format&#95;allow&#95;errors&#95;num](/ko/operations/settings/settings-formats.md/#input_format_allow_errors_num) 및
[input&#95;format&#95;allow&#95;errors&#95;ratio](/ko/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) 설정을 참조하십시오.
제한 사항:

* 파싱 오류가 발생하면 `JSONEachRow`는 새 줄(또는 EOF)까지의 모든 데이터를 건너뛰므로, 오류를 정확하게 계산하려면 행이 `\n`으로 구분되어야 합니다.
* `Template`와 `CustomSeparated`는 다음 행의 시작을 찾기 위해 마지막 컬럼 뒤의 구분자와 행 사이의 구분자를 사용하므로, 오류 건너뛰기는 이 둘 중 적어도 하나가 비어 있지 않은 경우에만 동작합니다.