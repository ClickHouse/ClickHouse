---
description: 'Visão geral dos formatos de dados suportados para entrada e saída no ClickHouse'
sidebar_label: 'Ver todos os formatos...'
sidebar_position: 21
slug: /interfaces/formats
title: 'Formatos de dados de entrada e saída'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # Formatos para dados de entrada e saída
</div>

O ClickHouse oferece suporte à maioria dos formatos conhecidos de texto e dados binários. Isso permite fácil integração com praticamente qualquer pipeline de dados existente
para aproveitar os benefícios do ClickHouse.

<div id="input-formats">
  ## Formatos de entrada
</div>

Os formatos de entrada são usados para:

* Fazer o parsing dos dados fornecidos a instruções `INSERT`
* Executar consultas `SELECT` em tabelas baseadas em arquivo, como `File`, `URL` ou `HDFS`
* Ler dicionários

Escolher o formato de entrada certo é crucial para uma ingestão de dados eficiente no ClickHouse. Com mais de 70 formatos compatíveis,
selecionar a opção de melhor desempenho pode afetar significativamente a velocidade de inserção, o uso de CPU e memória e a eficiência geral
do sistema. Para ajudar a orientar essas escolhas, fizemos um benchmark do desempenho de ingestão entre os formatos, destacando os principais pontos:

* **O formato [Native](formats/Native.md) é o formato de entrada mais eficiente**, oferecendo a melhor compressão, o menor
  uso de recursos e a menor sobrecarga de processamento no servidor.
* **A compressão é essencial** - o LZ4 reduz o tamanho dos dados com custo mínimo de CPU, enquanto o ZSTD oferece maior compressão ao
  custo de uso adicional de CPU.
* **A pré-ordenação tem impacto moderado**, já que o ClickHouse já ordena com eficiência.
* **O processamento em lotes melhora significativamente a eficiência** - lotes maiores reduzem a sobrecarga de inserção e aumentam a vazão.

Para uma análise detalhada dos resultados e das melhores práticas,
leia a [análise completa do benchmark](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient).
Para ver os resultados completos do teste, explore o [dashboard online FastFormats](https://fastformats.clickhouse.com/).

<div id="output-formats">
  ## Formatos de saída
</div>

Os formatos de saída compatíveis são usados para:

* Organizar os resultados de uma consulta `SELECT`
* Executar operações `INSERT` em tabelas baseadas em arquivos

<div id="formats-overview">
  ## Visão geral dos formatos
</div>

Os formatos suportados são:

| Formato                                                                                                    | Entrada | Saída |
| ---------------------------------------------------------------------------------------------------------- | ------- | ----- |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔       | ✔     |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔       | ✔     |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔       | ✔     |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔       | ✔     |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔       | ✔     |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔       | ✔     |
| [Template](./formats/Template/Template.md)                                                                 | ✔       | ✔     |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔       | ✗     |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔       | ✔     |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔       | ✔     |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔       | ✔     |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔       | ✔     |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔       | ✔     |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔       | ✔     |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗       | ✔     |
| [Values](./formats/Values.md)                                                                              | ✔       | ✔     |
| [Vertical](./formats/Vertical.md)                                                                          | ✗       | ✔     |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔       | ✔     |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔       | ✗     |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔       | ✗     |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗       | ✔     |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔       | ✔     |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔       | ✔     |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔       | ✔     |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗       | ✔     |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔       | ✔     |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔       | ✔     |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗       | ✔     |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗       | ✔     |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔       | ✔     |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗       | ✔     |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔       | ✔     |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔       | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔       | ✔     |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗       | ✔     |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔       | ✔     |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔       | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔       | ✔     |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗       | ✔     |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔       | ✔     |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔       | ✔     |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔       | ✔     |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗       | ✔     |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗       | ✔     |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗       | ✔     |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗       | ✔     |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗       | ✔     |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗       | ✔     |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗       | ✔     |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗       | ✔     |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗       | ✔     |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗       | ✔     |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗       | ✔     |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗       | ✔     |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗       | ✔     |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔       | ✔     |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔       | ✔     |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔       | ✔     |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔       | ✔     |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔       | ✔     |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔       | ✔     |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔       | ✗     |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔       | ✔     |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔       | ✔     |
| [ORC](./formats/ORC.md)                                                                                    | ✔       | ✔     |
| [One](./formats/One.md)                                                                                    | ✔       | ✗     |
| [Npy](./formats/Npy.md)                                                                                    | ✔       | ✔     |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔       | ✔     |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔       | ✔     |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔       | ✔     |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔       | ✗     |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔       | ✗     |
| [Native](./formats/Native.md)                                                                              | ✔       | ✔     |
| [Buffers](./formats/Buffers.md)                                                                            | ✔       | ✔     |
| [Null](./formats/Null.md)                                                                                  | ✗       | ✔     |
| [Hash](./formats/Hash.md)                                                                                  | ✗       | ✔     |
| [XML](./formats/XML.md)                                                                                    | ✗       | ✔     |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔       | ✔     |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔       | ✔     |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗       | ✔     |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗       | ✔     |
| [Regexp](./formats/Regexp.md)                                                                              | ✔       | ✗     |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔       | ✔     |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔       | ✔     |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔       | ✗     |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔       | ✔     |
| [DWARF](./formats/DWARF.md)                                                                                | ✔       | ✗     |
| [Markdown](./formats/Markdown.md)                                                                          | ✗       | ✔     |
| [Form](./formats/Form.md)                                                                                  | ✔       | ✗     |

Você pode controlar alguns parâmetros de processamento de formato com as configurações do ClickHouse. Para obter mais informações, consulte a seção [Configurações](/pt-BR/operations/settings/settings-formats.md).

<div id="formatschema">
  ## Esquema de formato
</div>

O nome do arquivo que contém o esquema do formato é definido pela configuração `format_schema`.
É necessário definir essa configuração ao usar um dos formatos `Cap'n Proto` e `Protobuf`.
O esquema do formato é uma combinação do nome de um arquivo com o nome de um tipo de mensagem nesse arquivo, separados por dois-pontos,
por exemplo, `schemafile.proto:MessageType`.
Se o arquivo tiver a extensão padrão do formato (por exemplo, `.proto` para `Protobuf`),
ela poderá ser omitida e, nesse caso, o esquema do formato ficará como `schemafile:MessageType`.

Se você fizer entrada ou saída de dados por meio do [cliente](/pt-BR/interfaces/client.md) no modo interativo, o nome do arquivo especificado no esquema do formato
pode conter um caminho absoluto ou um caminho relativo ao diretório atual no cliente.
Se você usar o cliente no [modo em lote](/pt-BR/interfaces/client.md/#batch-mode), o caminho para o esquema deverá ser relativo por motivos de segurança.

Se você fizer entrada ou saída de dados por meio da [interface HTTP](/pt-BR/interfaces/http), o nome do arquivo especificado no esquema do formato
deve estar localizado no diretório especificado em [format&#95;schema&#95;path](/pt-BR/operations/server-configuration-parameters/settings.md/#format_schema_path)
na configuração do servidor.

<div id="skippingerrors">
  ## Ignorar erros
</div>

Alguns formatos, como `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` e `Protobuf`, podem ignorar uma linha inválida se ocorrer um erro de parsing e continuar o parsing a partir do início da próxima linha. Consulte as configurações [input&#95;format&#95;allow&#95;errors&#95;num](/pt-BR/operations/settings/settings-formats.md/#input_format_allow_errors_num) e
[input&#95;format&#95;allow&#95;errors&#95;ratio](/pt-BR/operations/settings/settings-formats.md/#input_format_allow_errors_ratio).
Limitações:

* Em caso de erro de parsing, `JSONEachRow` ignora todos os dados até a quebra de linha (ou EOF); portanto, as linhas devem ser delimitadas por `\n` para que os erros sejam contados corretamente.
* `Template` e `CustomSeparated` usam o delimitador após a última coluna e o delimitador entre linhas para localizar o início da próxima linha, portanto a funcionalidade de ignorar erros só funciona se pelo menos um deles não estiver vazio.