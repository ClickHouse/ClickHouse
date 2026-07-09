---
description: 'Documentação sobre motores de tabela'
slug: /engines/table-engines/
toc_folder_title: 'Motores de tabela'
toc_priority: 26
toc_title: 'Introdução'
title: 'Motores de tabela'
doc_type: 'reference'
---

O motor de tabela (tipo de tabela) determina:

* Como e onde os dados são armazenados, para onde são gravados e de onde são lidos.
* Quais consultas são suportadas e como.
* O acesso simultâneo aos dados.
* O uso de índices, se houver.
* Se é possível executar solicitações em múltiplas threads.
* Os parâmetros de replicação de dados.

<div id="engine-families">
  ## Famílias de motores
</div>

<div id="mergetree">
  ### MergeTree
</div>

Os motores de tabela mais universais e versáteis para tarefas de alta carga. A característica comum desses motores é a inserção rápida de dados, com posterior processamento em segundo plano. Os motores da família `MergeTree` oferecem suporte à replicação de dados (com versões [Replicated*](/pt-BR/engines/table-engines/mergetree-family/replication) dos motores), ao particionamento, a índices secundários de data-skipping e a outros recursos não suportados por outros motores.

Motores da família:

| Motores MergeTree                                                                                    |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/pt-BR/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/pt-BR/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/pt-BR/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/pt-BR/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/pt-BR/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/pt-BR/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/pt-BR/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

[motores](../../engines/table-engines/log-family/index.md) leves com funcionalidade mínima. São mais eficientes quando você precisa gravar rapidamente muitas tabelas pequenas (até aproximadamente 1 milhão de linhas) e depois lê-las por completo.

Motores da família:

| Motores Log                                              |
| -------------------------------------------------------- |
| [TinyLog](/pt-BR/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/pt-BR/engines/table-engines/log-family/stripelog) |
| [Log](/pt-BR/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### Motores de integração
</div>

Motores para se comunicar com outros sistemas de armazenamento e processamento de dados.

Motores da família:

| Motores de integração                                                           |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### Motores especiais
</div>

Motores desta família:

| Motores especiais                                              |
| -------------------------------------------------------------- |
| [Distributed](/pt-BR/engines/table-engines/special/distributed)      |
| [Dicionário](/pt-BR/engines/table-engines/special/dictionary)        |
| [Merge](/pt-BR/engines/table-engines/special/merge)                  |
| [Executable](/pt-BR/engines/table-engines/special/executable)        |
| [File](/pt-BR/engines/table-engines/special/file)                    |
| [Null](/pt-BR/engines/table-engines/special/null)                    |
| [Set](/pt-BR/engines/table-engines/special/set)                      |
| [Join](/pt-BR/engines/table-engines/special/join)                    |
| [URL](/pt-BR/engines/table-engines/special/url)                      |
| [View](/pt-BR/engines/table-engines/special/view)                    |
| [Memory](/pt-BR/engines/table-engines/special/memory)                |
| [Buffer](/pt-BR/engines/table-engines/special/buffer)                |
| [Dados externos](/pt-BR/engines/table-engines/special/external-data) |
| [GenerateRandom](/pt-BR/engines/table-engines/special/generate)      |
| [KeeperMap](/pt-BR/engines/table-engines/special/keeper-map)         |
| [FileLog](/pt-BR/engines/table-engines/special/filelog)              |

<div id="table_engines-virtual_columns">
  ## Colunas virtuais
</div>

Uma coluna virtual é um atributo inerente do motor de tabela, definido no código-fonte do motor.

Você não deve especificar colunas virtuais na consulta `CREATE TABLE`, e não pode vê-las nos resultados das consultas `SHOW CREATE TABLE` e `DESCRIBE TABLE`. As colunas virtuais também são somente leitura, portanto você não pode inserir dados nelas.

Para selecionar dados de uma coluna virtual, você deve especificar seu nome na consulta `SELECT`. `SELECT *` não retorna valores de colunas virtuais.

Se você criar uma tabela com uma coluna que tenha o mesmo nome de uma das colunas virtuais da tabela, a coluna virtual ficará inacessível. Não recomendamos fazer isso. Para ajudar a evitar conflitos, os nomes das colunas virtuais geralmente recebem um prefixo de sublinhado.

* `_table` — Contém o nome da tabela da qual os dados foram lidos. Tipo: [String](../../sql-reference/data-types/string.md).

  Independentemente do motor de tabela usado, cada tabela inclui uma coluna virtual universal chamada `_table`.

  Ao consultar uma tabela com o motor de tabela Merge, você pode definir condições constantes sobre `_table` na cláusula `WHERE/PREWHERE` (por exemplo, `WHERE _table='xyz'`). Nesse caso, a operação de leitura é realizada apenas para as tabelas em que a condição sobre `_table` é satisfeita, portanto a coluna `_table` atua como um índice.

  Ao usar consultas no formato `SELECT ... FROM (... UNION ALL ...)`, podemos determinar de qual tabela real as linhas retornadas se originam especificando a coluna `_table`.