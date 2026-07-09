---
description: 'Documentação da família de motores Log'
sidebar_label: 'Família de motores Log'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Família de motores Log'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Família de motores Log
</div>

<CloudNotSupportedBadge />

Esses motores foram desenvolvidos para cenários em que você precisa gravar rapidamente muitas tabelas pequenas (até cerca de 1 milhão de linhas) e depois lê-las como um todo.

Motores da família:

| Motores Log                                                 |
| ----------------------------------------------------------- |
| [StripeLog](/pt-BR/engines/table-engines/log-family/stripelog.md) |
| [Log](/pt-BR/engines/table-engines/log-family/log.md)             |
| [TinyLog](/pt-BR/engines/table-engines/log-family/tinylog.md)     |

Os motores de tabela da família `Log` podem armazenar dados em sistemas de arquivos distribuídos [HDFS](/pt-BR/engines/table-engines/integrations/hdfs) ou [S3](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).

:::warning Este motor não é destinado a dados de log.
Apesar do nome, *os motores de tabela Log não se destinam ao armazenamento de dados de log. Eles devem ser usados apenas para pequenos volumes que precisem ser gravados rapidamente.
:::

<div id="common-properties">
  ## Propriedades comuns
</div>

Motores:

* Armazenam dados em disco.

* Acrescentam dados ao final do arquivo durante a gravação.

* Oferecem suporte a bloqueios para acesso simultâneo aos dados.

  Durante consultas `INSERT`, a tabela é bloqueada, e outras consultas de leitura e gravação de dados aguardam até que a tabela seja desbloqueada. Se não houver consultas de gravação de dados, qualquer número de consultas de leitura de dados poderá ser executado simultaneamente.

* Não oferecem suporte a [mutações](/pt-BR/sql-reference/statements/alter#mutations).

* Não oferecem suporte a índices.

  Isso significa que consultas `SELECT` para intervalos de dados não são eficientes.

* Não gravam dados de forma atômica.

  Você pode acabar com uma tabela com dados corrompidos se algo interromper a operação de gravação, por exemplo, um desligamento anormal do servidor.

<div id="differences">
  ## Diferenças
</div>

O motor `TinyLog` é o mais simples da família e oferece a funcionalidade mais limitada e a menor eficiência. O motor `TinyLog` não oferece suporte à leitura paralela de dados por várias threads em uma única consulta. Ele lê os dados mais lentamente do que outros motores da família que oferecem suporte à leitura paralela em uma única consulta e usa quase tantos descritores de arquivo quanto o motor `Log`, porque armazena cada coluna em um arquivo separado. Use-o apenas em cenários simples.

Os motores `Log` e `StripeLog` oferecem suporte à leitura paralela de dados. Ao ler os dados, o ClickHouse usa várias threads. Cada thread processa um bloco de dados separado. O motor `Log` usa um arquivo separado para cada coluna da tabela. O `StripeLog` armazena todos os dados em um único arquivo. Como resultado, o motor `StripeLog` usa menos descritores de arquivo, mas o motor `Log` oferece maior eficiência na leitura dos dados.