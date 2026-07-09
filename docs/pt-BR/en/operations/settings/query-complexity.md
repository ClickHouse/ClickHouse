---
description: 'Configurações que restringem a complexidade da consulta.'
sidebar_label: 'Restrições de complexidade da consulta'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'Restrições de complexidade da consulta'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

Como parte das [configurações](/pt-BR/operations/settings/overview), o ClickHouse oferece
a possibilidade de impor restrições à complexidade das consultas. Isso ajuda a evitar
consultas potencialmente muito intensivas em recursos, garantindo uma execução
mais segura e previsível, principalmente ao usar a interface do usuário.

Quase todas as restrições se aplicam apenas a consultas `SELECT` e, no processamento
distribuído de consultas, são aplicadas separadamente em cada servidor.

Em geral, o ClickHouse verifica as restrições somente depois que as partes de dados
são totalmente processadas, em vez de verificá-las para cada linha. Isso pode
resultar em uma situação em que as restrições sejam violadas enquanto a parte está sendo
processada.

<div id="overflow_mode_setting">
  ## Configurações de `overflow_mode`
</div>

A maioria das restrições também tem uma configuração `overflow_mode`, que define o que acontece
quando o limite é excedido e pode assumir um de dois valores:

* `throw`: gerar uma exceção (padrão).
* `break`: interromper a execução da consulta e retornar o resultado parcial, como se os
  dados de origem tivessem se esgotado.

<div id="group_by_overflow_mode_settings">
  ## Configurações de `group_by_overflow_mode`
</div>

A configuração `group_by_overflow_mode` também tem
o valor `any`:

* `any` : continua a agregação para as chaves que entraram no conjunto, mas não
  adiciona novas chaves ao conjunto.

<div id="relevant-settings">
  ## Lista de configurações
</div>

As configurações a seguir são usadas para aplicar restrições à complexidade da consulta.

:::note
As restrições sobre a &quot;quantidade máxima de algo&quot; podem ter o valor `0`,
o que significa que são &quot;sem restrição&quot;.
:::

| Configuração                                                                                                           | Descrição breve                                                                                                                                                                |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`max_memory_usage`](/pt-BR/operations/settings/settings#max_memory_usage)                                                   | A quantidade máxima de RAM usada para executar uma consulta em um único servidor.                                                                                              |
| [`max_memory_usage_for_user`](/pt-BR/operations/settings/settings#max_memory_usage_for_user)                                 | A quantidade máxima de RAM usada para executar as consultas de um usuário em um único servidor.                                                                                |
| [`max_rows_to_read`](/pt-BR/operations/settings/settings#max_rows_to_read)                                                   | O número máximo de linhas que podem ser lidas de uma tabela ao executar uma consulta.                                                                                          |
| [`max_bytes_to_read`](/pt-BR/operations/settings/settings#max_bytes_to_read)                                                 | O número máximo de bytes (de dados não comprimidos) que podem ser lidos de uma tabela ao executar uma consulta.                                                                |
| [`read_overflow_mode_leaf`](/pt-BR/operations/settings/settings#read_overflow_mode_leaf)                                     | Define o que acontece quando o volume de dados lidos excede um dos limites do leaf.                                                                                            |
| [`max_rows_to_read_leaf`](/pt-BR/operations/settings/settings#max_rows_to_read_leaf)                                         | O número máximo de linhas que podem ser lidas de uma tabela local em um leaf node ao executar uma consulta distribuída.                                                        |
| [`max_bytes_to_read_leaf`](/pt-BR/operations/settings/settings#max_bytes_to_read_leaf)                                       | O número máximo de bytes (de dados não comprimidos) que podem ser lidos de uma tabela local em um leaf node ao executar uma consulta distribuída.                              |
| [`read_overflow_mode_leaf`](/pt-BR/docs/operations/settings/settings#read_overflow_mode_leaf)                                | Define o que acontece quando o volume de dados lidos excede um dos limites do leaf.                                                                                            |
| [`max_rows_to_group_by`](/pt-BR/operations/settings/settings#max_rows_to_group_by)                                           | O número máximo de chaves exclusivas recebidas da agregação.                                                                                                                   |
| [`group_by_overflow_mode`](/pt-BR/operations/settings/settings#group_by_overflow_mode)                                       | Define o que acontece quando o número de chaves exclusivas para agregação excede o limite.                                                                                     |
| [`max_bytes_before_external_group_by`](/pt-BR/operations/settings/settings#max_bytes_before_external_group_by)               | Habilita ou desabilita a execução de cláusulas `GROUP BY` em memória externa.                                                                                                  |
| [`max_bytes_ratio_before_external_group_by`](/pt-BR/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | A razão da memória disponível permitida para `GROUP BY`. Ao atingir esse valor, a memória externa é usada para agregação.                                                      |
| [`max_bytes_before_external_sort`](/pt-BR/operations/settings/settings#max_bytes_before_external_sort)                       | Habilita ou desabilita a execução de cláusulas `ORDER BY` em memória externa.                                                                                                  |
| [`max_bytes_ratio_before_external_sort`](/pt-BR/operations/settings/settings#max_bytes_ratio_before_external_sort)           | A razão da memória disponível permitida para `ORDER BY`. Ao atingir esse valor, a ordenação externa é usada.                                                                   |
| [`max_rows_to_sort`](/pt-BR/operations/settings/settings#max_rows_to_sort)                                                   | O número máximo de linhas antes da ordenação. Permite limitar o consumo de memória durante a ordenação.                                                                        |
| [`max_bytes_to_sort`](/pt-BR/operations/settings/settings#max_rows_to_sort)                                                  | O número máximo de bytes antes da ordenação.                                                                                                                                   |
| [`sort_overflow_mode`](/pt-BR/operations/settings/settings#sort_overflow_mode)                                               | Define o que acontece se o número de linhas recebidas antes da ordenação exceder um dos limites.                                                                               |
| [`max_result_rows`](/pt-BR/operations/settings/settings#max_result_rows)                                                     | Limita o número de linhas no resultado.                                                                                                                                        |
| [`max_result_bytes`](/pt-BR/operations/settings/settings#max_result_bytes)                                                   | Limita o tamanho do resultado em bytes (não comprimidos).                                                                                                                      |
| [`result_overflow_mode`](/pt-BR/operations/settings/settings#result_overflow_mode)                                           | Define o que fazer se o volume do resultado exceder um dos limites.                                                                                                            |
| [`max_execution_time`](/pt-BR/operations/settings/settings#max_execution_time)                                               | O tempo máximo de execução da consulta, em segundos.                                                                                                                           |
| [`timeout_overflow_mode`](/pt-BR/operations/settings/settings#timeout_overflow_mode)                                         | Define o que fazer se a consulta levar mais tempo que `max_execution_time` ou se o tempo estimado de execução for maior que `max_estimated_execution_time`.                    |
| [`max_execution_time_leaf`](/pt-BR/operations/settings/settings#max_execution_time_leaf)                                     | Semanticamente semelhante a `max_execution_time`, mas aplicado apenas em leaf nodes para consultas distribuídas ou remotas.                                                    |
| [`timeout_overflow_mode_leaf`](/pt-BR/operations/settings/settings#timeout_overflow_mode_leaf)                               | Define o que acontece quando a consulta em um leaf node é executada por mais tempo que `max_execution_time_leaf`.                                                              |
| [`min_execution_speed`](/pt-BR/operations/settings/settings#min_execution_speed)                                             | Velocidade mínima de execução em linhas por segundo.                                                                                                                           |
| [`min_execution_speed_bytes`](/pt-BR/operations/settings/settings#min_execution_speed_bytes)                                 | O número mínimo de bytes processados por segundo durante a execução.                                                                                                           |
| [`max_execution_speed`](/pt-BR/operations/settings/settings#max_execution_speed)                                             | O número máximo de linhas processadas por segundo durante a execução.                                                                                                          |
| [`max_execution_speed_bytes`](/pt-BR/operations/settings/settings#max_execution_speed_bytes)                                 | O número máximo de bytes processados por segundo durante a execução.                                                                                                           |
| [`timeout_before_checking_execution_speed`](/pt-BR/operations/settings/settings#timeout_before_checking_execution_speed)     | Verifica se a velocidade de execução não está muito baixa (não inferior a `min_execution_speed`) após o tempo especificado, em segundos, expirar.                              |
| [`max_estimated_execution_time`](/pt-BR/operations/settings/settings#max_estimated_execution_time)                           | O tempo máximo estimado de execução da consulta, em segundos.                                                                                                                  |
| [`max_columns_to_read`](/pt-BR/operations/settings/settings#max_columns_to_read)                                             | O número máximo de colunas que podem ser lidas de uma tabela em uma única consulta.                                                                                            |
| [`max_temporary_columns`](/pt-BR/operations/settings/settings#max_temporary_columns)                                         | O número máximo de colunas temporárias que devem ser mantidas na RAM simultaneamente durante a execução de uma consulta, incluindo colunas constantes.                         |
| [`max_temporary_non_const_columns`](/pt-BR/operations/settings/settings#max_temporary_non_const_columns)                     | O número máximo de colunas temporárias que devem ser mantidas na RAM simultaneamente durante a execução de uma consulta, sem contar as colunas constantes.                     |
| [`max_subquery_depth`](/pt-BR/operations/settings/settings#max_subquery_depth)                                               | Define o que acontece se uma consulta tiver mais subconsultas aninhadas do que o número especificado.                                                                          |
| [`max_ast_depth`](/pt-BR/operations/settings/settings#max_ast_depth)                                                         | A profundidade máxima de aninhamento da árvore sintática de uma consulta.                                                                                                      |
| [`max_ast_elements`](/pt-BR/operations/settings/settings#max_ast_elements)                                                   | O número máximo de elementos na árvore sintática de uma consulta.                                                                                                              |
| [`max_rows_in_set`](/pt-BR/operations/settings/settings#max_rows_in_set)                                                     | O número máximo de linhas de um conjunto de dados na cláusula IN criado a partir de uma subconsulta.                                                                           |
| [`max_bytes_in_set`](/pt-BR/operations/settings/settings#max_bytes_in_set)                                                   | O número máximo de bytes (de dados não comprimidos) usados por um conjunto na cláusula IN criado a partir de uma subconsulta.                                                  |
| [`set_overflow_mode`](/pt-BR/operations/settings/settings#max_bytes_in_set)                                                  | Define o que acontece quando a quantidade de dados excede um dos limites.                                                                                                      |
| [`max_rows_in_distinct`](/pt-BR/operations/settings/settings#max_rows_in_distinct)                                           | O número máximo de linhas distintas ao usar DISTINCT.                                                                                                                          |
| [`max_bytes_in_distinct`](/pt-BR/operations/settings/settings#max_bytes_in_distinct)                                         | O número máximo de bytes do estado em memória (em bytes não comprimidos) usado por uma tabela hash ao usar DISTINCT.                                                           |
| [`distinct_overflow_mode`](/pt-BR/operations/settings/settings#distinct_overflow_mode)                                       | Define o que acontece quando a quantidade de dados excede um dos limites.                                                                                                      |
| [`max_rows_to_transfer`](/pt-BR/operations/settings/settings#max_rows_to_transfer)                                           | Tamanho máximo (em linhas) que pode ser transferido para um servidor remoto ou salvo em uma tabela temporária quando a seção GLOBAL IN/JOIN é executada.                       |
| [`max_bytes_to_transfer`](/pt-BR/operations/settings/settings#max_bytes_to_transfer)                                         | O número máximo de bytes (dados não comprimidos) que pode ser transferido para um servidor remoto ou salvo em uma tabela temporária quando a seção GLOBAL IN/JOIN é executada. |
| [`transfer_overflow_mode`](/pt-BR/operations/settings/settings#transfer_overflow_mode)                                       | Define o que acontece quando a quantidade de dados excede um dos limites.                                                                                                      |
| [`max_rows_in_join`](/pt-BR/operations/settings/settings#max_rows_in_join)                                                   | Limita o número de linhas na tabela hash usada ao fazer join de tabelas.                                                                                                       |
| [`max_bytes_in_join`](/pt-BR/operations/settings/settings#max_bytes_in_join)                                                 | O tamanho máximo, em bytes, da tabela hash usada ao fazer join de tabelas.                                                                                                     |
| [`join_overflow_mode`](/pt-BR/operations/settings/settings#join_overflow_mode)                                               | Define qual ação o ClickHouse executa quando qualquer um dos limites de join a seguir é atingido.                                                                              |
| [`max_partitions_per_insert_block`](/pt-BR/operations/settings/settings#max_partitions_per_insert_block)                     | Limita o número máximo de partições em um único bloco inserido, e uma exceção é lançada se o bloco contiver partições demais.                                                  |
| [`throw_on_max_partitions_per_insert_block`](/pt-BR/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | Permite controlar o comportamento quando `max_partitions_per_insert_block` é atingido.                                                                                         |
| [`max_temporary_data_on_disk_size_for_user`](/pt-BR/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | A quantidade máxima de dados consumida por arquivos temporários em disco, em bytes, para todas as consultas do usuário executadas simultaneamente.                             |
| [`max_temporary_data_on_disk_size_for_query`](/pt-BR/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | A quantidade máxima de dados consumida por arquivos temporários em disco, em bytes, para todas as consultas executadas simultaneamente.                                        |
| [`max_sessions_for_user`](/pt-BR/operations/settings/settings#max_sessions_for_user)                                         | Número máximo de sessões simultâneas por usuário autenticado no servidor ClickHouse.                                                                                           |
| [`max_partitions_to_read`](/pt-BR/operations/settings/settings#max_partitions_to_read)                                       | Limita o número máximo de partições que podem ser acessadas em uma única consulta.                                                                                             |

<div id="obsolete-settings">
  ## Configurações obsoletas
</div>

:::note
As configurações a seguir são obsoletas
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

Profundidade máxima do pipeline. Corresponde ao número de transformações pelas quais cada
bloco de dados passa durante o processamento da consulta. O cálculo é feito dentro dos limites de um
único servidor. Se a profundidade do pipeline for maior, uma exceção é lançada.