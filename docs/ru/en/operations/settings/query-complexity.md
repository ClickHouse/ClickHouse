---
description: 'Настройки, ограничивающие сложность запросов.'
sidebar_label: 'Ограничения на сложность запросов'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'Ограничения на сложность запросов'
doc_type: 'reference'
---

<div id="overview">
  ## Обзор
</div>

В рамках [настроек](/ru/operations/settings/overview) ClickHouse позволяет
устанавливать ограничения на сложность запросов. Это помогает защититься от
потенциально требовательных к ресурсам запросов, обеспечивая более безопасное и предсказуемое
выполнение, особенно при использовании пользовательского интерфейса.

Почти все ограничения применяются только к запросам `SELECT`, а при распределённой
обработке запросов — отдельно на каждом сервере.

Как правило, ClickHouse проверяет ограничения только после полной обработки
частей данных, а не для каждой строки. Это может привести к ситуации,
когда ограничения нарушаются ещё в процессе обработки
части.

<div id="overflow_mode_setting">
  ## Настройки `overflow_mode`
</div>

У большинства ограничений также есть настройка `overflow_mode`, которая определяет, что происходит при превышении лимита, и может принимать одно из двух значений:

* `throw`: сгенерировать исключение (по умолчанию).
* `break`: остановить выполнение запроса и вернуть частичный результат, как если бы
  исходные данные закончились.

<div id="group_by_overflow_mode_settings">
  ## Настройки `group_by_overflow_mode`
</div>

У настройки `group_by_overflow_mode` также есть
значение `any`:

* `any` : продолжать агрегацию для ключей, которые попали в набор, но не
  добавлять в набор новые ключи.

<div id="relevant-settings">
  ## Список настроек
</div>

Следующие настройки используются для задания ограничений на сложность запросов.

:::note
Ограничения на «максимальное количество чего-либо» могут принимать значение `0`,
что означает «без ограничений».
:::

| Параметр                                                                                                               | Краткое описание                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`max_memory_usage`](/ru/operations/settings/settings#max_memory_usage)                                                   | Максимальный объем оперативной памяти, используемый для выполнения запроса на одном сервере.                                                                          |
| [`max_memory_usage_for_user`](/ru/operations/settings/settings#max_memory_usage_for_user)                                 | Максимальный объем оперативной памяти, используемый для выполнения запросов пользователя на одном сервере.                                                            |
| [`max_rows_to_read`](/ru/operations/settings/settings#max_rows_to_read)                                                   | Максимальное количество строк, которое можно прочитать из таблицы при выполнении запроса.                                                                             |
| [`max_bytes_to_read`](/ru/operations/settings/settings#max_bytes_to_read)                                                 | Максимальное количество байтов (несжатых данных), которое можно прочитать из таблицы при выполнении запроса.                                                          |
| [`read_overflow_mode_leaf`](/ru/operations/settings/settings#read_overflow_mode_leaf)                                     | Определяет, что происходит, когда объем прочитанных данных превышает один из лимитов листового узла.                                                                  |
| [`max_rows_to_read_leaf`](/ru/operations/settings/settings#max_rows_to_read_leaf)                                         | Максимальное количество строк, которое можно прочитать из локальной таблицы на листовом узле при выполнении распределенного запроса.                                  |
| [`max_bytes_to_read_leaf`](/ru/operations/settings/settings#max_bytes_to_read_leaf)                                       | Максимальное количество байтов (несжатых данных), которое можно прочитать из локальной таблицы на листовом узле при выполнении распределенного запроса.               |
| [`read_overflow_mode_leaf`](/ru/docs/operations/settings/settings#read_overflow_mode_leaf)                                | Определяет, что происходит, когда объем прочитанных данных превышает один из лимитов листового узла.                                                                  |
| [`max_rows_to_group_by`](/ru/operations/settings/settings#max_rows_to_group_by)                                           | Максимальное количество уникальных ключей, полученных при агрегации.                                                                                                  |
| [`group_by_overflow_mode`](/ru/operations/settings/settings#group_by_overflow_mode)                                       | Определяет, что происходит, когда количество уникальных ключей для агрегации превышает лимит.                                                                         |
| [`max_bytes_before_external_group_by`](/ru/operations/settings/settings#max_bytes_before_external_group_by)               | Включает или отключает выполнение `GROUP BY` с использованием внешней памяти.                                                                                         |
| [`max_bytes_ratio_before_external_group_by`](/ru/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | Доля доступной памяти, которую можно использовать для `GROUP BY`. После достижения этого значения для агрегации используется внешняя память.                          |
| [`max_bytes_before_external_sort`](/ru/operations/settings/settings#max_bytes_before_external_sort)                       | Включает или отключает выполнение `ORDER BY` с использованием внешней памяти.                                                                                         |
| [`max_bytes_ratio_before_external_sort`](/ru/operations/settings/settings#max_bytes_ratio_before_external_sort)           | Доля доступной памяти, которую можно использовать для `ORDER BY`. После достижения этого значения используется внешняя сортировка.                                    |
| [`max_rows_to_sort`](/ru/operations/settings/settings#max_rows_to_sort)                                                   | Максимальное количество строк перед сортировкой. Позволяет ограничить потребление памяти при сортировке.                                                              |
| [`max_bytes_to_sort`](/ru/operations/settings/settings#max_rows_to_sort)                                                  | Максимальное количество байтов перед сортировкой.                                                                                                                     |
| [`sort_overflow_mode`](/ru/operations/settings/settings#sort_overflow_mode)                                               | Определяет, что происходит, если количество строк, полученных перед сортировкой, превышает один из лимитов.                                                           |
| [`max_result_rows`](/ru/operations/settings/settings#max_result_rows)                                                     | Ограничивает количество строк в результате.                                                                                                                           |
| [`max_result_bytes`](/ru/operations/settings/settings#max_result_bytes)                                                   | Ограничивает размер результата в байтах (несжатых).                                                                                                                   |
| [`result_overflow_mode`](/ru/operations/settings/settings#result_overflow_mode)                                           | Определяет, что делать, если объем результата превышает один из лимитов.                                                                                              |
| [`max_execution_time`](/ru/operations/settings/settings#max_execution_time)                                               | Максимальное время выполнения запроса в секундах.                                                                                                                     |
| [`timeout_overflow_mode`](/ru/operations/settings/settings#timeout_overflow_mode)                                         | Определяет, что делать, если запрос выполняется дольше, чем `max_execution_time`, или расчетное время выполнения превышает `max_estimated_execution_time`.            |
| [`max_execution_time_leaf`](/ru/operations/settings/settings#max_execution_time_leaf)                                     | По смыслу аналогичен `max_execution_time`, но применяется только на листовых узлах для распределенных или удаленных запросов.                                         |
| [`timeout_overflow_mode_leaf`](/ru/operations/settings/settings#timeout_overflow_mode_leaf)                               | Определяет, что происходит, когда запрос на листовом узле выполняется дольше, чем `max_execution_time_leaf`.                                                          |
| [`min_execution_speed`](/ru/operations/settings/settings#min_execution_speed)                                             | Минимальная скорость выполнения в строках в секунду.                                                                                                                  |
| [`min_execution_speed_bytes`](/ru/operations/settings/settings#min_execution_speed_bytes)                                 | Минимальное количество байтов в секунду.                                                                                                                              |
| [`max_execution_speed`](/ru/operations/settings/settings#max_execution_speed)                                             | Максимальное количество строк в секунду.                                                                                                                              |
| [`max_execution_speed_bytes`](/ru/operations/settings/settings#max_execution_speed_bytes)                                 | Максимальное количество байтов в секунду.                                                                                                                             |
| [`timeout_before_checking_execution_speed`](/ru/operations/settings/settings#timeout_before_checking_execution_speed)     | Проверяет, что скорость выполнения не слишком низкая (не меньше `min_execution_speed`), после истечения указанного времени в секундах.                                |
| [`max_estimated_execution_time`](/ru/operations/settings/settings#max_estimated_execution_time)                           | Максимальное расчетное время выполнения запроса в секундах.                                                                                                           |
| [`max_columns_to_read`](/ru/operations/settings/settings#max_columns_to_read)                                             | Максимальное количество столбцов, которое можно прочитать из таблицы в одном запросе.                                                                                 |
| [`max_temporary_columns`](/ru/operations/settings/settings#max_temporary_columns)                                         | Максимальное количество временных столбцов, которые должны одновременно находиться в оперативной памяти при выполнении запроса, включая константные столбцы.          |
| [`max_temporary_non_const_columns`](/ru/operations/settings/settings#max_temporary_non_const_columns)                     | Максимальное количество временных столбцов, которые должны одновременно находиться в оперативной памяти при выполнении запроса, без учета константных столбцов.       |
| [`max_subquery_depth`](/ru/operations/settings/settings#max_subquery_depth)                                               | Задает, что происходит, если запрос содержит больше указанного числа вложенных подзапросов.                                                                           |
| [`max_ast_depth`](/ru/operations/settings/settings#max_ast_depth)                                                         | Максимальная глубина вложенности синтаксического дерева запроса.                                                                                                      |
| [`max_ast_elements`](/ru/operations/settings/settings#max_ast_elements)                                                   | Максимальное количество элементов в синтаксическом дереве запроса.                                                                                                    |
| [`max_rows_in_set`](/ru/operations/settings/settings#max_rows_in_set)                                                     | Максимальное количество строк в наборе данных в условии IN, созданном из подзапроса.                                                                                  |
| [`max_bytes_in_set`](/ru/operations/settings/settings#max_bytes_in_set)                                                   | Максимальное количество байтов (несжатых данных), используемых множеством в условии IN, созданном из подзапроса.                                                      |
| [`set_overflow_mode`](/ru/operations/settings/settings#max_bytes_in_set)                                                  | Задает, что происходит, когда объем данных превышает одно из ограничений.                                                                                             |
| [`max_rows_in_distinct`](/ru/operations/settings/settings#max_rows_in_distinct)                                           | Максимальное количество различных строк при использовании DISTINCT.                                                                                                   |
| [`max_bytes_in_distinct`](/ru/operations/settings/settings#max_bytes_in_distinct)                                         | Максимальный размер состояния в памяти в байтах (несжатых байтах), используемого хеш-таблицей при DISTINCT.                                                           |
| [`distinct_overflow_mode`](/ru/operations/settings/settings#distinct_overflow_mode)                                       | Задает, что происходит, когда объем данных превышает одно из ограничений.                                                                                             |
| [`max_rows_to_transfer`](/ru/operations/settings/settings#max_rows_to_transfer)                                           | Максимальный размер (в строках), который можно передать на удаленный сервер или сохранить во временной таблице при выполнении секции GLOBAL IN/JOIN.                  |
| [`max_bytes_to_transfer`](/ru/operations/settings/settings#max_bytes_to_transfer)                                         | Максимальное количество байтов (несжатых данных), которое можно передать на удаленный сервер или сохранить во временной таблице при выполнении секции GLOBAL IN/JOIN. |
| [`transfer_overflow_mode`](/ru/operations/settings/settings#transfer_overflow_mode)                                       | Задает, что происходит, когда объем данных превышает одно из ограничений.                                                                                             |
| [`max_rows_in_join`](/ru/operations/settings/settings#max_rows_in_join)                                                   | Ограничивает количество строк в хеш-таблице, которая используется при JOIN таблиц.                                                                                    |
| [`max_bytes_in_join`](/ru/operations/settings/settings#max_bytes_in_join)                                                 | Максимальный размер хеш-таблицы в байтах, используемой при JOIN таблиц.                                                                                               |
| [`join_overflow_mode`](/ru/operations/settings/settings#join_overflow_mode)                                               | Определяет, какое действие ClickHouse выполняет при достижении любого из следующих ограничений JOIN.                                                                  |
| [`max_partitions_per_insert_block`](/ru/operations/settings/settings#max_partitions_per_insert_block)                     | Ограничивает максимальное количество партиций в одном вставляемом блоке; если блок содержит слишком много партиций, генерируется исключение.                          |
| [`throw_on_max_partitions_per_insert_block`](/ru/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | Позволяет управлять поведением при достижении `max_partitions_per_insert_block`.                                                                                      |
| [`max_temporary_data_on_disk_size_for_user`](/ru/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | Максимальный объем данных во временных файлах на диске в байтах для всех одновременно выполняемых пользовательских запросов.                                          |
| [`max_temporary_data_on_disk_size_for_query`](/ru/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | Максимальный объем данных во временных файлах на диске в байтах для всех одновременно выполняемых запросов.                                                           |
| [`max_sessions_for_user`](/ru/operations/settings/settings#max_sessions_for_user)                                         | Максимальное количество одновременных сеансов для каждого аутентифицированного пользователя ClickHouse server.                                                        |
| [`max_partitions_to_read`](/ru/operations/settings/settings#max_partitions_to_read)                                       | Ограничивает максимальное количество партиций, к которым можно обратиться в одном запросе.                                                                            |

<div id="obsolete-settings">
  ## Устаревшие настройки
</div>

:::note
Следующие настройки устарели
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

Максимальная глубина конвейера. Соответствует числу преобразований, через которые
проходит каждый блок данных при обработке запроса. Учитывается в пределах
одного сервера. Если глубина конвейера превышает это значение, генерируется исключение.