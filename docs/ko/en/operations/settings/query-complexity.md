---
description: '쿼리 복잡도를 제한하는 설정입니다.'
sidebar_label: '쿼리 복잡도 제한'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: '쿼리 복잡도 제한'
doc_type: 'reference'
---

<div id="overview">
  ## 개요
</div>

ClickHouse는 [설정](/ko/operations/settings/overview)의 일부로 쿼리 복잡도 제한을 설정할 수 있는 기능을 제공합니다. 이를 통해 리소스를 과도하게 사용하는 쿼리를 방지하고, 특히 사용자 인터페이스를 사용할 때 더욱 안전하고 예측 가능한 방식으로 실행되도록 할 수 있습니다.

거의 모든 제한은 `SELECT` 쿼리에만 적용되며, 분산 쿼리 처리에서는 각 서버에 개별적으로 적용됩니다.

ClickHouse는 일반적으로 각 행마다 제한을 확인하지 않고, 데이터 파트가 완전히 처리된 후에만 제한을 확인합니다. 따라서 파트를 처리하는 도중에 제한을 초과하는 상황이 발생할 수 있습니다.

<div id="overflow_mode_setting">
  ## `overflow_mode` 설정
</div>

대부분의 제한에는 `overflow_mode` 설정도 있으며, 이는
한도를 초과했을 때 어떻게 처리할지를 정의합니다. 사용할 수 있는 값은 다음 두 가지입니다:

* `throw`: 예외를 발생시킵니다(기본값).
* `break`: 쿼리 실행을 중지하고 부분 결과를 반환합니다. 마치
  원본 데이터가 소진된 것처럼 동작합니다.

<div id="group_by_overflow_mode_settings">
  ## `group_by_overflow_mode` 설정
</div>

`group_by_overflow_mode` 설정에는
값 `any`도 있습니다:

* `any` : 집합에 이미 들어 있는 키에 대해서는 집계를 계속하지만,
  새 키는 집합에 추가하지 않습니다.

<div id="relevant-settings">
  ## 설정 목록
</div>

다음 설정은 쿼리 복잡도 제한을 적용하는 데 사용됩니다.

:::note
「무언가의 최대값」에 대한 제한에는 `0` 값을 사용할 수 있으며,
이는 「제한 없음」을 의미합니다.
:::

| 설정                                                                                                                     | 간단한 설명                                                                                            |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [`max_memory_usage`](/ko/operations/settings/settings#max_memory_usage)                                                   | 단일 서버에서 쿼리를 실행할 때 사용할 수 있는 RAM의 최대량입니다.                                                           |
| [`max_memory_usage_for_user`](/ko/operations/settings/settings#max_memory_usage_for_user)                                 | 단일 서버에서 사용자 쿼리를 실행할 때 사용할 수 있는 RAM의 최대량입니다.                                                       |
| [`max_rows_to_read`](/ko/operations/settings/settings#max_rows_to_read)                                                   | 쿼리를 실행할 때 테이블에서 읽을 수 있는 최대 행 수입니다.                                                                |
| [`max_bytes_to_read`](/ko/operations/settings/settings#max_bytes_to_read)                                                 | 쿼리를 실행할 때 테이블에서 읽을 수 있는 최대 바이트 수(압축되지 않은 데이터 기준)입니다.                                              |
| [`read_overflow_mode_leaf`](/ko/operations/settings/settings#read_overflow_mode_leaf)                                     | 읽은 데이터 양이 리프 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                          |
| [`max_rows_to_read_leaf`](/ko/operations/settings/settings#max_rows_to_read_leaf)                                         | 분산 쿼리를 실행할 때 리프 노드의 로컬 테이블에서 읽을 수 있는 최대 행 수입니다.                                                   |
| [`max_bytes_to_read_leaf`](/ko/operations/settings/settings#max_bytes_to_read_leaf)                                       | 분산 쿼리를 실행할 때 리프 노드의 로컬 테이블에서 읽을 수 있는 최대 바이트 수(압축되지 않은 데이터 기준)입니다.                                 |
| [`read_overflow_mode_leaf`](/ko/docs/operations/settings/settings#read_overflow_mode_leaf)                                | 읽은 데이터 양이 리프 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                          |
| [`max_rows_to_group_by`](/ko/operations/settings/settings#max_rows_to_group_by)                                           | 집계 결과 생성되는 고유 키의 최대 개수입니다.                                                                        |
| [`group_by_overflow_mode`](/ko/operations/settings/settings#group_by_overflow_mode)                                       | 집계를 위한 고유 키 수가 제한을 초과할 때의 동작을 설정합니다.                                                              |
| [`max_bytes_before_external_group_by`](/ko/operations/settings/settings#max_bytes_before_external_group_by)               | 외부 메모리에서 `GROUP BY` 절을 실행할지 여부를 설정합니다.                                                            |
| [`max_bytes_ratio_before_external_group_by`](/ko/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | `GROUP BY`에 사용할 수 있는 가용 메모리 비율입니다. 이 한도에 도달하면 집계에 외부 메모리를 사용합니다.                                  |
| [`max_bytes_before_external_sort`](/ko/operations/settings/settings#max_bytes_before_external_sort)                       | 외부 메모리에서 `ORDER BY` 절을 실행할지 여부를 설정합니다.                                                            |
| [`max_bytes_ratio_before_external_sort`](/ko/operations/settings/settings#max_bytes_ratio_before_external_sort)           | `ORDER BY`에 사용할 수 있는 가용 메모리 비율입니다. 이 한도에 도달하면 외부 정렬을 사용합니다.                                       |
| [`max_rows_to_sort`](/ko/operations/settings/settings#max_rows_to_sort)                                                   | 정렬 전 최대 행 수입니다. 정렬 시 메모리 활용을 제한할 수 있습니다.                                                          |
| [`max_bytes_to_sort`](/ko/operations/settings/settings#max_rows_to_sort)                                                  | 정렬 전 최대 바이트 수입니다.                                                                                 |
| [`sort_overflow_mode`](/ko/operations/settings/settings#sort_overflow_mode)                                               | 정렬 전에 받은 행 수가 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                         |
| [`max_result_rows`](/ko/operations/settings/settings#max_result_rows)                                                     | 결과의 최대 행 수를 제한합니다.                                                                                |
| [`max_result_bytes`](/ko/operations/settings/settings#max_result_bytes)                                                   | 결과 크기를 바이트 단위로 제한합니다(압축되지 않은 데이터 기준).                                                             |
| [`result_overflow_mode`](/ko/operations/settings/settings#result_overflow_mode)                                           | 결과 크기가 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                                |
| [`max_execution_time`](/ko/operations/settings/settings#max_execution_time)                                               | 쿼리의 최대 실행 시간(초)입니다.                                                                               |
| [`timeout_overflow_mode`](/ko/operations/settings/settings#timeout_overflow_mode)                                         | 쿼리 실행 시간이 `max_execution_time`을 초과하거나 예상 실행 시간이 `max_estimated_execution_time`을 초과할 때의 동작을 설정합니다. |
| [`max_execution_time_leaf`](/ko/operations/settings/settings#max_execution_time_leaf)                                     | 의미상 `max_execution_time`과 유사하지만, 분산 쿼리 또는 원격 쿼리의 리프 노드에만 적용됩니다.                                   |
| [`timeout_overflow_mode_leaf`](/ko/operations/settings/settings#timeout_overflow_mode_leaf)                               | 리프 노드에서 쿼리 실행 시간이 `max_execution_time_leaf`를 초과할 때의 동작을 설정합니다.                                    |
| [`min_execution_speed`](/ko/operations/settings/settings#min_execution_speed)                                             | 최소 실행 속도(초당 행 수)입니다.                                                                              |
| [`min_execution_speed_bytes`](/ko/operations/settings/settings#min_execution_speed_bytes)                                 | 최소 실행 속도(초당 바이트 수)입니다.                                                                            |
| [`max_execution_speed`](/ko/operations/settings/settings#max_execution_speed)                                             | 최대 실행 속도(초당 행 수)입니다.                                                                              |
| [`max_execution_speed_bytes`](/ko/operations/settings/settings#max_execution_speed_bytes)                                 | 최대 실행 속도(초당 바이트 수)입니다.                                                                            |
| [`timeout_before_checking_execution_speed`](/ko/operations/settings/settings#timeout_before_checking_execution_speed)     | 지정된 시간(초)이 지난 후 실행 속도가 너무 느리지 않은지(`min_execution_speed` 이상인지) 확인합니다.                              |
| [`max_estimated_execution_time`](/ko/operations/settings/settings#max_estimated_execution_time)                           | 쿼리의 최대 예상 실행 시간(초)입니다.                                                                            |
| [`max_columns_to_read`](/ko/operations/settings/settings#max_columns_to_read)                                             | 단일 쿼리에서 테이블에서 읽을 수 있는 최대 컬럼 수입니다.                                                                 |
| [`max_temporary_columns`](/ko/operations/settings/settings#max_temporary_columns)                                         | 상수 컬럼을 포함해, 쿼리 실행 시 동시에 RAM에 유지해야 하는 임시 컬럼의 최대 수입니다.                                              |
| [`max_temporary_non_const_columns`](/ko/operations/settings/settings#max_temporary_non_const_columns)                     | 쿼리 실행 시 동시에 RAM에 유지해야 하는 임시 컬럼의 최대 수입니다. 단, 상수 컬럼은 제외합니다.                                         |
| [`max_subquery_depth`](/ko/operations/settings/settings#max_subquery_depth)                                               | 쿼리에 지정된 수보다 더 많이 중첩된 서브쿼리가 있을 때 어떻게 동작할지 설정합니다.                                                   |
| [`max_ast_depth`](/ko/operations/settings/settings#max_ast_depth)                                                         | 쿼리 구문 트리의 최대 중첩 깊이입니다.                                                                            |
| [`max_ast_elements`](/ko/operations/settings/settings#max_ast_elements)                                                   | 쿼리 구문 트리의 최대 요소 수입니다.                                                                             |
| [`max_rows_in_set`](/ko/operations/settings/settings#max_rows_in_set)                                                     | 서브쿼리로 생성된 IN 절의 데이터 집합에 허용되는 최대 행 수입니다.                                                           |
| [`max_bytes_in_set`](/ko/operations/settings/settings#max_bytes_in_set)                                                   | 서브쿼리로 생성된 IN 절의 Set이 사용할 수 있는 최대 바이트 수(비압축 데이터 기준)입니다.                                            |
| [`set_overflow_mode`](/ko/operations/settings/settings#max_bytes_in_set)                                                  | 데이터 양이 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                                |
| [`max_rows_in_distinct`](/ko/operations/settings/settings#max_rows_in_distinct)                                           | DISTINCT 사용 시 서로 다른 행의 최대 수입니다.                                                                   |
| [`max_bytes_in_distinct`](/ko/operations/settings/settings#max_bytes_in_distinct)                                         | DISTINCT 사용 시 해시 테이블이 사용하는 메모리 내 state의 최대 크기(비압축 바이트 기준)입니다.                                     |
| [`distinct_overflow_mode`](/ko/operations/settings/settings#distinct_overflow_mode)                                       | 데이터 양이 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                                |
| [`max_rows_to_transfer`](/ko/operations/settings/settings#max_rows_to_transfer)                                           | GLOBAL IN/JOIN 구문이 실행될 때 원격 서버로 전달되거나 임시 테이블에 저장될 수 있는 최대 크기(행 수 기준)입니다.                          |
| [`max_bytes_to_transfer`](/ko/operations/settings/settings#max_bytes_to_transfer)                                         | GLOBAL IN/JOIN 구문이 실행될 때 원격 서버로 전달되거나 임시 테이블에 저장될 수 있는 최대 바이트 수(비압축 데이터 기준)입니다.                   |
| [`transfer_overflow_mode`](/ko/operations/settings/settings#transfer_overflow_mode)                                       | 데이터 양이 제한값 중 하나를 초과할 때의 동작을 설정합니다.                                                                |
| [`max_rows_in_join`](/ko/operations/settings/settings#max_rows_in_join)                                                   | 테이블을 조인할 때 사용되는 해시 테이블의 최대 행 수를 제한합니다.                                                            |
| [`max_bytes_in_join`](/ko/operations/settings/settings#max_bytes_in_join)                                                 | 테이블을 조인할 때 사용되는 해시 테이블의 최대 크기(바이트 수 기준)입니다.                                                       |
| [`join_overflow_mode`](/ko/operations/settings/settings#join_overflow_mode)                                               | 다음 조인 제한 중 하나에 도달했을 때 ClickHouse가 수행할 동작을 정의합니다.                                                  |
| [`max_partitions_per_insert_block`](/ko/operations/settings/settings#max_partitions_per_insert_block)                     | 단일 삽입 블록의 최대 파티션 수를 제한하며, 블록에 너무 많은 파티션이 포함되면 예외가 발생합니다.                                          |
| [`throw_on_max_partitions_per_insert_block`](/ko/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | `max_partitions_per_insert_block`에 도달했을 때의 동작을 제어할 수 있습니다.                                        |
| [`max_temporary_data_on_disk_size_for_user`](/ko/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | 동시에 실행 중인 모든 사용자 쿼리에 대해 디스크의 임시 파일이 사용할 수 있는 최대 데이터 양(바이트 기준)입니다.                                 |
| [`max_temporary_data_on_disk_size_for_query`](/ko/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | 동시에 실행 중인 모든 쿼리에 대해 디스크의 임시 파일이 사용할 수 있는 최대 데이터 양(바이트 기준)입니다.                                     |
| [`max_sessions_for_user`](/ko/operations/settings/settings#max_sessions_for_user)                                         | 인증된 사용자별로 ClickHouse 서버에 동시에 유지할 수 있는 최대 세션 수입니다.                                                 |
| [`max_partitions_to_read`](/ko/operations/settings/settings#max_partitions_to_read)                                       | 단일 쿼리에서 접근할 수 있는 최대 파티션 수를 제한합니다.                                                                 |

<div id="obsolete-settings">
  ## 더 이상 사용되지 않는 설정
</div>

:::note
다음 설정은 더 이상 사용되지 않는 설정입니다.
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

최대 파이프라인 깊이입니다. 각 데이터 블록이 쿼리 처리 중 거치는 변환 단계의 수를 의미합니다. 이 값은 단일 서버 기준으로 계산됩니다. 파이프라인 깊이가 이 값을 초과하면 예외가 발생합니다.