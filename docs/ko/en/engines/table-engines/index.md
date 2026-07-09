---
description: '테이블 엔진 문서'
slug: /engines/table-engines/
toc_folder_title: '테이블 엔진'
toc_priority: 26
toc_title: '소개'
title: '테이블 엔진'
doc_type: 'reference'
---

테이블 엔진(테이블의 유형)은 다음을 결정합니다.

* 데이터가 저장되는 방식과 위치, 그리고 데이터를 기록할 위치와 읽어올 위치
* 어떤 쿼리를 지원하는지와 그 지원 방식
* 동시 데이터 액세스
* 인덱스가 있는 경우 해당 인덱스의 사용 방식
* 멀티스레드 요청 실행 가능 여부
* 데이터 복제 매개변수

<div id="engine-families">
  ## 엔진 계열
</div>

<div id="mergetree">
  ### MergeTree
</div>

고부하 작업에 적합한, 가장 범용적이고 기능이 풍부한 테이블 엔진입니다. 이러한 엔진의 공통적인 특성은 데이터를 빠르게 삽입하고, 이후 백그라운드에서 데이터를 처리한다는 점입니다. `MergeTree` 계열 엔진은 데이터 복제([Replicated*](/ko/engines/table-engines/mergetree-family/replication) 버전의 엔진), 파티셔닝, 보조 데이터 스키핑 인덱스, 그리고 다른 엔진에서는 지원되지 않는 기타 기능을 지원합니다.

이 계열에 속하는 엔진:

| MergeTree 엔진                                                                                         |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/ko/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/ko/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/ko/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/ko/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/ko/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/ko/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/ko/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/ko/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

최소한의 기능만 제공하는 경량 [엔진](../../engines/table-engines/log-family/index.md)입니다. 많은 수의 작은 테이블(약 100만 행 이하)을 빠르게 작성한 뒤, 나중에 전체를 한 번에 읽어야 할 때 가장 효과적입니다.

이 계열의 엔진:

| Log 엔진                                                   |
| -------------------------------------------------------- |
| [TinyLog](/ko/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/ko/engines/table-engines/log-family/stripelog) |
| [Log](/ko/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### 통합 엔진
</div>

다른 데이터 저장 및 처리 시스템과 통신하기 위한 엔진입니다.

엔진 계열에 속한 엔진:

| 통합 엔진                                                                           |
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
  ### 특수 엔진
</div>

이 계열에 속하는 엔진:

| 특수 엔진                                                     |
| --------------------------------------------------------- |
| [분산](/ko/engines/table-engines/special/distributed)          |
| [딕셔너리](/ko/engines/table-engines/special/dictionary)         |
| [머지](/ko/engines/table-engines/special/merge)                |
| [실행형](/ko/engines/table-engines/special/executable)          |
| [File](/ko/engines/table-engines/special/file)               |
| [Null](/ko/engines/table-engines/special/null)               |
| [Set](/ko/engines/table-engines/special/set)                 |
| [Join](/ko/engines/table-engines/special/join)               |
| [URL](/ko/engines/table-engines/special/url)                 |
| [View](/ko/engines/table-engines/special/view)               |
| [메모리](/ko/engines/table-engines/special/memory)              |
| [Buffer](/ko/engines/table-engines/special/buffer)           |
| [외부 데이터](/ko/engines/table-engines/special/external-data)    |
| [GenerateRandom](/ko/engines/table-engines/special/generate) |
| [KeeperMap](/ko/engines/table-engines/special/keeper-map)    |
| [FileLog](/ko/engines/table-engines/special/filelog)         |

<div id="table_engines-virtual_columns">
  ## 가상 컬럼
</div>

가상 컬럼은 엔진 소스 코드에 정의되는 테이블 엔진의 내장 속성입니다.

가상 컬럼은 `CREATE TABLE` 쿼리에서 지정하지 않아야 하며, `SHOW CREATE TABLE` 및 `DESCRIBE TABLE` 쿼리 결과에도 표시되지 않습니다. 또한 가상 컬럼은 읽기 전용이므로 여기에 데이터를 삽입할 수 없습니다.

가상 컬럼에서 데이터를 조회하려면 `SELECT` 쿼리에서 해당 이름을 명시해야 합니다. `SELECT *`는 가상 컬럼의 값을 반환하지 않습니다.

테이블의 가상 컬럼 중 하나와 같은 이름의 컬럼을 만들어 테이블을 생성하면 해당 가상 컬럼에는 접근할 수 없게 됩니다. 이렇게 하는 것은 권장하지 않습니다. 충돌을 피할 수 있도록 가상 컬럼 이름에는 일반적으로 앞에 밑줄이 붙습니다.

* `_table` — 데이터를 읽어온 테이블의 이름을 포함합니다. 유형: [String](../../sql-reference/data-types/string.md).

  사용 중인 테이블 엔진과 관계없이 모든 테이블에는 `_table`이라는 공통 가상 컬럼이 포함됩니다.

  머지 테이블 엔진으로 테이블을 쿼리할 때는 `WHERE/PREWHERE` 절에서 `_table`에 대한 상수 조건을 설정할 수 있습니다(예: `WHERE _table='xyz'`). 이 경우 `_table` 조건을 만족하는 테이블에 대해서만 읽기 작업이 수행되므로 `_table` 컬럼은 인덱스처럼 동작합니다.

  `SELECT ... FROM (... UNION ALL ...)` 형식의 쿼리를 사용할 때는 `_table` 컬럼을 지정해 반환된 행이 실제로 어떤 테이블에서 나온 것인지 확인할 수 있습니다.