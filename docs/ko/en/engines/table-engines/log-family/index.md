---
description: 'Log 엔진 계열에 대한 문서'
sidebar_label: 'Log 계열'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Log 엔진 계열'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Log 테이블 엔진 계열
</div>

<CloudNotSupportedBadge />

이 엔진들은 많은 수의 작은 테이블(최대 약 100만 행)을 빠르게 기록한 뒤, 나중에 이를 한 번에 전체로 읽어야 하는 시나리오를 위해 개발되었습니다.

이 계열의 엔진:

| Log Engines                                                 |
| ----------------------------------------------------------- |
| [StripeLog](/ko/engines/table-engines/log-family/stripelog.md) |
| [Log](/ko/engines/table-engines/log-family/log.md)             |
| [TinyLog](/ko/engines/table-engines/log-family/tinylog.md)     |

`Log` 계열 테이블 엔진은 데이터를 [HDFS](/ko/engines/table-engines/integrations/hdfs) 또는 [S3](/ko/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) 분산 파일 시스템에 저장할 수 있습니다.

:::warning 이 엔진은 로그 데이터용이 아닙니다.
이름과는 달리 *Log 테이블 엔진은 로그 데이터 저장을 위한 용도가 아닙니다. 빠르게 기록해야 하는 소량의 데이터에만 사용해야 합니다.
:::

<div id="common-properties">
  ## 공통 속성
</div>

엔진:

* 데이터를 디스크에 저장합니다.

* 쓰기 시 파일 끝에 데이터를 추가합니다.

* 동시 데이터 액세스를 위한 잠금을 지원합니다.

  `INSERT` 쿼리 중에는 테이블이 잠기며, 데이터를 읽거나 쓰는 다른 쿼리는 모두 테이블 잠금이 해제될 때까지 대기합니다. 데이터 쓰기 쿼리가 없으면 데이터 읽기 쿼리는 개수 제한 없이 동시에 수행할 수 있습니다.

* [뮤테이션](/ko/sql-reference/statements/alter#mutations)을 지원하지 않습니다.

* 인덱스를 지원하지 않습니다.

  즉, 데이터 범위에 대한 `SELECT` 쿼리를 효율적으로 수행할 수 없습니다.

* 데이터를 원자적으로 기록하지 않습니다.

  예를 들어 서버가 비정상적으로 종료되는 등 쓰기 작업이 중단되면 손상된 데이터가 있는 테이블이 생성될 수 있습니다.

<div id="differences">
  ## 차이점
</div>

`TinyLog` 엔진은 이 계열 중 가장 단순하며, 기능이 가장 제한적이고 효율도 가장 낮습니다. `TinyLog` 엔진은 단일 쿼리에서 여러 스레드를 사용한 병렬 데이터 읽기를 지원하지 않습니다. 단일 쿼리의 병렬 읽기를 지원하는 같은 계열의 다른 엔진보다 데이터를 더 느리게 읽으며, 각 컬럼을 별도의 파일에 저장하므로 `Log` 엔진과 거의 같은 수의 파일 디스크립터를 사용합니다. 간단한 시나리오에서만 사용하십시오.

`Log` 및 `StripeLog` 엔진은 병렬 데이터 읽기를 지원합니다. 데이터를 읽을 때 ClickHouse는 여러 스레드를 사용합니다. 각 스레드는 별도의 데이터 블록을 처리합니다. `Log` 엔진은 테이블의 각 컬럼마다 별도의 파일을 사용합니다. `StripeLog`는 모든 데이터를 하나의 파일에 저장합니다. 그 결과 `StripeLog` 엔진은 더 적은 수의 파일 디스크립터를 사용하지만, 데이터를 읽을 때의 효율은 `Log` 엔진이 더 높습니다.