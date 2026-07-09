---
description: '데이터 스킵 인덱스 조작 문서'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: '데이터 스킵 인덱스 조작'
toc_hidden_folder: true
doc_type: '참고'
---

다음 작업을 수행할 수 있습니다:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - 테이블 메타데이터에 인덱스 정의를 추가합니다.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - 테이블 메타데이터에서 인덱스 설명을 제거하고 디스크에서 인덱스 파일을 삭제합니다. 이 작업은 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 지정한 `partition_name`에 대해 보조 인덱스 `name`를 재구성합니다. [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다. `IN PARTITION` 부분을 생략하면 테이블 전체 데이터에 대해 인덱스를 재구성합니다.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 정의는 제거하지 않고 디스크에서 보조 인덱스(secondary index) 파일을 삭제합니다. 이는 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

`ADD`, `DROP`, `CLEAR` 명령은 메타데이터만 변경하거나 파일만 제거한다는 점에서 경량입니다.
또한 이 명령은 복제되며, ClickHouse Keeper 또는 ZooKeeper를 통해 인덱스 메타데이터를 동기화합니다.

:::note
인덱스 조작은 [`*MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree.md) 엔진을 사용하는 테이블([복제된](/ko/engines/table-engines/mergetree-family/replication.md) 변형 포함)에서만 지원됩니다.
:::