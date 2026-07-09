---
description: '이 ClickHouse 서버에 등록된 현재 활성 상태의 ZooKeeper watch를 보여주는 시스템 테이블입니다.'
keywords: ['시스템 테이블', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: '참고'
---

<div id="description">
  ## 설명
</div>

이 ClickHouse 서버가 ZooKeeper 노드(보조 ZooKeepers 포함)에 등록한 현재 활성 [watch](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches)를 표시합니다. 각 행은 watch 1개를 나타냅니다.

<div id="columns">
  ## 컬럼
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper 연결 이름(`default`는 기본 연결, 그 외에는 보조 이름)입니다.
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — watch가 생성된 시간입니다.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — watch가 생성된 시간을 마이크로초 정밀도로 나타낸 값입니다.
* `path` ([String](../../sql-reference/data-types/string.md)) — watch가 설정된 ZooKeeper 경로입니다.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — watch를 등록한 연결의 세션 ID입니다.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — watch를 생성한 요청의 XID입니다.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — watch를 생성한 요청의 유형입니다.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — watch 유형입니다. 가능한 값:
  * `Children` — child nodes 목록의 변경을 감시합니다(`List` 작업으로 설정).
  * `Exists` — node의 생성 또는 삭제를 감시합니다.
  * `Data` — node 데이터의 변경을 감시합니다(`Get` 작업으로 설정).

예시:

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**관련 항목**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [ZooKeeper 가이드](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)