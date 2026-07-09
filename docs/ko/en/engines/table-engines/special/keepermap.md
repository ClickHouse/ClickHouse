---
description: '이 엔진을 사용하면 Keeper/ZooKeeper 클러스터를 선형화 가능한 쓰기와 순차적 일관성이 보장되는 읽기를 지원하는 일관된 키-값 저장소로 사용할 수 있습니다.'
sidebar_label: 'KeeperMap'
sidebar_position: 150
slug: /engines/table-engines/special/keeper-map
title: 'KeeperMap 테이블 엔진'
doc_type: 'reference'
---

이 엔진을 사용하면 Keeper/ZooKeeper 클러스터를 선형화 가능한 쓰기와 순차적 일관성이 보장되는 읽기를 지원하는 일관된 키-값 저장소로 사용할 수 있습니다.

KeeperMap 스토리지 엔진을 활성화하려면 `<keeper_map_path_prefix>` 구성에서 테이블을 저장할 ZooKeeper 경로를 정의해야 합니다.

예시:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

여기서 경로에는 다른 유효한 ZooKeeper 경로를 사용할 수 있습니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

엔진 매개변수:

* `root_path` - `table_name`이 저장될 ZooKeeper 경로입니다.
  이 경로에는 `<keeper_map_path_prefix>` 구성에 정의된 접두사를 포함하면 안 됩니다. 해당 접두사는 `root_path`에 자동으로 추가되기 때문입니다.
  또한 `auxiliary_zookeeper_cluster_name:/some/path` 형식도 지원합니다. 여기서 `auxiliary_zookeeper_cluster`는 `<auxiliary_zookeepers>` 구성 내에 정의된 ZooKeeper 클러스터입니다.
  기본값으로는 `<zookeeper>` 구성 내에 정의된 ZooKeeper 클러스터가 사용됩니다.
* `keys_limit` - 테이블 내에서 허용되는 키 수입니다.
  이 리밋은 소프트 리밋이므로, 일부 예외적인 경우에는 테이블에 더 많은 키가 들어갈 수 있습니다.
* `primary_key_name` – 컬럼 목록에 있는 임의의 컬럼 이름입니다.
* `primary key`는 반드시 지정해야 하며, 프라이머리 키(primary key)로는 하나의 컬럼만 지원됩니다. 프라이머리 키는 ZooKeeper에서 `node name`으로 바이너리 직렬화됩니다.
* 프라이머리 키를 제외한 컬럼은 해당 순서대로 바이너리 직렬화되며, 직렬화된 키로 정의된 결과 node의 값으로 저장됩니다.
* 키 `equals` 또는 `in` 필터링이 있는 쿼리는 `Keeper`에서 여러 키를 조회하도록 최적화되며, 그렇지 않으면 모든 값을 fetch합니다.

예시:

```sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

및

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

각 값은 `(v1, v2, v3)`를 바이너리로 직렬화한 형태이며, `Keeper`의 `/keeper_map_tables/keeper_map_table/data/serialized_key`에 저장됩니다.
또한 키 수의 소프트 리밋은 4개입니다.

여러 테이블이 동일한 ZooKeeper 경로에 생성되면, 이를 사용하는 테이블이 1개 이상 존재하는 한 값은 유지됩니다.
따라서 테이블을 생성할 때 `ON CLUSTER` 절을 사용하여 여러 ClickHouse 인스턴스에서 데이터를 공유할 수 있습니다.
물론, 서로 독립적인 ClickHouse 인스턴스에서 동일한 경로로 `CREATE TABLE`을 수동 실행하여 동일한 데이터 공유 효과를 얻는 것도 가능합니다.

<div id="supported-operations">
  ## 지원되는 작업
</div>

<div id="inserts">
  ### 삽입
</div>

새 행이 `KeeperMap`에 삽입될 때 키가 존재하지 않으면 해당 키에 대한 새 항목이 생성됩니다.
키가 이미 존재하고 `keeper_map_strict_mode` 설정이 `true`로 지정되어 있으면 예외가 발생하며, 그렇지 않으면 해당 키의 값이 덮어써집니다.

예시:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### 삭제
</div>

행은 `DELETE` 쿼리 또는 `TRUNCATE`를 사용해 삭제할 수 있습니다.
키가 존재하고 `keeper_map_strict_mode` 설정이 `true`로 설정되어 있으면, 데이터 가져오기와 삭제는 원자적으로 수행할 수 있을 때만 성공합니다.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

<div id="updates">
  ### 업데이트
</div>

값은 `ALTER TABLE` 쿼리를 사용해 업데이트할 수 있습니다. 프라이머리 키(프라이머리 키)는 업데이트할 수 없습니다.
`keeper_map_strict_mode` 설정이 `true`로 지정된 경우, 데이터 가져오기와 업데이트는 원자적으로 실행될 때에만 성공합니다.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse와 Hex로 실시간 분석 애플리케이션 구축하기](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)