---
slug: /ja/engines/table-engines/special/keeper-map
sidebar_position: 150
sidebar_label: KeeperMap
---

# KeeperMap {#keepermap}

このエンジンを使用すると、Keeper/ZooKeeper クラスターを一貫したキー・バリュー ストアとして利用でき、線形化された書き込みと、順次整合性のある読み取りが可能になります。

KeeperMap ストレージエンジンを有効にするには、テーブルを保存する ZooKeeper パスを `<keeper_map_path_prefix>` 設定で定義する必要があります。

例:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

ここで、パスは他の任意の有効な ZooKeeper パスであることができます。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

エンジンパラメータ:

- `root_path` - `table_name` が保存される ZooKeeper パス。
このパスには `<keeper_map_path_prefix>` 設定で定義されたプレフィックスを含める必要はありません。プレフィックスは自動的に `root_path` に追加されます。
さらに、`auxiliary_zookeeper_cluster_name:/some/path` の形式もサポートされており、`auxiliary_zookeeper_cluster` は `<auxiliary_zookeepers>` 設定内で定義された ZooKeeper クラスターです。
デフォルトでは、`<zookeeper>` 設定内で定義された ZooKeeper クラスターが使用されます。
- `keys_limit` - テーブル内に許可されるキーの数。
この制限はソフトリミットであり、エッジケースでテーブルにより多くのキーが入り込む可能性があります。
- `primary_key_name` – カラムリスト内の任意のカラム名。
- `主キー` は指定する必要があり、主キーには1つのカラムのみをサポートします。主キーは ZooKeeper 内で `ノード名` としてバイナリでシリアル化されます。
- 主キー以外のカラムは対応する順序でバイナリにシリアル化され、シリアル化されたキーによって定義された結果ノードの値として保存されます。
- キーに対する `equals` または `in` フィルタリングを使用したクエリは `Keeper` からのマルチキー検索に最適化され、それ以外の場合はすべての値がフェッチされます。

例:

``` sql
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

以下の設定で

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

各値、すなわち `(v1, v2, v3)` のバイナリシリアル化されたものは、`Keeper` 内の `/keeper_map_tables/keeper_map_table/data/serialized_key` 内に保存されます。
また、キー数にはソフトリミットとして4の制限があります。

同じ ZooKeeper パスに対して複数のテーブルを作成した場合、値は少なくとも1つのテーブルがそれを使用している限り、永続化されます。
したがって、テーブルを作成する際に `ON CLUSTER` 句を使用して、複数の ClickHouse インスタンスからデータを共有することが可能です。
もちろん、無関連な ClickHouse インスタンスで同じパスを持つ `CREATE TABLE` を手動で実行して、同じデータ共有効果を得ることもできます。

## 対応する操作 {#supported-operations}

### 挿入

`KeeperMap` に新しい行を挿入する場合、キーが存在しない場合は、そのキーのための新しいエントリが作成されます。
キーが存在する場合で、`keeper_map_strict_mode` 設定が `true` に設定されている場合は、例外がスローされ、そうでない場合はキーの値が上書きされます。

例:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

### 削除

行は `DELETE` クエリまたは `TRUNCATE` を使用して削除できます。
キーが存在し、かつ `keeper_map_strict_mode` 設定が `true` に設定されている場合、データの取得と削除は、アトミックに実行される場合にのみ成功します。

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

### 更新

値は `ALTER TABLE` クエリを使用して更新できます。主キーは更新できません。
`keeper_map_strict_mode` 設定が `true` に設定されている場合、データの取得と更新はアトミックに実行された場合にのみ成功します。

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

## 関連コンテンツ

- ブログ: [リアルタイム分析アプリケーションを ClickHouse と Hex で構築する](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)
