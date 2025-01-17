---
title: データ管理
description: 観測性のためのデータ管理
slug: /ja/observability/managing-data
keywords: [observability, logs, traces, metrics, OpenTelemetry, Grafana, otel]
---

# データ管理

観測性のためのClickHouseの導入は常に大規模なデータセットが関与しており、これを管理する必要があります。ClickHouseにはデータ管理を支援するためのさまざまな機能があります。

## パーティション

ClickHouseでのパーティションは、カラムまたはSQL式に従ってディスク上でデータを論理的に分割することを可能にします。データを論理的に分割することで、それぞれのパーティションを独立して操作（例: 削除）することができます。これにより、ユーザーは時間や[クラスタからの効率的なデータ削除](/ja/sql-reference/statements/alter/partition)に基づいてパーティションを移動し、したがってサブセットを効率的にストレージ層間で移動することができます。

パーティションは、テーブルが初めて定義される際に`PARTITION BY`句を通じて指定されます。この句には、行をどのパーティションに送信するかを決定するためのSQL式を任意のカラムに対して含めることができます。

<img src={require('./images/observability-14.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

データのパーツはディスク上の各パーティションと論理的に関連付けられており（共通のフォルダ名のプレフィックスを介して）、独立してクエリを実行することができます。以下の例では、デフォルトの`otel_logs`スキーマは`toDate(Timestamp)`式を使用して日ごとにパーティションを分割します。データがClickHouseに挿入される際に、この式が各行に対して評価され、該当するパーティションにルーティングされます（その日の最初の行である場合はパーティションが作成されます）。

```sql
CREATE TABLE default.otel_logs
(
...
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
```

[いくつかの操作](/ja/sql-reference/statements/alter/partition)をパーティション上で実行することができます。例えば、[バックアップ](/ja/sql-reference/statements/alter/partition#freeze-partition)、[カラムの操作](/ja/sql-reference/statements/alter/partition#clear-column-in-partition)、[行ごとのデータの更新](/ja/sql-reference/statements/alter/partition#update-in-partition)/[削除](/ja/sql-reference/statements/alter/partition#delete-in-partition)、および[インデックスのクリア（例: 二次インデックス）](/ja/sql-reference/statements/alter/partition#clear-index-in-partition)が含まれます。

例えば、`otel_logs`テーブルが日ごとにパーティション分割されていると仮定すると、構造化されたログデータセットであれば、以下のように数日分のデータが含まれることになります。

```sql
SELECT Timestamp::Date AS day,
	 count() AS c
FROM otel_logs
GROUP BY day
ORDER BY c DESC

┌────────day─┬───────c─┐
│ 2019-01-22 │ 2333977 │
│ 2019-01-23 │ 2326694 │
│ 2019-01-26 │ 1986456 │
│ 2019-01-24 │ 1896255 │
│ 2019-01-25 │ 1821770 │
└────────────┴─────────┘

5 rows in set. Elapsed: 0.058 sec. Processed 10.37 million rows, 82.92 MB (177.96 million rows/s., 1.42 GB/s.)
Peak memory usage: 4.41 MiB.
```

現在のパーティションは、簡単なシステムテーブルクエリを使用して見つけることができます。

```sql
SELECT DISTINCT partition
FROM system.parts
WHERE `table` = 'otel_logs'

┌─partition──┐
│ 2019-01-22 │
│ 2019-01-23 │
│ 2019-01-24 │
│ 2019-01-25 │
│ 2019-01-26 │
└────────────┘

5 rows in set. Elapsed: 0.005 sec.
```

別のテーブル、例えば古いデータを保存するための`otel_logs_archive`があるかもしれません。データはパーティションごとにこのテーブルに効率的に移動できます（これは単なるメタデータの変更です）。

```sql
CREATE TABLE otel_logs_archive AS otel_logs
--move data to archive table
ALTER TABLE otel_logs
	(MOVE PARTITION tuple('2019-01-26') TO TABLE otel_logs_archive
--confirm data has been moved
SELECT
	Timestamp::Date AS day,
	count() AS c
FROM otel_logs
GROUP BY day
ORDER BY c DESC

┌────────day─┬───────c─┐
│ 2019-01-22 │ 2333977 │
│ 2019-01-23 │ 2326694 │
│ 2019-01-24 │ 1896255 │
│ 2019-01-25 │ 1821770 │
└────────────┴─────────┘

4 rows in set. Elapsed: 0.051 sec. Processed 8.38 million rows, 67.03 MB (163.52 million rows/s., 1.31 GB/s.)
Peak memory usage: 4.40 MiB.

SELECT Timestamp::Date AS day,
	count() AS c
FROM otel_logs_archive
GROUP BY day
ORDER BY c DESC

┌────────day─┬───────c─┐
│ 2019-01-26 │ 1986456 │
└────────────┴─────────┘

1 row in set. Elapsed: 0.024 sec. Processed 1.99 million rows, 15.89 MB (83.86 million rows/s., 670.87 MB/s.)
Peak memory usage: 4.99 MiB.
```

これは、`INSERT INTO SELECT`を使用し、新しいターゲットテーブルにデータを書き換える必要がある他の手法とは対照的です。

> [テーブル間のパーティションの移動](/ja/sql-reference/statements/alter/partition#move-partition-to-table)には、いくつかの条件を満たす必要があります。特に、テーブルは同じ構造、パーティションキー、主キー、およびインデックス/プロジェクションを持っている必要があります。`ALTER` DDLでのパーティションの指定方法についての詳細な説明は[こちら](/ja/sql-reference/statements/alter/partition#how-to-set-partition-expression)で確認できます。

さらに、データはパーティションごとに効率的に削除することができます。これは代替技法（変異または論理削除）よりもはるかにリソース効率が高く、優先されるべきです。

```sql
ALTER TABLE otel_logs
	(DROP PARTITION tuple('2019-01-25'))

SELECT
	Timestamp::Date AS day,
	count() AS c
FROM otel_logs
GROUP BY day
ORDER BY c DESC
┌────────day─┬───────c─┐
│ 2019-01-22 │ 4667954 │
│ 2019-01-23 │ 4653388 │
│ 2019-01-24 │ 3792510 │
└────────────┴─────────┘
```

> この機能は`ttl_only_drop_parts=1`が使用されると有効期限 (TTL) によって活用されます。詳細については「有効期限 (TTL) を使用したデータ管理」を参照してください。

### アプリケーション

上記の方法は、データを効率的に移動および操作する方法を示しています。実際にはユーザーはほとんどの場合、以下の2つのシナリオで観測性のユースケースにおいてパーティション操作を頻繁に利用するでしょう。

- **階層化アーキテクチャ** - ストレージ層間でデータを移動します（「ストレージティア」を参照）。これにより、ホットコールドアーキテクチャを構築することができます。
- **効率的な削除** - データが指定された有効期限に達した場合（「有効期限 (TTL) を使用したデータ管理」を参照）

以下に、これらの2つの詳細について詳しく説明します。

### クエリパフォーマンス

パーティションはクエリのパフォーマンスに助けになりますが、それにはアクセスパターンに非常に依存します。もしクエリが少数のパーティション（理想的には1つ）のみをターゲットとする場合、パフォーマンスが潜在的に改善する可能性があります。これは通常、パーティショニングキーが主キーに含まれておらず、それによってフィルタリングを行う場合に有用です。ただし、多くのパーティションをカバーする必要があるクエリは、場合によってはパーティションを使用しない場合よりもパフォーマンスが悪くなる場合があります（パーツが多いため）。単一パーティションをターゲットとする利点は、パーティショニングキーがすでに主キーの初期エントリーである場合にはほとんど、または全く現れないかもしれません。パーティションは、各パーティション内の値がユニークである場合に[GROUP BYクエリを最適化するために使用](/ja/engines/table-engines/mergetree-family/custom-partitioning-key#group-by-optimisation-using-partition-key)することもできます。ただし、一般的に、ユーザーは主キーが最適化されていることを確認し、特定の予測可能なサブセットのアクセスパターンがある特殊な場合（例えば、日ごとにパーティショニングしており、ほとんどのクエリが前日のもの）にのみパーティショニングをクエリの最適化技術として考慮するべきです。この動作の例は[こちら](https://medium.com/datadenys/using-partitions-in-clickhouse-3ea0decb89c4)で確認できます。

## 有効期限 (TTL) を使用したデータ管理

Time-to-Live (TTL) は、ClickHouseを利用した観測性ソリューションにおける効率的なデータ保持と管理のための重要な機能です。特に膨大なデータが継続的に生成される場合においてそうです。ClickHouseにおいてTTLを実装することにより、古いデータの自動有効期限切れと削除が可能になり、ストレージが最適に使用され、手動介入なしで性能が維持されます。この機能は、データベースをスリムに保ち、ストレージコストを削減し、最も関連性が高く最新のデータに焦点を当てることでクエリの速度と効率を確保するために不可欠です。また、データ保持ポリシーに準拠し、データライフサイクルをシステマティックに管理することで、観測性ソリューションの全体的な持続可能性と拡張性を向上させます。

TTLはClickHouseにおいて、テーブルレベルまたはカラムレベルで指定することができます。

### テーブルレベルのTTL

ログとトレースの両方のデフォルトのスキーマには、指定された期間後にデータの有効期限を設定するためのTTLが含まれています。これはClickHouseエクスポーターで`ttl`キーの下に指定されます。例:

```yaml
exporters:
 clickhouse:
   endpoint: tcp://localhost:9000?dial_timeout=10s&compress=lz4&async_insert=1
   ttl: 72h
```

この構文は現在[Golang Duration syntax](https://pkg.go.dev/time#ParseDuration)をサポートしています。**ユーザーには`h`を使用し、これがパーティショニング期間に一致することを確認することをお勧めします。例えば、日ごとにパーティショニングする場合、24h, 48h, 72hなどの日数の倍数であることを確認してください。** これにより、自動的にTTL句がテーブルに追加されます。例：`ttl: 96h`の場合。

```sql
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + toIntervalDay(4)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

デフォルトでは、期限切れのTTLを持つデータはClickHouseによって[データパーツがマージされる](/ja/engines/table-engines/mergetree-family/mergetree#mergetree-data-storage)時に削除されます。ClickHouseが期限切れのデータを検出すると、スケジュール外のマージを実行します。

> TTLはすぐに適用されるわけではなく、前述のようなスケジュールに従って適用されます。MergeTreeテーブル設定の`merge_with_ttl_timeout`は、削除TTLでのマージが繰り返される前の最小遅延を秒単位で設定します。デフォルト値は14400秒（4時間）です。しかし、それは最低限の遅延に過ぎず、TTLマージがトリガーされるまでに時間がかかる場合があります。値が低すぎると、多くのスケジュール外マージが発生し、リソースを多く消費する可能性があります。TTLの期限切れを強制するには、コマンド`ALTER TABLE my_table MATERIALIZE TTL`を使用します。

**重要：`ttl_only_drop_parts=1`の設定を使用することをお勧めします。**（デフォルトスキーマで適用されています）この設定が有効な場合、ClickHouseはすべての行が期限切れの状態になると全体のパートを削除します。部分的にTTL期限切れの行をクリーンアップする（`ttl_only_drop_parts=0`の場合にリソース集約的な変異を介して達成される）のではなく、全体のパートを削除することで、より短い`merge_with_ttl_timeout`時間を持ち、システム性能への影響を低くできます。データが、有効期間と同じ単位でパーティション化されている場合（例: 日）、パーツは自然に定義された間隔のデータのみを含むようになります。これにより`ttl_only_drop_parts=1`を効率的に適用することができます。

### カラムレベルのTTL

上記の例では、データがテーブルレベルで期限切れになります。ユーザーはカラムレベルでもデータを期限切れにすることができます。データが古くなるにつれて、調査時にリソースのオーバーヘッドを正当化する価値がないカラムを削除するために使用できます。例えば、`Body`カラムを保持し続けることをお勧めします。これは挿入時に抽出されていない新しい動的メタデータが追加される可能性があるためです（例：新しいKubernetesラベル）。例えば1ヶ月後、追加のメタデータが役に立たないと明らかであれば、その値を保持する価値があることを制限できます。

30日後に`Body`カラムが削除される例を以下に示します。

```sql
CREATE TABLE otel_logs_v2
(
	`Body` String TTL Timestamp + INTERVAL 30 DAY,
	`Timestamp` DateTime,
	...
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
```

> カラムレベルのTTLを指定する場合、ユーザー自身でスキーマを指定する必要があります。これはOTelコレクターでは指定できません。

## データの再圧縮

通常、観測性のデータセットには`ZSTD(1)`をお勧めしますが、ユーザーは異なる圧縮アルゴリズムや高レベルの圧縮、例えば`ZSTD(3)`を試行することができます。スキーマ作成時に指定することもでき、設定された期間後に変更できるように圧縮を設定することもできます。別のコーデックまたは圧縮アルゴリズムが圧縮を改善するがクエリパフォーマンスが低下する場合、このトレードオフは、調査での利用度が低い古いデータに対して許容できても、調査での利用度が高い新しいデータに対しては許容できません。

下記に、データを削除する代わりに4日後に`ZSTD(3)`でデータを圧縮する例を示します。

```sql
CREATE TABLE default.otel_logs_v2
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt16,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`Size` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`RemoteAddress` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8,
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
TTL Timestamp + INTERVAL 4 DAY RECOMPRESS CODEC(ZSTD(3))
```

> ユーザーは、異なる圧縮レベルやアルゴリズムが挿入およびクエリパフォーマンスに与える影響を評価することを常にお勧めします。例えば、デルタコーデックはタイムスタンプの圧縮に役立ちます。しかし、これが主キーの一部である場合、フィルタリング性能に影響を及ぼす可能性があります。

TTLの設定および例に関するさらなる詳細は[こちら](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)で確認できます。TTLがテーブルおよびカラムに追加および修正される方法については[こちら](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)で確認できます。TTLがホット-ウォーム-コールドアーキテクチャなどのストレージ階層を可能にする方法については、「ストレージティア」を参照してください。

## ストレージティア

ClickHouseでは、SSDによるホット/最近のデータとS3による古いデータを指すストレージティアを、異なるディスク上に作成することができます。このアーキテクチャにより、調査での利用頻度が低いために高いクエリSLAを持つ古いデータに、より低コストのストレージを用いることが可能です。

> ClickHouse Cloudは、S3にバックアップされた単一のデータコピーを使用し、SSD-backed ノードキャッシュを使用します。そのため、ClickHouse Cloudでストレージティアは必要ありません。

ストレージティアの作成には、ディスクの作成が必要で、その後ストレージポリシーを構築し、テーブル作成時に指定できるボリュームを使用します。データは、使用率、パーツサイズ、ボリュームの優先度に基づいて自動的に異なるディスク間で移動することができます。詳細は[こちら](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)で確認できます。

データは`ALTER TABLE MOVE PARTITION`コマンドを使用して手動でディスク間を移動することができますが、TTLを使用してボリューム間のデータの移動を制御することも可能です。完全な例は[こちら](/ja/guides/developer/ttl#implementing-a-hotwarmcold-architecture)で確認できます。

## スキーマ変更の管理

システムのライフタイム中にログとトレーススキーマは必ず変わるでしょう。例えば、新しいメタデータやポッドラベルを持つ新しいシステムを監視する場合など。OTelスキーマを使用してデータを生成し、構造化された形式でオリジナルのイベントデータをキャプチャすることで、ClickHouseスキーマはこれらの変更に対して適応性を持つようになります。しかし、新しいメタデータが利用可能になり、クエリアクセスパターンが変化するにつれて、ユーザーはこれらの開発に対応するためにスキーマを更新したいと考えるでしょう。

スキーマ変更時のダウンタイムを避けるために、ユーザーはいくつかのオプションがあります。以下にそれらを示します。

### デフォルト値を使用する

カラムは[`DEFAULT`値](/ja/sql-reference/statements/create/table#default)を使用してスキーマに追加することができます。INSERT時に指定されていない場合、指定されたデフォルトが使用されます。

スキーマ変更は、新しいカラムが送信されるようにするマテリアライズドビューの変換ロジックやOTelコレクターの設定を変更する前に行うことができます。

一度スキーマが変更されると、ユーザーはOTeLコレクターを再構成できます。推奨される「SQLでの構造抽出」で概説されているプロセスを使用することを想定した場合、OTeLコレクターがそのデータをNullテーブルエンジンに送信し、その結果を抽出してストレージに送信するためのマテリアライズドビューが担当し、ターゲットスキーマを抽出する場合、ビューは次の[`ALTER TABLE ... MODIFY QUERY`構文](/ja/sql-reference/statements/alter/view)を使用して修正できます。次のように、OTelの構造化ログからターゲットスキーマを抽出するためのビューを持つテーブルを例に示します：

```sql
CREATE TABLE default.otel_logs_v2
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt16,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`RemoteAddress` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)

CREATE MATERIALIZED VIEW otel_logs_mv TO otel_logs_v2 AS
SELECT
        Body,
	Timestamp::DateTime AS Timestamp,
	ServiceName,
	LogAttributes['status']::UInt16 AS Status,
	LogAttributes['request_protocol'] AS RequestProtocol,
	LogAttributes['run_time'] AS RunTime,
	LogAttributes['user_agent'] AS UserAgent,
	LogAttributes['referer'] AS Referer,
	LogAttributes['remote_user'] AS RemoteUser,
	LogAttributes['request_type'] AS RequestType,
	LogAttributes['request_path'] AS RequestPath,
	LogAttributes['remote_addr'] AS RemoteAddress,
	domain(LogAttributes['referer']) AS RefererDomain,
	path(LogAttributes['request_path']) AS RequestPage,
	multiIf(Status::UInt64 > 500, 'CRITICAL', Status::UInt64 > 400, 'ERROR', Status::UInt64 > 300, 'WARNING', 'INFO') AS SeverityText,
	multiIf(Status::UInt64 > 500, 20, Status::UInt64 > 400, 17, Status::UInt64 > 300, 13, 9) AS SeverityNumber
FROM otel_logs
```

`LogAttributes`から新しいカラム`Size`を抽出したいとします。これを`ALTER TABLE`を使用してスキーマに追加し、デフォルト値を指定できます：

```sql
ALTER TABLE otel_logs_v2
	(ADD COLUMN `Size` UInt64 DEFAULT JSONExtractUInt(Body, 'size'))
```

上記の例では、`LogAttributes`の`size`キーをデフォルトとして指定しています（存在しない場合には0になります）。これは、値が挿入されなかった行に対して、このカラムをアクセスするクエリはマップをアクセスする必要があるため、これにより後のクエリのコストが増加します。もちろん、定数で指定することも可能です。例えば、0にすると、値が挿入されなかった行に対するその後のクエリのコストが下がります。このテーブルをクエリすることで、マップから期待通りに値が生成されることがわかります：

```sql
SELECT Size
FROM otel_logs_v2
LIMIT 5
┌──Size─┐
│ 30577 │
│  5667 │
│  5379 │
│  1696 │
│ 41483 │
└───────┘

5 rows in set. Elapsed: 0.012 sec.
```

この値がすべての将来のデータに対して挿入されることを保証するために、以下に示す`ALTER TABLE`構文を使用してマテリアライズドビューを修正できます：

```sql
ALTER TABLE otel_logs_mv
	MODIFY QUERY
SELECT
    	Body,
        Timestamp::DateTime AS Timestamp,
        ServiceName,
        LogAttributes['status']::UInt16 AS Status,
        LogAttributes['request_protocol'] AS RequestProtocol,
        LogAttributes['run_time'] AS RunTime,
        LogAttributes['size'] AS Size,
        LogAttributes['user_agent'] AS UserAgent,
        LogAttributes['referer'] AS Referer,
        LogAttributes['remote_user'] AS RemoteUser,
        LogAttributes['request_type'] AS RequestType,
        LogAttributes['request_path'] AS RequestPath,
        LogAttributes['remote_addr'] AS RemoteAddress,
        domain(LogAttributes['referer']) AS RefererDomain,
        path(LogAttributes['request_path']) AS RequestPage,
        multiIf(Status::UInt64 > 500, 'CRITICAL', Status::UInt64 > 400, 'ERROR', Status::UInt64 > 300,                 'WARNING', 'INFO') AS SeverityText,
        multiIf(Status::UInt64 > 500, 20, Status::UInt64 > 400, 17, Status::UInt64 > 300, 13, 9) AS SeverityNumber
FROM otel_logs
```

その後の行には、挿入時に`Size`カラムが埋められます。

### 新しいテーブルを作成

上記のプロセスの代替として、ユーザーは新しいスキーマを持つ新しいターゲットテーブルを単に作成することができます。マテリアライズドビューは、上記の`ALTER TABLE MODIFY QUERY`を使用して新しいテーブルを使用するように修正できます。このアプローチでは、ユーザーは例えば`otel_logs_v3`のようにテーブルをバージョン管理することができます。

このアプローチでは、複数のテーブルをクエリする必要があります。テーブル名のワイルドカードパターンを受け入れる[`merge`関数](/ja/sql-reference/table-functions/merge)を使用することができます。v2とv3の`otel_logs`テーブルをクエリする例を以下に示します：

```sql
SELECT Status, count() AS c
FROM merge('otel_logs_v[2|3]')
GROUP BY Status
ORDER BY c DESC
LIMIT 5

┌─Status─┬────────c─┐
│	200 │ 38319300 │
│	304 │  1360912 │
│	302 │   799340 │
│	404 │   420044 │
│	301 │   270212 │
└────────┴──────────┘

5 rows in set. Elapsed: 0.137 sec. Processed 41.46 million rows, 82.92 MB (302.43 million rows/s., 604.85 MB/s.)
```

ユーザーが`merge`関数を使用することを回避し、複数のテーブルを組み合わせたテーブルをエンドユーザーに公開したい場合、[Mergeテーブルエンジン](/ja/engines/table-engines/special/merge)を使用できます。次にこの例を示します：

```sql
CREATE TABLE otel_logs_merged
ENGINE = Merge('default', 'otel_logs_v[2|3]')

SELECT Status, count() AS c
FROM otel_logs_merged
GROUP BY Status
ORDER BY c DESC
LIMIT 5

┌─Status─┬────────c─┐
│	200 │ 38319300 │
│	304 │  1360912 │
│	302 │   799340 │
│	404 │   420044 │
│	301 │   270212 │
└────────┴──────────┘

5 rows in set. Elapsed: 0.073 sec. Processed 41.46 million rows, 82.92 MB (565.43 million rows/s., 1.13 GB/s.)
```

新しいテーブルが追加された場合、このテーブルは`EXCHANGE`テーブル構文を使用して更新できます。例えば、v4テーブルを追加するには、新しいテーブルを作成し、以前のバージョンと原子交換できます。

```sql
CREATE TABLE otel_logs_merged_temp
ENGINE = Merge('default', 'otel_logs_v[2|3|4]')

EXCHANGE TABLE otel_logs_merged_temp AND otel_logs_merged

SELECT Status, count() AS c
FROM otel_logs_merged
GROUP BY Status
ORDER BY c DESC
LIMIT 5

┌─Status─┬────────c─┐
│	200 │ 39259996 │
│	304 │  1378564 │
│	302 │   820118 │
│	404 │   429220 │
│	301 │   276960 │
└────────┴──────────┘

5 rows in set. Elapsed: 0.068 sec. Processed 42.46 million rows, 84.92 MB (620.45 million rows/s., 1.24 GB/s.)
```
