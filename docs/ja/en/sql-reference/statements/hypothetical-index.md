---
description: '仮想（what-if）索引に関するドキュメント'
sidebar_label: '仮想索引'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: '仮想索引'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # 仮想索引
</div>

仮想索引は、実際に構築したり保存したりすることなく `MergeTree` ファミリーのテーブルにアタッチできる、セッションスコープの仮想的なスキップ索引です。これらは現在のセッション内でのみ存在し、実際のスキップ索引がクエリにどのような影響を与えるか、主にスキップ率 (スキップできるマークの割合) や、マーク数とバイト数で見たおおよそのコストを見積もるために [`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif) で使用されます。

仮想索引を使うと、索引をディスクにマテリアライズするコストをかける前に、候補となる索引を評価できます。

<div id="create-hypothetical-index">
  ## CREATE 仮想索引
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

構文は `ALTER TABLE ... ADD INDEX` と同じですが、索引が構築されたり書き込まれたりすることはありません。現在のセッション内に索引定義だけが保存されます。

* `name` — 索引名。このセッションでは `(database, table)` 内で一意である必要があります。
* `expression` — 索引を作成するカラムまたは式。
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`。`text` と `vector_similarity` はサポートされておらず、`CREATE` 時に拒否されます。これは、実際の `ALTER TABLE ... ADD INDEX` の検証が、session-only store では再現できないテーブルレベルの設定に依存しているためです。
* `GRANULARITY value` — 1 つのインデックスグラニュールに含まれるデータグラニュール数。デフォルトは 1 です。

ターゲットテーブルは、`Atomic` データベース内の `MergeTree` ファミリーのテーブルである必要があります (UUID を持っている必要があります) 。UUID を持たないテーブル — たとえば従来の `Ordinary` データベース内のテーブルや旧構文の `MergeTree` — は拒否されます。これは、セッションストアが仮想索引をテーブル UUID をキーとして管理するためです。

**例**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## EXPLAIN WHATIF を使った仮想索引の評価
</div>

仮想索引は、定義しただけでは何も起こりません。クエリにどのような影響があるかを確認するには、代表的な `SELECT` に対して [`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif) を実行します。エスティメータは、各候補索引の適用可否、読み取るマーク数、推定されるスキップ率、およびその推定がどのように行われたか (`empirical`、`statistical`、または `applicability_only`) を報告します。

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

結果:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` はテーブルの平均行サイズに基づく推定値であるため、正確な値はストレージや圧縮によって変動します。

インメモリの実測スキャンをスキップし、代わりに [カラム STATISTICS](/ja/engines/table-engines/mergetree-family/mergetree#column-statistics) から推定するには、まず該当するカラムにそれらを定義し (デフォルトでは無効です) 、materialize ミューテーションが完了するのを待ってから、empirical パスを無効にします:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

出力スキーマ全体と設定については、[`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif) のリファレンスを参照してください。

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

現在のセッションから仮想索引を削除します。

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

現在のセッションで定義されているすべての仮想索引を、テーブルに関係なく削除します。

<div id="scope-and-lifetime">
  ## スコープと有効期間
</div>

* 仮想索引は **現在のセッション** でのみ有効で、他のセッションからは見えず、セッションが終了すると破棄されます。
* 仮想索引を定義または削除しても、実際の索引が構築されることはなく、テーブルに対する通常のクエリにも影響しません。実測型の `EXPLAIN WHATIF` では、候補となる索引をメモリ内に構築するためにテーブルデータを読み取ります。そのスキャンはセッションの読み取り制限とクォータに計上されます。
* 現在のセッションの仮想索引は [`system.hypothetical_indexes`](/ja/operations/system-tables/hypothetical_indexes) で確認できます。

<div id="limitations">
  ## 制限事項
</div>

`text` と `vector_similarity` の候補は、実際の検証がセッション限定ストアでは再現できないテーブルレベルの設定に依存するため、`CREATE HYPOTHETICAL INDEX` の時点で拒否されます。

`EXPLAIN WHATIF` は、`FINAL` を含むクエリに対しては `status: not_applicable` を報告し (スキップ索引のプルーニングが `PrimaryKeyExpand` と相互作用するため) 、クエリが projection から返される場合は `NOT_IMPLEMENTED` エラーになります (親テーブルの索引は projection parts にはマテリアライズされません) 。

経験的な `skip_ratio` は**上限値**です。これは、生き残った各グラニュールを個別に数えるものであり、seek ギャップの統合 (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`) や、選言 (`OR`) 述語の下で候補と既存のスキップ索引を組み合わせる場合はモデル化していません。そのため、実際にマテリアライズされた索引では、わずかに多く読み込まれることもあれば、推定ではプルーニングされないケースでプルーニングされることもあります。

<div id="required-privileges">
  ## 必要な権限
</div>

`CREATE HYPOTHETICAL INDEX` には、索引式で参照されるカラムに対する `SELECT` が必要です。これは `EXPLAIN WHATIF` が実際にそれらのカラムを読み取るためで、カラムレベルの `SELECT` (たとえば `GRANT SELECT(b)`) で十分です。

`DROP HYPOTHETICAL INDEX` と `DROP ALL HYPOTHETICAL INDEXES` には追加の権限は不要です。これらはセッションローカルのストアからエントリを削除するだけです。

<div id="see-also">
  ## 関連項目
</div>

* [`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/ja/operations/system-tables/hypothetical_indexes)
* [データスキッピングインデックス](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)