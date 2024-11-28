---
title: BigQuery と ClickHouse Cloud の比較
slug: /ja/migrations/bigquery
description: BigQuery と ClickHouse Cloud の違い
keywords: [移行, データ, etl, elt, bigquery]
---

# BigQuery と ClickHouse Cloud: 同等概念と異なる概念

## リソースの組織化

ClickHouse Cloud 内のリソースの組織化は、[BigQuery のリソース階層](https://cloud.google.com/bigquery/docs/resource-hierarchy) に似ています。以下の ClickHouse Cloud のリソース階層を示す図を基にした特定の違いを説明します。

<img src={require('../images/bigquery-1.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '600px'}} />

<br />

### 組織

BigQuery と同様に、組織は ClickHouse Cloud リソース階層のルートノードです。ClickHouse Cloud アカウントで最初に設定したユーザーは、ユーザーが所有する組織に自動的に割り当てられます。ユーザーは他のユーザーをその組織に招待することができます。

### BigQuery プロジェクト vs ClickHouse Cloud サービス

組織内では、ClickHouse Cloud でのデータがサービスに関連付けられるため、BigQuery プロジェクトに緩やかに相当するサービスを作成できます。ClickHouse Cloud では[いくつかのサービスタイプが利用可能](/ja/cloud/manage/service-types)です。各 ClickHouse Cloud サービスは特定の地域にデプロイされ、以下を含みます：

1. コンピュートノードのグループ（現在、開発ティアサービスには 2 ノード、運用ティアサービスには 3 ノード）。これらのノードに対して、ClickHouse Cloud は[垂直および水平スケーリング](/ja/manage/scaling#vertical-and-horizontal-scaling)の両方を手動および自動でサポートしています。
2. サービスがすべてのデータを保存するオブジェクトストレージフォルダ。
3. エンドポイント（または ClickHouse Cloud UI コンソールを通じて作成される複数のエンドポイント）- サービスに接続するためのサービス URL（例：`https://dv2fzne24g.us-east-1.aws.clickhouse.cloud:8443`）。

### BigQuery データセット vs ClickHouse Cloud データベース

ClickHouse はテーブルをデータベースに論理的にグループ化します。BigQuery データセットと同様に、ClickHouse データベースはテーブルデータへのアクセスを組織し制御する論理コンテナです。

### BigQuery フォルダ

ClickHouse Cloud には現在、BigQuery フォルダに相当する概念はありません。

### BigQuery スロット予約とクォータ

BigQuery スロット予約と同様に、ClickHouse Cloud では[垂直および水平のオートスケーリングを設定](/ja/manage/scaling#configuring-vertical-auto-scaling)できます。垂直オートスケーリングでは、サービスのコンピュートノードのメモリと CPU コアの最小および最大サイズを設定することができます。この設定は、初期のサービス作成のフローでも利用可能です。各コンピュートノードは同じサイズを持っています。[水平スケーリング](/ja/manage/scaling#self-serve-horizontal-scaling)を使用して、サービス内のコンピュートノードの数を変更できます。

さらに、BigQuery クォータに類似して、ClickHouse Cloud は並行制御、メモリ使用量制限、および I/O スケジューリングを提供し、クエリをワークロードクラスに隔離することを可能にします。特定のワークロードクラスに対する共有リソース（CPU コア、DRAM、ディスクおよびネットワーク I/O）の制限を設定することで、これらのクエリが他の重要なビジネスクエリに影響を与えないようにします。並行制御は、多数の同時クエリがある状況でのスレッドの過剰なサブスクリプションを防ぎます。

ClickHouse は、サーバ、ユーザー、およびクエリレベルでメモリアロケーションのバイトサイズを追跡し、柔軟なメモリ使用量制限を設定できます。メモリオーバーコミットにより、クエリは保証されたメモリを超える追加の空きメモリを使用できる一方で、他のクエリのメモリ制限を保証します。加えて、集約、ソート、および結合句のためのメモリ使用量を制限し、メモリ制限が超えられた場合には外部アルゴリズムにフォールバックすることができます。

最後に、I/O スケジューリングによりユーザーは、最大帯域幅、インフライトリクエスト、およびポリシーに基づいて、ワークロードクラスのローカルおよびリモートディスクアクセスを制限することができます。

### 権限

ClickHouse Cloud は[クラウドコンソール](/ja/get-started/sql-console)およびデータベースを通じて[ユーザーアクセスを管理](/ja/cloud/security/cloud-access-management)します。コンソールアクセスは [clickhouse.cloud](https://clickhouse.cloud) のユーザーインターフェイスを通じて管理されます。データベースアクセスは、データベースのユーザーアカウントとロールを通じて管理されます。さらに、コンソールのユーザーには、SQL コンソールを通じてデータベースと対話することを可能にするデータベース内の役割を付与することができます。

## データ型

ClickHouse は数値に関してより詳細な精度を提供します。たとえば、BigQuery は[INT64, NUMERIC, BIGNUMERIC および FLOAT64](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types)の数値型を提供しています。これに対して ClickHouse は、デシマル、フロート、整数に対して複数の精度を提供しています。これらのデータ型により、ClickHouse ユーザーはストレージおよびメモリのオーバーヘッドを最適化し、より高速なクエリとリソース消費を低減します。以下に、各 BigQuery 型に対する ClickHouse の同等型を示します：

| BigQuery | ClickHouse |
|----------|------------|
| [ARRAY](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type)    | [Array(t)](/ja/sql-reference/data-types/array)   |
| [NUMERIC](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types)  | [Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)](/ja/sql-reference/data-types/decimal)    |
| [BIG NUMERIC](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types) | [Decimal256(S)](/ja/sql-reference/data-types/decimal) |
| [BOOL](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#boolean_type)     | [Bool](/ja/sql-reference/data-types/boolean)       |
| [BYTES](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#bytes_type)    | [FixedString](/ja/sql-reference/data-types/fixedstring) |
| [DATE](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#date_type)     | [Date32](/ja/sql-reference/data-types/date32) (より狭い範囲) |
| [DATETIME](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type) | [DateTime](/ja/sql-reference/data-types/datetime), [DateTime64](/ja/sql-reference/data-types/datetime64) (狭い範囲で高精度) |
| [FLOAT64](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_types)  | [Float64](/ja/sql-reference/data-types/float)    |
| [GEOGRAPHY](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#geography_type) | [Geo Data Types](/ja/sql-reference/data-types/float) |
| [INT64](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_types)    | [UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256](/ja/sql-reference/data-types/int-uint) |
| [INTERVAL](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_types) | 該当なし - [式としてサポート](/ja/sql-reference/data-types/special-data-types/interval#usage-remarks)または[関数を通じて](/ja/sql-reference/functions/date-time-functions#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters) |
| [JSON](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#json_type)     | [JSON](/ja/integrations/data-formats/json/overview#relying-on-schema-inference)       |
| [STRING](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#string_type)   | [String (bytes)](/ja/sql-reference/data-types/string) |
| [STRUCT](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct)   | [Tuple](/ja/sql-reference/data-types/tuple), [Nested](/ja/sql-reference/data-types/nested-data-structures/nested) |
| [TIME](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_type)     | [DateTime64](/ja/sql-reference/data-types/datetime64) |
| [TIMESTAMP](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp_type) | [DateTime64](/ja/sql-reference/data-types/datetime64) |

ClickHouse 型が複数のオプションで提示された場合、データの実際の範囲を考慮し、必要最低限のものを選択してください。また、さらなる圧縮のために[適切なコーデック](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)の利用を検討してください。

## クエリ高速化技術

### 主キーおよび外部キーと主インデックス

BigQuery では、テーブルに[主キーおよび外部キーの制約](https://cloud.google.com/bigquery/docs/information-schema-table-constraints)を持たせることができます。通常、主キーおよび外部キーは、リレーショナルデータベースでデータの整合性を確保するために使用されます。主キーの値は通常、各行のユニークな値であり `NULL` ではありません。外部キー値は、主キー表の主キー列で存在するか `NULL` でなければなりません。BigQuery では、これらの制約は強制されませんが、クエリオプティマイザはこの情報を用いてクエリの最適化を行うことができます。

ClickHouse でもテーブルに主キーが設定可能です。BigQuery と同様に、ClickHouse はテーブルの主キーカラムの値の一意性を強制しません。BigQuery とは異なり、テーブルのデータは主キーカラムによって[ディスクに順序付けされて](/ja/optimize/sparse-primary-indexes#data-is-stored-on-disk-ordered-by-primary-key-columns)保存されます。クエリオプティマイザはこの並び順を用いてリゾートを防ぎ、結合時のメモリ使用量を最小化し、リミット句に対してショートサーキットを可能にします。BigQuery と異なり、ClickHouse は自動的に[（スパースな）プライマリインデックス](/ja/optimize/sparse-primary-indexes#an-index-design-for-massive-data-scales)を主キーのカラムの値に基づいて作成します。このインデックスは、主キーカラムのフィルタを含むすべてのクエリを高速化するために使用されます。ClickHouse は現在、外部キーの制約をサポートしていません。

## セカンダリインデックス（ClickHouse のみに存在）

テーブルの主キーカラムとは異なるカラムにセカンダリインデックスを作成することが ClickHouse ではできます。ClickHouse はクエリの種類に応じたさまざまなセカンダリインデックスを提供しています：

- **ブルームフィルターインデックス**：
  - 等価条件（例：=、IN）でクエリを高速化するために使用されます。
  - 値がデータブロックに存在するかどうかを決定するための確率的なデータ構造を使用します。
- **トークンブルームフィルターインデックス**：
  - トークン化された文字列、およびフルテキスト検索クエリに適したブルームフィルターインデックス。
- **Min-Max インデックス**：
  - 各データ部分のカラムの最小値と最大値を維持します。指定された範囲に入らないデータ部分の読み取りをスキップするのに役立ちます。

## 検索インデックス

BigQuery の[検索インデックス](https://cloud.google.com/bigquery/docs/search-index)と同様に、ClickHouse のテーブルには文字列値を有するカラムに対して[全文検索インデックス](/ja/engines/table-engines/mergetree-family/invertedindexes)を作成することができます。

## ベクトルインデックス

BigQuery は最近、[ベクトルインデックス](https://cloud.google.com/bigquery/docs/vector-index)をプリ GA 機能として導入しました。同様に、ClickHouse にはベクトル検索の使用ケースを高速化するエクスペリメンタルな[インデックスのサポート](/ja/engines/table-engines/mergetree-family/annindexes)があります。

## パーティショニング
　　
BigQuery と同様に、ClickHouse は大規模テーブルのパフォーマンスと管理性を向上させるために、テーブルをパーティションと呼ばれる小さく管理しやすい部分に分割するためのテーブルパーティショニングを使用します。ClickHouse のパーティショニングについての詳細は[こちら](/ja/engines/table-engines/mergetree-family/custom-partitioning-key)で説明しています。

## クラスタリング

BigQuery のクラスタリングは、指定されたカラムの値に基づいてテーブルデータを自動的にソートし、適切なサイズのブロックに配置します。クラスタリングはクエリのパフォーマンスを向上させ、BigQuery がクエリの実行コストをより良く見積もることを可能にします。クラスタリングされたカラムにより、クエリは不要なデータのスキャンを排除します。

ClickHouse では、データはテーブルの主キーカラムに基づいてディスク上に自動的に[クラスタリング](/ja/optimize/sparse-primary-indexes#data-is-stored-on-disk-ordered-by-primary-key-columns)され、クエリが主インデックスデータ構造を利用するために迅速に検索またはプルーニングされ得るブロックに論理的に整理されます。

## マテリアライズドビュー

BigQuery と ClickHouse の両方がマテリアライズドビューをサポートしています。これは、パフォーマンスと効率を向上させるために、ベーステーブルに対しての変換クエリの結果に基づいた事前計算された結果です。

## マテリアライズドビューのクエリ

BigQuery マテリアライズドビューは直接クエリされるか、オプティマイザによってベーステーブルをクエリする際に使用されます。ベーステーブルへの変更がマテリアライズドビューを無効にする可能性がある場合、データはベーステーブルから直接読み取られます。ベーステーブルへの変更がマテリアライズドビューを無効にしない場合、残りのデータはマテリアライズドビューから読み取られ、変更のみがベーステーブルから読み取られます。

ClickHouse では、マテリアライズドビューは直接クエリのみ可能です。しかし、BigQuery とは異なり（マテリアライズドビューは通常 5 分以内に自動的に更新され、[30 分おき](https://cloud.google.com/bigquery/docs/materialized-views-manage#refresh)より頻繁には更新されません）、ClickHouse のマテリアライズドビューは常にベーステーブルと同期しています。

**マテリアライズドビューの更新**

BigQuery は定期的にベーステーブルに対するビューの変換クエリを実行することでマテリアライズドビューを完全に更新します。更新間隔には、マテリアライズドビューのデータをベーステーブルの新しいデータと組み合わせて一貫性のあるクエリ結果を提供しながら依然としてマテリアライズドビューを使用します。

ClickHouse では、マテリアライズドビューは増分で更新されます。この増分更新機構は高いスケーラビリティと低い計算コストを提供します。特にベーステーブルに数十億または数兆の行が含まれるシナリオに対して設計されています。マテリアライズドビューをベーステーブル全体から繰り返し更新する代わりに、ClickHouse は（新たに挿入されたベーステーブルの行の）値のみから部分結果を計算します。この部分結果は、バックグラウンドで以前に計算された部分結果とインクリメンタルにつながります。これにより、完全なベーステーブルから繰り返しマテリアライズドビューを更新する場合に比べて計算コストが劇的に低下します。

## トランザクション

ClickHouse とは対照的に、BigQuery はセッションを使用する際、単一のクエリ内または複数のクエリにまたがるマルチステートメントトランザクションをサポートしています。マルチステートメントトランザクションにより、1 つまたは複数のテーブルで行の挿入または削除などの変異操作を実行し、変更を原子的にコミットまたはロールバックすることができます。マルチステートメントトランザクションは[ClickHouse のロードマップに2024年の予定](https://github.com/ClickHouse/ClickHouse/issues/58392)としてあります。

## 集約関数

BigQuery と比較して、ClickHouse はかなり多くの組み込み集約関数を提供しています：

- BigQuery は [18 の集約関数](https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions) と、[4 つの近似集約関数](https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions) を持っています。
- ClickHouse は [150 以上の組み込み集計関数](https://clickhouse.com/docs/ja/sql-reference/aggregate-functions/reference)を備えており、組み込み集計関数の挙動を[拡張](/ja/sql-reference/aggregate-functions/combinators)する強力な[集約コンビネータ](/ja/sql-reference/aggregate-functions/combinators)も備えています。たとえば、150 以上の組み込み集計関数をテーブルの行ではなく配列に適用することができ、[-Array サフィックス](/ja/sql-reference/aggregate-functions/combinators#-array)を使用するだけで済みます。[-Map サフィックス](/ja/sql-reference/aggregate-functions/combinators#-map)を使用すれば、マップに対して任意の集約関数を適用できます。[-ForEach サフィックス](/ja/sql-reference/aggregate-functions/combinators#-foreach)を使用して配列に対して任意の集約関数を適用することが可能です。

## データソースとファイルフォーマット

BigQuery と比較して、ClickHouse は圧倒的に多くのファイルフォーマットとデータソースをサポートしています：

- ClickHouse には、あらゆるデータソースから 90 以上のファイルフォーマットでデータをロードするネイティブサポートがあります。
- BigQuery は 5 つのファイルフォーマットと 19 のデータソースをサポートしています。

## SQL 言語の特徴

ClickHouse は標準 SQL を提供しており、分析タスクに対してよりフレンドリーな多くの拡張機能と改善点を備えています。例えば、ClickHouse SQL では [ラムダ関数](/ja/sql-reference/functions#higher-order-functions---operator-and-lambdaparams-expr-function) および高次関数をサポートしており、配列に変換を適用する際にアンネス化/爆発処理を行う必要がありません。これはBigQueryのような他のシステムに対する大きな利点です。

## 配列

BigQuery の 8 つの配列関数と比較して、ClickHouse には幅広い問題をエレガントかつシンプルにモデリングおよび解決するための 80 以上の[組み込み配列関数](/ja/sql-reference/functions/array-functions)があります。

ClickHouse における典型的なデザインパターンは、特定のテーブルの行の値を（暫定的に）配列に変換するために[`groupArray`](/ja/sql-reference/aggregate-functions/reference/grouparray)集計関数を使用することです。これにより、配列関数を通じて便利に処理し、[`arrayJoin`](/ja/sql-reference/functions/array-join)集計関数を通じてその結果を個別のテーブル行に戻すことができます。

ClickHouse SQL は[高次ラムダ関数](/ja/sql-reference/functions#higher-order-functions---operator-and-lambdaparams-expr-function)をサポートしているため、多くの高度な配列操作がビルトイン配列関数の一つを単に呼び出すことで実現可能です。多くの場合、BigQuery ではそれが必要です。BigQuery では、配列の[フィルタリング](https://cloud.google.com/bigquery/docs/arrays#filtering_arrays)や[ジッピング](https://cloud.google.com/bigquery/docs/arrays#zipping_arrays)を行う際に、配列表を一時的に元に戻すことが必要でしたが、ClickHouse ではこれらの操作は高次関数[`arrayFilter`](/ja/sql-reference/functions/array-functions#arrayfilterfunc-arr1-)および[`arrayZip`](/ja/sql-reference/functions/array-functions#arrayzip)の単なる関数呼び出しです。

以下に、BigQuery から ClickHouse への配列操作のマッピングを示します：

| BigQuery | ClickHouse |
|----------|------------|
| [ARRAY_CONCAT](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat) | [arrayConcat](/ja/sql-reference/functions/array-functions#arrayconcat) |
| [ARRAY_LENGTH](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_length) | [length](/ja/sql-reference/functions/array-functions#length) |
| [ARRAY_REVERSE](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_reverse) | [arrayReverse](/ja/sql-reference/functions/array-functions#arrayreverse) |
| [ARRAY_TO_STRING](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string) | [arrayStringConcat](/ja/sql-reference/functions/splitting-merging-functions#arraystringconcat) |
| [GENERATE_ARRAY](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array) | [range](/ja/sql-reference/functions/array-functions#rangeend-rangestart--end--step) |

**サブクエリの各行に1つの要素が含まれる配列を作成**

_BigQuery_

[ARRAY 関数](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array)

```sql
SELECT ARRAY
  (SELECT 1 UNION ALL
   SELECT 2 UNION ALL
   SELECT 3) AS new_array;

/*-----------*
 | new_array |
 +-----------+
 | [1, 2, 3] |
 *-----------*/
```

_ClickHouse_

[groupArray](/ja/sql-reference/aggregate-functions/reference/grouparray) 集計関数

```sql
SELECT groupArray(*) AS new_array
FROM
(
    SELECT 1
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3
)
   ┌─new_array─┐
1. │ [1,2,3]   │
   └───────────┘
```

**配列をセットに変換**

_BigQuery_

[UNNEST](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator) オペレーター

```sql
SELECT *
FROM UNNEST(['foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'])
  AS element
WITH OFFSET AS offset
ORDER BY offset;

/*----------+--------*
 | element  | offset |
 +----------+--------+
 | foo      | 0      |
 | bar      | 1      |
 | baz      | 2      |
 | qux      | 3      |
 | corge    | 4      |
 | garply   | 5      |
 | waldo    | 6      |
 | fred     | 7      |
 *----------+--------*/
```

_ClickHouse_

[ARRAY JOIN](/ja/sql-reference/statements/select/array-join) 節

```sql
WITH ['foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'] AS values
SELECT element, num-1 AS offset
FROM (SELECT values AS element) AS subquery
ARRAY JOIN element, arrayEnumerate(element) AS num;

/*----------+--------*
 | element  | offset |
 +----------+--------+
 | foo      | 0      |
 | bar      | 1      |
 | baz      | 2      |
 | qux      | 3      |
 | corge    | 4      |
 | garply   | 5      |
 | waldo    | 6      |
 | fred     | 7      |
 *----------+--------*/
```

**日付を返す配列**

_BigQuery_

[GENERATE_DATE_ARRAY](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array) 関数

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;

/*--------------------------------------------------*
 | example                                          |
 +--------------------------------------------------+
 | [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
 *--------------------------------------------------*/
```

[range](https://clickhouse.com/docs/ja/sql-reference/functions/array-functions#rangeend-rangestart--end--step) + [arrayMap](/ja/sql-reference/functions/array-functions#arraymapfunc-arr1-) 関数

_ClickHouse_

```sql
SELECT arrayMap(x -> (toDate('2016-10-05') + x), range(toUInt32((toDate('2016-10-08') - toDate('2016-10-05')) + 1))) AS example

   ┌─example───────────────────────────────────────────────┐
1. │ ['2016-10-05','2016-10-06','2016-10-07','2016-10-08'] │
   └───────────────────────────────────────────────────────┘
```

**タイムスタンプを返す配列**

_BigQuery_

[GENERATE_TIMESTAMP_ARRAY](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array) 関数

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00',
                                INTERVAL 1 DAY) AS timestamp_array;

/*--------------------------------------------------------------------------*
 | timestamp_array                                                          |
 +--------------------------------------------------------------------------+
 | [2016-10-05 00:00:00+00, 2016-10-06 00:00:00+00, 2016-10-07 00:00:00+00] |
 *--------------------------------------------------------------------------*/
```

_ClickHouse_

[range](https://clickhouse.com/docs/ja/sql-reference/functions/array-functions#rangeend-rangestart--end--step) + [arrayMap](/ja/sql-reference/functions/array-functions#arraymapfunc-arr1-) 関数

```sql
SELECT arrayMap(x -> (toDateTime('2016-10-05 00:00:00') + toIntervalDay(x)), range(dateDiff('day', toDateTime('2016-10-05 00:00:00'), toDateTime('2016-10-07 00:00:00')) + 1)) AS timestamp_array

Query id: b324c11f-655b-479f-9337-f4d34fd02190

   ┌─timestamp_array─────────────────────────────────────────────────────┐
1. │ ['2016-10-05 00:00:00','2016-10-06 00:00:00','2016-10-07 00:00:00'] │
   └─────────────────────────────────────────────────────────────────────┘
```

**配列のフィルタリング**

_BigQuery_

[UNNEST](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator) オペレーターを通じた一時的な配列をテーブルに戻すことが必要 

```sql
WITH Sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x
        WHERE x < 5) AS doubled_less_than_five
FROM Sequences;

/*------------------------*
 | doubled_less_than_five |
 +------------------------+
 | [0, 2, 2, 4, 6]        |
 | [4, 8]                 |
 | []                     |
 *------------------------*/
```

_ClickHouse_

[arrayFilter](/ja/sql-reference/functions/array-functions#arrayfilterfunc-arr1-) 関数

```sql
WITH Sequences AS
    (
        SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
        UNION ALL
        SELECT [2, 4, 8, 16, 32] AS some_numbers
        UNION ALL
        SELECT [5, 10] AS some_numbers
    )
SELECT arrayMap(x -> (x * 2), arrayFilter(x -> (x < 5), some_numbers)) AS doubled_less_than_five
FROM Sequences;
   ┌─doubled_less_than_five─┐
1. │ [0,2,2,4,6]            │
   └────────────────────────┘
   ┌─doubled_less_than_five─┐
2. │ []                     │
   └────────────────────────┘
   ┌─doubled_less_than_five─┐
3. │ [4,8]                  │
   └────────────────────────┘
```

**配列のジッピング**

_BigQuery_

[UNNEST](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator) オペレーターを通じた配列をテーブルすることが必要

```sql
WITH
  Combinations AS (
    SELECT
      ['a', 'b'] AS letters,
      [1, 2, 3] AS numbers
  )
SELECT
  ARRAY(
    SELECT AS STRUCT
      letters[SAFE_OFFSET(index)] AS letter,
      numbers[SAFE_OFFSET(index)] AS number
    FROM Combinations
    CROSS JOIN
      UNNEST(
        GENERATE_ARRAY(
          0,
          LEAST(ARRAY_LENGTH(letters), ARRAY_LENGTH(numbers)) - 1)) AS index
    ORDER BY index
  );

/*------------------------------*
 | pairs                        |
 +------------------------------+
 | [{ letter: "a", number: 1 }, |
 |  { letter: "b", number: 2 }] |
 *------------------------------*/
```

_ClickHouse_

[arrayZip](/ja/sql-reference/functions/array-functions#arrayzip) 関数

```sql
WITH Combinations AS
    (
        SELECT
            ['a', 'b'] AS letters,
            [1, 2, 3] AS numbers
    )
SELECT arrayZip(letters, arrayResize(numbers, length(letters))) AS pairs
FROM Combinations;
   ┌─pairs─────────────┐
1. │ [('a',1),('b',2)] │
   └───────────────────┘
```

**配列の集計**

_BigQuery_

[UNNEST](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator) オペレーターを通じた配列テーブルへの変換が必要

```sql
WITH Sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
  (SELECT SUM(x)
   FROM UNNEST(s.some_numbers) AS x) AS sums
FROM Sequences AS s;

/*--------------------+------*
 | some_numbers       | sums |
 +--------------------+------+
 | [0, 1, 1, 2, 3, 5] | 12   |
 | [2, 4, 8, 16, 32]  | 62   |
 | [5, 10]            | 15   |
 *--------------------+------*/
```

_ClickHouse_

[arraySum](/ja/sql-reference/functions/array-functions#arraysum), [arrayAvg](/ja/sql-reference/functions/array-functions#arrayavg), … 関数、または任意の 90 以上の集約関数名を[arrayReduce](/ja/sql-reference/functions/array-functions#arrayreduce)関数の引数として使用

```sql
WITH Sequences AS
    (
        SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
        UNION ALL
        SELECT [2, 4, 8, 16, 32] AS some_numbers
        UNION ALL
        SELECT [5, 10] AS some_numbers
    )
SELECT
    some_numbers,
    arraySum(some_numbers) AS sums
FROM Sequences;
   ┌─some_numbers──┬─sums─┐
1. │ [0,1,1,2,3,5] │   12 │
   └───────────────┴──────┘
   ┌─some_numbers──┬─sums─┐
2. │ [2,4,8,16,32] │   62 │
   └───────────────┴──────┘
   ┌─some_numbers─┬─sums─┐
3. │ [5,10]       │   15 │
   └──────────────┴──────┘
```
