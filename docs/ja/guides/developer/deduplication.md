---
slug: /ja/guides/developer/deduplication
sidebar_label: 重複排除戦略
sidebar_position: 3
description: 頻繁にupsert、更新、および削除を実行する必要がある場合は、重複排除を使用します。
---

# 重複排除戦略

**重複排除**とは、***データセットの重複行を削除するプロセス***を指します。OLTPデータベースでは、各行に一意の主キーがあるため、これを簡単に行うことができますが、その代わりに挿入が遅くなります。挿入された各行は最初に検索され、存在する場合は置き換える必要があります。

ClickHouseはデータ挿入のスピードを追求して設計されています。ストレージファイルは不変であり、ClickHouseは行を挿入する前に既存の主キーを確認しないため、重複排除はもう少し手間がかかります。また、重複排除は即時には行われず、**最終的**であるため、以下のような副作用があります：

- 任意の時点で、テーブルにはまだ重複（同じソートキーを持つ行）が存在する可能性があります
- 重複行の実際の削除は、パーツのマージ時に行われます
- クエリでは、重複の可能性を考慮する必要があります

<div class='transparent-table'>

|||
|------|----|
|<img src={require('./images/Deduplication.png').default} class="image" alt="Cassandra logo" style={{width: '16rem', 'background-color': 'transparent'}}/>|ClickHouseは重複排除やその他多くのトピックに関する無料トレーニングを提供しています。[データの削除と更新トレーニングモジュール](https://learn.clickhouse.com/visitor_catalog_class/show/1328954/?utm_source=clickhouse&utm_medium=docs)は素晴らしい出発点です。|

</div>

## 重複排除のオプション

ClickHouseにおける重複排除は、以下のテーブルエンジンを使用して実装されます：

1. `ReplacingMergeTree`テーブルエンジン：このテーブルエンジンを使用すると、同じソートキーを持つ重複行がマージ時に削除されます。`ReplacingMergeTree`は、クエリが最後に挿入された行を返すようなupsert動作をエミュレートするのに適しています。

2. 行の折りたたみ：`CollapsingMergeTree`および`VersionedCollapsingMergeTree`テーブルエンジンは、既存の行が"キャンセル"されて新しい行が挿入されるロジックを利用します。これらは`ReplacingMergeTree`よりも複雑ですが、クエリや集計を書く際にデータがまだマージされているかどうかを気にせずに済むため、クエリがシンプルになることがあります。データを頻繁に更新する必要がある場合、これらのテーブルエンジンが役立ちます。

以下でこれらの技術を詳しく説明します。詳細については、[データの削除と更新トレーニングモジュール](https://learn.clickhouse.com/visitor_catalog_class/show/1328954/?utm_source=clickhouse&utm_medium=docs)をご覧ください。

## ReplacingMergeTreeを使用したUpserts

ハッカーニュースのコメントを含むテーブルを例に見てみましょう。このテーブルは、コメントの表示回数を表す`views`カラムを持っています。新しい記事が公開されると新しい行を挿入し、表示回数が増えた場合は1日ごとに新しい行をupsertするとします：

```sql
CREATE TABLE hackernews_rmt (
    id UInt32,
    author String,
    comment String,
    views UInt64
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (author, id)
```

2行を挿入してみましょう：

```sql
INSERT INTO hackernews_rmt VALUES
   (1, 'ricardo', 'This is post #1', 0),
   (2, 'ch_fan', 'This is post #2', 0)
```

`views`カラムを更新するために、同じ主キーで新しい行を挿入します（`views`カラムの新しい値に注目してください）：

```sql
INSERT INTO hackernews_rmt VALUES
   (1, 'ricardo', 'This is post #1', 100),
   (2, 'ch_fan', 'This is post #2', 200)
```

現在、テーブルには4行あります：

```sql
SELECT *
FROM hackernews_rmt
```

```response
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │     0 │
│  1 │ ricardo │ This is post #1 │     0 │
└────┴─────────┴─────────────────┴───────┘
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │   200 │
│  1 │ ricardo │ This is post #1 │   100 │
└────┴─────────┴─────────────────┴───────┘
```

上の出力の別々のボックスは、実際のマージがまだ行われておらず、重複行がまだ削除されていないことを示しています。`SELECT`クエリで`FINAL`キーワードを使用して、クエリ結果の論理的なマージを行います：

```sql
SELECT *
FROM hackernews_rmt
FINAL
```

```response
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │   200 │
│  1 │ ricardo │ This is post #1 │   100 │
└────┴─────────┴─────────────────┴───────┘
```

結果には2行だけが含まれ、最後に挿入された行が返されます。

:::note
`FINAL`を使用することは、データ量が少ない場合には問題ありません。大量のデータを扱う場合、`FINAL`の使用は最良の選択肢ではないかもしれません。カラムの最新値を見つけるためのより良い選択肢について議論しましょう…
:::

### FINALを避ける

2つのユニークな行の`views`カラムを再度更新しましょう：

```sql
INSERT INTO hackernews_rmt VALUES
   (1, 'ricardo', 'This is post #1', 150),
   (2, 'ch_fan', 'This is post #2', 250)
```

テーブルには現在6行ありますが、実際のマージはまだ行われていません（ `FINAL`を使用したクエリ時間のマージのみ）。

```sql
SELECT *
FROM hackernews_rmt
```

```response
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │   200 │
│  1 │ ricardo │ This is post #1 │   100 │
└────┴─────────┴─────────────────┴───────┘
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │     0 │
│  1 │ ricardo │ This is post #1 │     0 │
└────┴─────────┴─────────────────┴───────┘
┌─id─┬─author──┬─comment─────────┬─views─┐
│  2 │ ch_fan  │ This is post #2 │   250 │
│  1 │ ricardo │ This is post #1 │   150 │
└────┴─────────┴─────────────────┴───────┘
```

`FINAL`を使用する代わりに、ビジネスロジックを使用しましょう - `views`カラムは常に増加すると分かっているので、最大値を持つ行を選択するために`max`関数を使用して、望むカラムでグループ化します：

```sql
SELECT
    id,
    author,
    comment,
    max(views)
FROM hackernews_rmt
GROUP BY (id, author, comment)
```

```response
┌─id─┬─author──┬─comment─────────┬─max(views)─┐
│  2 │ ch_fan  │ This is post #2 │        250 │
│  1 │ ricardo │ This is post #1 │        150 │
└────┴─────────┴─────────────────┴────────────┘
```

上記のクエリに示されているようにグループ化することで、`FINAL`キーワードを使用するよりもクエリパフォーマンスの観点で効率的になることがあります。

私たちの[データの削除と更新トレーニングモジュール](https://learn.clickhouse.com/visitor_catalog_class/show/1328954/?utm_source=clickhouse&utm_medium=docs)では、この例をさらに展開し、`ReplacingMergeTree`を使用した`version`カラムの使用方法も紹介しています。

## カラムの頻繁な更新にCollapsingMergeTreeを使用する

カラムの更新は、既存の行を削除し、新しい値で置き換えることを伴います。これまでに見た通り、ClickHouseにおけるこの種の変更は_最終的に_発生します - マージ中に。多くの行を更新する場合、`ALTER TABLE..UPDATE`を回避し、既存データと共に新しいデータを挿入する方が実際には効率的である可能性があります。データが古いものか新しいものかを示すカラムを追加することができます…そして実際、古いデータを自動的に削除するという振る舞いを非常にうまく実装するテーブルエンジンがあります。それがどのように機能するかを見てみましょう。

外部システムを使用して、ハッカーニュースのコメントの表示回数を追跡し、数時間ごとにデータをClickHouseにプッシュするとします。古い行を削除し、新しい行が各ハッカーニュースコメントの新しい状態を表すようにしたいです。この振る舞いを実現するために、`CollapsingMergeTree`を使用することができます。

表示回数を格納するためのテーブルを定義しましょう：

```sql
CREATE TABLE hackernews_views (
    id UInt32,
    author String,
    views UInt64,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY (id, author)
```

`hackernews_views`テーブルには、signと呼ばれる`Int8`カラムがあり、**sign**カラムとして参照されます。signカラムの名前は任意ですが、`Int8`データ型である必要があります。また、`CollapsingMergeTree`テーブルのコンストラクタにカラム名が渡されています。

`CollapsingMergeTree`テーブルのsignカラムとは何でしょうか？これは行の_状態_を表し、signカラムは1または-1のみであることができます。その働き方を見てみましょう：

- 二つの行が同じ主キー（またはソート順が主キーとは異なる場合、そのソート順）を持つが、signカラムの値が異なる場合、最後に挿入された行の+1が状態行となり、他の行はお互いをキャンセルします
- お互いをキャンセルする行はマージ中に削除されます
- マッチするペアを持たない行は残ります

`hackernews_views`テーブルに行を追加しましょう。この主キーに対応する唯一の行であるため、その状態を1に設定します：

```sql
INSERT INTO hackernews_views VALUES
   (123, 'ricardo', 0, 1)
```

次に、viewsカラムを変更したいとします。既存の行をキャンセルする1行と、その行の新しい状態を含む1行の計2行を挿入します：

```sql
INSERT INTO hackernews_views VALUES
   (123, 'ricardo', 0, -1),
   (123, 'ricardo', 150, 1)
```

テーブルには現在、主キー(123, 'ricardo')を持つ3行があります：

```sql
SELECT *
FROM hackernews_views
```

```response
┌──id─┬─author──┬─views─┬─sign─┐
│ 123 │ ricardo │     0 │   -1 │
│ 123 │ ricardo │   150 │    1 │
└─────┴─────────┴───────┴──────┘
┌──id─┬─author──┬─views─┬─sign─┐
│ 123 │ ricardo │     0 │    1 │
└─────┴─────────┴───────┴──────┘
```

`FINAL`を追加すると、現在の状態行が返されることに注目してください：

```sql
SELECT *
FROM hackernews_views
FINAL
```

```response
┌──id─┬─author──┬─views─┬─sign─┐
│ 123 │ ricardo │   150 │    1 │
└─────┴─────────┴───────┴──────┘
```

しかし、当然のことながら、大きなテーブルに対して`FINAL`を使用することは推奨されていません。

:::note
この例で`views`カラムに渡された値は本当に必要なく、古い行の現在の`views`の値と一致している必要もありません。実際、主キーと-1だけで行をキャンセルすることができます：

```sql
INSERT INTO hackernews_views(id, author, sign) VALUES
   (123, 'ricardo', -1)
```
:::

## 複数スレッドからのリアルタイム更新

`CollapsingMergeTree`テーブルを使用すると、行はsignカラムを使って互いにキャンセルされ、行の状態は最後に挿入された行によって決定されます。しかし、行が異なるスレッドから順不同で挿入される場合、これは問題になる可能性があります。この場合、最後の行を使用することは機能しません。

ここで`VersionedCollapsingMergeTree`が役立ちます。`VersionedCollapsingMergeTree`は、`CollapsingMergeTree`と同様に行を折りたたみますが、最後に挿入された行ではなく、指定したバージョンカラムの最大値を持つ行を保持します。

例を見てみましょう。ハッカーニュースのコメントの表示回数を追跡し、データは頻繁に更新されます。レポートでは、マージを強制したり待つことなく最新の値を使用したいです。`CollapsedMergeTree`に似たテーブルから始めますが、行の状態のバージョンを格納するカラムを追加します：

```sql
CREATE TABLE hackernews_views_vcmt (
    id UInt32,
    author String,
    views UInt64,
    sign Int8,
    version UInt32
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
PRIMARY KEY (id, author)
```

このテーブルは`VersionsedCollapsingMergeTree`をエンジンとして使用し、**signカラム**と**versionカラム**を渡しています。このテーブルの働き方は次の通りです：

- 同じ主キーとバージョンを持ち、異なるsignを持つ各ペアの行を削除します
- 行が挿入された順序は関係ありません
- バージョンカラムが主キーパートでない場合、ClickHouseはそれを最後のフィールドとして主キーに暗黙的に追加します

クエリを記述するときに同様のロジックを使用します - 主キーでグループ化し、キャンセルされたがまだ削除されていない行を避ける巧妙なロジックを使用します。`hackernews_views_vcmt`テーブルにいくつかの行を追加しましょう：

```sql
INSERT INTO hackernews_views_vcmt VALUES
   (1, 'ricardo', 0, 1, 1),
   (2, 'ch_fan', 0, 1, 1),
   (3, 'kenny', 0, 1, 1)
```

次に、2つの行を更新し、1つを削除します。行をキャンセルするには、前のバージョン番号を含めてください（これは主キーの一部であるため）：

```sql
INSERT INTO hackernews_views_vcmt VALUES
   (1, 'ricardo', 0, -1, 1),
   (1, 'ricardo', 50, 1, 2),
   (2, 'ch_fan', 0, -1, 1),
   (3, 'kenny', 0, -1, 1),
   (3, 'kenny', 1000, 1, 2)
```

signカラムに基づいて値を加減算する同じクエリを実行します：

```sql
SELECT
    id,
    author,
    sum(views * sign)
FROM hackernews_views_vcmt
GROUP BY (id, author)
HAVING sum(sign) > 0
ORDER BY id ASC
```

結果は2行です：

```response
┌─id─┬─author──┬─sum(multiply(views, sign))─┐
│  1 │ ricardo │                         50 │
│  3 │ kenny   │                       1000 │
└────┴─────────┴────────────────────────────┘
```

テーブルのマージを強制します：

```sql
OPTIMIZE TABLE hackernews_views_vcmt
```

結果には2行だけが含まれます：

```sql
SELECT *
FROM hackernews_views_vcmt
```

```response
┌─id─┬─author──┬─views─┬─sign─┬─version─┐
│  1 │ ricardo │    50 │    1 │       2 │
│  3 │ kenny   │  1000 │    1 │       2 │
└────┴─────────┴───────┴──────┴─────────┘
```

`VersionedCollapsingMergeTree`テーブルは、複数のクライアントやスレッドからの行の挿入を行いつつ、重複排除を実装したい場合に非常に役立ちます。

## なぜ行が重複排除されていないのか？

挿入された行が重複排除されない理由の一つは、`INSERT`ステートメントで非冪等関数や式を使用している場合です。例えば、`createdAt DateTime64(3) DEFAULT now()`というカラムを使用して行を挿入すると、各行は一意のデフォルト値を持つため、必ずユニークになります。MergeTree / ReplicatedMergeTreeテーブルエンジンは、各挿入行が一意のチェックサムを生成するため、重複排除を認識しません。

この場合、バッチの行ごとに独自の`insert_deduplication_token`を指定して、同じバッチの複数の挿入が同じ行を再挿入することにならないようにすることができます。[`insert_deduplication_token`に関するドキュメント](/docs/ja/operations/settings/settings#insert_deduplication_token)をご覧ください。この設定の使用方法についてもっと詳しく知ることができます。
