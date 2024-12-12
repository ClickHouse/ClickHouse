---
slug: /ja/guides/developer/ttl
sidebar_label: 有効期限 (TTL)
sidebar_position: 2
keywords: [ttl, time to live, clickhouse, 古い, データ]
description: TTL (time-to-live) は、一定期間経過後に行やカラムを移動、削除、または集約する機能を指します。
---

# 有効期限 (TTL) を用いたデータ管理

## TTLの概要

TTL (time-to-live) は、一定期間経過後に行やカラムを移動、削除、または集約する機能を指します。「time-to-live」という表現は古いデータを削除することだけを意味しているように聞こえますが、TTLにはいくつかの用途があります：

- 古いデータの削除: 予想どおり、指定された時間経過後に行やカラムを削除できます
- ディスク間のデータ移動: 一定時間経過後にストレージボリューム間でデータを移動できます - ホット/ウォーム/コールドアーキテクチャの展開に役立ちます
- データのロールアップ: 古いデータを削除する前に、有用な集計や計算にまとめます

:::note
TTLは、テーブル全体または特定のカラムに適用できます。
:::

## TTLの構文

`TTL`句は、カラム定義の後および/またはテーブル定義の最後に記述できます。`INTERVAL`句を使用して、時間の長さを定義します（これには`Date`または`DateTime`データ型が必要です）。たとえば、次のテーブルには`TTL`句を持つ二つのカラムがあります：

```sql
CREATE TABLE example1 (
   timestamp DateTime,
   x UInt32 TTL timestamp + INTERVAL 1 MONTH,
   y String TTL timestamp + INTERVAL 1 DAY,
   z String
)
ENGINE = MergeTree
ORDER BY tuple()
```

- xカラムは、timestampカラムから1か月の有効期限があります
- yカラムは、timestampカラムから1日の有効期限があります
- インターバルが経過すると、カラムは期限切れになります。ClickHouseはデフォルト値に置き換え、データ部分の全カラム値が期限切れになると、そのカラムをファイルシステムから削除します。

:::note
TTLルールは変更または削除できます。詳細は[テーブルTTLの操作](/docs/ja/sql-reference/statements/alter/ttl.md)ページを参照してください。
:::

## TTLイベントのトリガー

期限切れの行を削除または集約する操作は即時には行われず、テーブルマージ時にのみ実行されます。アクティブにマージされていないテーブルがある場合、TTLイベントをトリガーする2つの設定があります：

- `merge_with_ttl_timeout`: 削除TTLで再度マージするまでの最小遅延時間（秒）。デフォルトは14400秒（4時間）です。
- `merge_with_recompression_ttl_timeout`: 再圧縮TTL（削除前にデータをロールアップするルール）で再度マージするまでの最小遅延時間（秒）。デフォルト値は14400秒（4時間）です。

したがって、デフォルトでは、TTLルールは少なくとも4時間ごとにテーブルに適用されます。より頻繁にTTLルールを適用する必要がある場合は、上記の設定を変更してください。

:::note
推奨する頻繁な使用法ではありませんが、`OPTIMIZE`を使用してマージを強制することもできます：

```sql
OPTIMIZE TABLE example1 FINAL
```

`OPTIMIZE`はテーブルの部分の予定外のマージを初期化し、`FINAL`はテーブルがすでに単一部分になっている場合に再最適化を強制します。
:::

## 行の削除

一定時間経過後にテーブル全体の行を削除するには、テーブルレベルでTTLルールを定義します：

```sql
CREATE TABLE customers (
timestamp DateTime,
name String,
balance Int32,
address String
)
ENGINE = MergeTree
ORDER BY timestamp
TTL timestamp + INTERVAL 12 HOUR
```

さらに、レコードの値に基づくTTLルールを定義することも可能です。
これは、条件を指定することで簡単に実装できます。
複数の条件を許可しています：

```sql
CREATE TABLE events
(
    `event` String,
    `time` DateTime,
    `value` UInt64
)
ENGINE = MergeTree
ORDER BY (event, time)
TTL time + INTERVAL 1 MONTH DELETE WHERE event != 'error',
    time + INTERVAL 6 MONTH DELETE WHERE event = 'error'
```

## カラムの削除

行全体を削除する代わりに、balanceとaddressカラムだけを期限切れにしたいとします。`customers`テーブルを変更し、両方のカラムに2時間のTTLを追加します：

```sql
ALTER TABLE customers
MODIFY COLUMN balance Int32 TTL timestamp + INTERVAL 2 HOUR,
MODIFY COLUMN address String TTL timestamp + INTERVAL 2 HOUR
```

## ロールアップの実装

特定の期間経過後に行を削除したいが、報告目的のために一部のデータを保持したい場合を考えてみましょう。すべての詳細が欲しいわけではなく、履歴データの集約された結果を少し欲しいだけです。これは、`TTL`式に`GROUP BY`句を追加し、テーブルのカラムを使用して集約結果を保存することで実装できます。

以下の`hits`テーブルでは、古い行を削除しますが、削除する前に`hits`カラムの合計と最大を保持したいとします。それらの値を保存するフィールドが必要であり、それに加えて合計と最大をロールアップする`TTL`句に`GROUP BY`句を追加する必要があります：

```sql
CREATE TABLE hits (
   timestamp DateTime,
   id String,
   hits Int32,
   max_hits Int32 DEFAULT hits,
   sum_hits Int64 DEFAULT hits
)
ENGINE = MergeTree
PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
TTL timestamp + INTERVAL 1 DAY
    GROUP BY id, toStartOfDay(timestamp)
    SET
        max_hits = max(max_hits),
        sum_hits = sum(sum_hits);
```

`hits`テーブルに関するいくつかの注意点：

- `TTL`句の`GROUP BY`カラムは`PRIMARY KEY`のプレフィックスである必要があり、結果を日付の開始時点でグループ化したいです。そのため、`toStartOfDay(timestamp)`が主キーに追加されました
- 集約結果を保存するためのフィールド`max_hits`と`sum_hits`を追加しました
- `max_hits`と`sum_hits`のデフォルト値を`hits`に設定することは、`SET`句に基づいて、ロジックが機能するために必要です

## ホット/ウォーム/コールドアーキテクチャの実装

:::note
ClickHouse Cloudを使用している場合、このレッスンの手順は適用されません。ClickHouse Cloudでは、古いデータを移動するといったことを気にする必要はありません。
:::

大量のデータを扱う際に一般的な手法として、データが古くなるにつれてデータを移動する方法があります。以下は`TTL`コマンドの`TO DISK`と`TO VOLUME`句を使用して、ClickHouseでホット/ウォーム/コールドアーキテクチャを実装する手順です。（ちなみに、これをホットとコールドのものとする必要はなく、どのようなユースケースでもデータを移動するためにTTLを使用できます。）

1. `TO DISK`と`TO VOLUME`オプションは、ClickHouseの構成ファイルに定義されているディスクまたはボリュームの名前を指します。ディスクを定義する新しいファイル`my_system.xml`（または任意のファイル名）を作成し、そのディスクを使用するボリュームを定義します。このXMLファイルを`/etc/clickhouse-server/config.d/`に配置して、システムに構成を適用します：

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <default>
            </default>
           <hot_disk>
              <path>./hot/</path>
           </hot_disk>
           <warm_disk>
              <path>./warm/</path>
           </warm_disk>
           <cold_disk>
              <path>./cold/</path>
           </cold_disk>
        </disks>
        <policies>
            <default>
                <volumes>
                    <default>
                        <disk>default</disk>
                    </default>
                    <hot_volume>
                        <disk>hot_disk</disk>
                    </hot_volume>
                    <warm_volume>
                        <disk>warm_disk</disk>
                    </warm_volume>
                    <cold_volume>
                        <disk>cold_disk</disk>
                    </cold_volume>
                </volumes>
            </default>
        </policies>
    </storage_configuration>
</clickhouse>
```

2. 上記の設定は、ClickHouseが読み書きできるフォルダを指す三つのディスクを指しています。ボリュームは一つ以上のディスクを含むことができ、私たちは三つのディスクのためにそれぞれのボリュームを定義しました。ディスクを確認してみましょう：

```sql
SELECT name, path, free_space, total_space
FROM system.disks
```

```response
┌─name────────┬─path───────────┬───free_space─┬──total_space─┐
│ cold_disk   │ ./data/cold/   │ 179143311360 │ 494384795648 │
│ default     │ ./             │ 179143311360 │ 494384795648 │
│ hot_disk    │ ./data/hot/    │ 179143311360 │ 494384795648 │
│ warm_disk   │ ./data/warm/   │ 179143311360 │ 494384795648 │
└─────────────┴────────────────┴──────────────┴──────────────┘
```

3. そして…ボリュームを確認しましょう：

```sql
SELECT
    volume_name,
    disks
FROM system.storage_policies
```

```response
┌─volume_name─┬─disks─────────┐
│ default     │ ['default']   │
│ hot_volume  │ ['hot_disk']  │
│ warm_volume │ ['warm_disk'] │
│ cold_volume │ ['cold_disk'] │
└─────────────┴───────────────┘
```

4. では、データをホット、ウォーム、そしてコールドボリューム間で移動する`TTL`ルールを追加します：

```sql
ALTER TABLE my_table
   MODIFY TTL
      trade_date TO VOLUME 'hot_volume',
      trade_date + INTERVAL 2 YEAR TO VOLUME 'warm_volume',
      trade_date + INTERVAL 4 YEAR TO VOLUME 'cold_volume';
```

5. 新しい`TTL`ルールは材料化されるはずですが、確認のために強制することもできます：

```sql
ALTER TABLE my_table
    MATERIALIZE TTL
```

6. `system.parts`テーブルを使って、データが期待通りのディスクに移動したかを確認してください：

```sql
Using the system.parts table, view which disks the parts are on for the crypto_prices table:

SELECT
    name,
    disk_name
FROM system.parts
WHERE (table = 'my_table') AND (active = 1)
```

レスポンスは以下のようになります：

```response
┌─name────────┬─disk_name─┐
│ all_1_3_1_5 │ warm_disk │
│ all_2_2_0   │ hot_disk  │
└─────────────┴───────────┘
```

## 関連コンテンツ

- ブログ&ウェビナー: [Using TTL to Manage Data Lifecycles in ClickHouse](https://clickhouse.com/blog/using-ttl-to-manage-data-lifecycles-in-clickhouse)
