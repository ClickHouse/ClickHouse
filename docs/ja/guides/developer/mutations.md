---
slug: /ja/guides/developer/mutations
sidebar_label: データの更新と削除
sidebar_position: 1
keywords: [更新, 削除, ミューテーション]
---

# ClickHouse データの更新と削除

ClickHouse は高ボリュームの分析ワークロードに適しているものの、特定の状況では既存のデータを変更または削除できます。これらの操作は「ミューテーション」と呼ばれ、`ALTER TABLE` コマンドを使用して実行されます。また、ClickHouse の論理削除の機能を使用して、行を `DELETE` することもできます。

:::tip
頻繁な更新が必要な場合は、ClickHouse の [重複排除](../developer/deduplication.md) を使用することを検討してください。これにより、ミューテーションイベントを生成せずに行を更新および/または削除できます。
:::

## データの更新

テーブルの行を更新するには `ALTER TABLE...UPDATE` コマンドを使用します:

```sql
ALTER TABLE [<database>.]<table> UPDATE <column> = <expression> WHERE <filter_expr>
```

`<expression>` は `<filter_expr>` を満たすカラムの新しい値です。`<expression>` はカラムと同じデータ型であるか、`CAST` 演算子を使用して同じデータ型に変換可能である必要があります。`<filter_expr>` はデータの各行に対して `UInt8`（0 または 0 以外）の値を返す必要があります。複数の `UPDATE <column>` ステートメントをカンマで区切って単一の `ALTER TABLE` コマンドに組み合わせることができます。

**例**:

 1. このようなミューテーションは、Dictionaryルックアップを使用して `visitor_id` を新しいものに更新できます。

     ```sql
    ALTER TABLE website.clicks
    UPDATE visitor_id = getDict('visitors', 'new_visitor_id', visitor_id)
    WHERE visit_date < '2022-01-01'
    ```

2. 一つのコマンドで複数の値を変更することで、複数のコマンドよりも効率的になります。

     ```sql
     ALTER TABLE website.clicks
     UPDATE url = substring(url, position(url, '://') + 3), visitor_id = new_visit_id
     WHERE visit_date < '2022-01-01'
     ```

3. シャード化されたテーブルに対して `ON CLUSTER` でミューテーションを実行できます。

     ```sql
     ALTER TABLE clicks ON CLUSTER main_cluster
     UPDATE click_count = click_count / 2
     WHERE visitor_id ILIKE '%robot%'
     ```

:::note
主キーまたはソートキーの一部であるカラムを更新することはできません。
:::

## データの削除

行を削除するには、`ALTER TABLE` コマンドを使用します:

```sql
ALTER TABLE [<database>.]<table> DELETE WHERE <filter_expr>
```

`<filter_expr>` はデータの各行に対して `UInt8` の値を返す必要があります。

**例**

1. カラムが値の配列に含まれるレコードを削除する:
    ```sql
    ALTER TABLE website.clicks DELETE WHERE visitor_id in (253, 1002, 4277)
    ```

2. このクエリは何を変更するのか？
    ```sql
    ALTER TABLE clicks ON CLUSTER main_cluster DELETE WHERE visit_date < '2022-01-02 15:00:00' AND page_id = '573'
    ```

:::note
テーブル内のすべてのデータを削除するには、`TRUNCATE TABLE [<database].]<table>` コマンドを使用する方が効率的です。このコマンドは `ON CLUSTER` でも実行できます。
:::

詳細については、[`DELETE` ステートメント](/docs/ja/sql-reference/statements/delete.md) ドキュメントページを参照してください。

## 論理削除

行を削除する別のオプションとして、`DELETE FROM` コマンドがあります。これは **論理削除** と呼ばれます。削除された行は即座に削除としてマークされ、後続のすべてのクエリから自動的にフィルタリングされるため、パーツのマージを待ったり `FINAL` キーワードを使用したりする必要はありません。データのクリーンアップはバックグラウンドで非同期に行われます。

``` sql
DELETE FROM [db.]table [ON CLUSTER cluster] [WHERE expr]
```

たとえば、次のクエリは、`Title` カラムに `hello` というテキストが含まれる `hits` テーブルのすべての行を削除します。

```sql
DELETE FROM hits WHERE Title LIKE '%hello%';
```

論理削除に関する注意事項:
- この機能は `MergeTree` テーブルエンジンファミリーでのみ利用可能です。
- 論理削除はデフォルトで非同期です。`mutations_sync` を 1 に設定すると 1 つのレプリカがステートメントを処理するのを待ち、2 に設定するとすべてのレプリカを待ちます。
