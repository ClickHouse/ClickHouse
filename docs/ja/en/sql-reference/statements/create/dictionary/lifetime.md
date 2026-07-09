---
description: 'Dictionary の自動更新のための LIFETIME 設定'
sidebar_label: 'LIFETIME'
sidebar_position: 5
slug: /sql-reference/statements/create/dictionary/lifetime
title: 'LIFETIME を使用した Dictionary データの更新'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

ClickHouse は、`LIFETIME` タグ (秒単位で定義) に基づいて定期的に Dictionary を更新します。
`LIFETIME` は、完全にダウンロードする Dictionary の更新間隔であり、cache Dictionary の無効化間隔でもあります。

更新中も、Dictionary の旧バージョンには引き続きクエリを実行できます。
初回使用時の読み込みを除き、Dictionary の更新によってクエリがブロックされることはありません。
更新中にエラーが発生した場合、そのエラーはサーバーログに書き込まれ、クエリは引き続き Dictionary の旧バージョンを使用できます。
Dictionary の更新が成功すると、Dictionary の旧バージョンは[アトミックに](/ja/concepts/glossary#atomicity)置き換えられます。

設定例:

<CloudDetails />

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

または

```sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

`<lifetime>0</lifetime>` (`LIFETIME(0)`) を設定すると、Dictionary は更新されなくなります。

更新間隔を設定すると、ClickHouse はその範囲内で一様ランダムに時刻を選択します。これは、多数のサーバーで更新を行う際に Dictionary ソースへの負荷を分散するために必要です。

設定例:

```xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

または

```sql
LIFETIME(MIN 300 MAX 360)
```

`<min>0</min>` と `<max>0</max>` の場合、ClickHouse はタイムアウトによって Dictionary を再読み込みしません。
この場合、Dictionary の設定ファイルが変更されたとき、または `SYSTEM RELOAD DICTIONARY` コマンドが実行されたときには、ClickHouse はそれより前に Dictionary を再読み込みできます。

Dictionary を更新する際、ClickHouse server は [source](./sources/) の種類に応じて異なるロジックを適用します。

* テキストファイルの場合は、更新時刻を確認します。時刻が以前に記録された時刻と異なる場合、Dictionary が更新されます。
* その他のソースの Dictionary は、デフォルトでは毎回更新されます。

その他のソース (ODBC、PostgreSQL、ClickHouse など) については、毎回ではなく実際に変更があった場合にのみ Dictionary を更新するクエリを設定できます。これを行うには、次の手順に従ってください。

* Dictionary テーブルには、ソースデータが更新されたときに必ず変化するフィールドが必要です。
* ソースの設定では、その変化するフィールドを取得するクエリを指定する必要があります。ClickHouse server はクエリ結果を 1 行として解釈し、この行が前回の状態から変化していれば、Dictionary が更新されます。クエリは [source](./sources/) の設定にある `<invalidate_query>` フィールドで指定します。

設定例:

```xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

または

```sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

`Cache`、`ComplexKeyCache`、`SSDCache`、`SSDComplexKeyCache` の Dictionary では、同期更新と非同期更新の両方がサポートされています。

また、`Flat`、`Hashed`、`HashedArray`、`ComplexKeyHashed` の Dictionary では、前回の更新後に変更されたデータのみをリクエストすることもできます。Dictionary ソースの構成の一部として `update_field` が指定されている場合、前回の更新時刻の秒単位の値がデータリクエストに追加されます。ソースの種類 (Executable、HTTP、MySQL、PostgreSQL、ClickHouse、または ODBC) に応じて、外部ソースにデータをリクエストする前に `update_field` に対して異なる処理が適用されます。

* ソースが HTTP の場合、`update_field` はクエリパラメータとして追加され、その値には前回の更新時刻が設定されます。
* ソースが Executable の場合、`update_field` は実行可能スクリプトの引数として追加され、その値には前回の更新時刻が設定されます。
* ソースが ClickHouse、MySQL、PostgreSQL、ODBC の場合は、`WHERE` 句が追加され、その中で `update_field` が前回の更新時刻以上であるかどうかが比較されます。
  * デフォルトでは、この `WHERE` 条件は SQL クエリの最上位レベルで評価されます。必要に応じて、クエリ内の別の `WHERE` 句で `{condition}` キーワードを使用してこの条件を評価することもできます。例:
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

`update_field` オプションが設定されている場合は、追加の `update_lag` オプションも設定できます。`update_lag` オプションの値は、更新データをリクエストする前に前回の更新時刻から差し引かれます。

設定例:

```xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

または

```sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```