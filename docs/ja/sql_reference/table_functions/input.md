---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 46
toc_title: "\u5165\u529B"
---

# 入力 {#input}

`input(structure)` -テーブル関数に送信されたデータを効果的に変換して挿入できるようにする
別の構造を持つテーブルに与えられた構造を持つサーバー。

`structure` -次の形式でサーバーに送信されるデータの構造 `'column1_name column1_type, column2_name column2_type, ...'`.
例えば, `'id UInt32, name String'`.

この関数は、以下でのみ使用できます `INSERT SELECT` query and only once but otherwiseは通常のテーブル関数のように動作します
（たとえば、サブクエリなどで使用することができます。).

データを送ることができ、そのような通常の `INSERT` 利用できる照会および渡される [書式](../../interfaces/formats.md#formats)
これは、クエリの最後に指定する必要があります（通常とは異なり `INSERT SELECT`).

この機能はサーバからデータを受け取clientで同時に変換して
式のリストによると、 `SELECT` ターゲットテーブルに句と挿入します。 一時テーブル
すべての転送されたデータは作成されません。

**例**

-   させる `test` テーブルには以下の構造 `(a String, b String)`
    とデータで `data.csv` 異なる構造を持っています `(col1 String, col2 Date, col3 Int32)`. 挿入のクエリ
    からのデータ `data.csv` に `test` 同時変換のテーブルは次のようになります:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   もし `data.csv` 同じ構造のデータを含む `test_structure` としてのテーブル `test` そしてこれら二つのクエリが等しい:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
