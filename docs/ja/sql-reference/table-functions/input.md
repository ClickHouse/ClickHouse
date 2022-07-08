---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u5165\u529B"
---

# 入力 {#input}

`input(structure)` -に送られるデータを効果的に変え、挿入することを可能にする表機能
別の構造を持つテーブルに指定された構造を持つサーバー。

`structure` -以下の形式でサーバーに送信されるデータの構造 `'column1_name column1_type, column2_name column2_type, ...'`.
例えば, `'id UInt32, name String'`.

この関数は、次の場合にのみ使用できます `INSERT SELECT` それ以外の場合は通常の表関数のように動作します
（例えば、サブクエリなどで使用できます。).

データは通常のような方法で送信することができます `INSERT` クエリと任意の利用可能に渡されます [形式](../../interfaces/formats.md#formats)
クエリの最後に指定する必要があります(通常とは異なります `INSERT SELECT`).

この機能の主な特徴は、サーバがクライアントからデータを受信すると同時に変換することです
の式のリストに従って `SELECT` 節とターゲットテーブルへの挿入。 一時テーブル
転送されたすべてのデータは作成されません。

**例**

-   は、 `test` 表の構造は次のとおりです `(a String, b String)`
    そしてデータ `data.csv` 異なる構造を持っています `(col1 String, col2 Date, col3 Int32)`. 挿入のクエリ
    からのデータ `data.csv` に `test` 同時変換のテーブルは次のようになります:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   もし `data.csv` 同じ構造のデータを含む `test_structure` 表として `test` そしてこれら二つのクエリが等しい:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[元の記事](https://clickhouse.com/docs/en/query_language/table_functions/input/) <!--hide-->
