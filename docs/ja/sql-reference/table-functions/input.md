---
slug: /ja/sql-reference/table-functions/input
sidebar_position: 95
sidebar_label: input
---

# input

`input(structure)` - 指定された構造でサーバーに送信されたデータを別の構造のテーブルに効率的に変換し、挿入するためのテーブル関数です。

`structure` - サーバーに送信されるデータの構造を次の形式で指定します: `'column1_name column1_type, column2_name column2_type, ...'`。
例えば、`'id UInt32, name String'`。

この関数は`INSERT SELECT`クエリでのみ使用でき、一度しか使用できませんが、それ以外は通常のテーブル関数のように動作します
（例えば、サブクエリで使用することができます）。

データは通常の`INSERT`クエリのように任意の方法で送信でき、クエリの最後に指定する必要がある任意の利用可能な[形式](../../interfaces/formats.md#formats)で渡すことができます（通常の`INSERT SELECT`とは異なります）。

この関数の主な機能は、サーバーがクライアントからデータを受信すると、`SELECT`句の式リストに従って同時に変換し、ターゲットテーブルに挿入することです。すべての転送データを格納する一時テーブルは作成されません。

**例**

- `test`テーブルが次の構造`(a String, b String)`を持ち、`data.csv`のデータが異なる構造`(col1 String, col2 Date, col3 Int32)`を持っているとします。この場合、`data.csv`から`test`テーブルにデータを同時に変換して挿入するクエリは次のようになります:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

- `data.csv`にテーブル`test`と同じ構造`test_structure`のデータが含まれている場合、これら二つのクエリは同等です:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```
