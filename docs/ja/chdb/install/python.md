---
title: Python向けのchDBのインストール
sidebar_label: Python
slug: /ja/chdb/install/python
description: Python向けのchDBのインストール方法
keywords: [chdb, embedded, clickhouse-lite, python, install]
---

# Python向けのchDBのインストール

## 必要条件

macOSとLinux（x86_64およびARM64）上のPython 3.8+

## インストール

```bash
pip install chdb
```

## 使用法

CLI例:

```python
python3 -m chdb [SQL] [OutputFormat]
```

```python
python3 -m chdb "SELECT 1, 'abc'" Pretty
```

Pythonファイル例:

```python
import chdb

res = chdb.query("SELECT 1, 'abc'", "CSV")
print(res, end="")
```

クエリは、`Dataframe`や`Debug`を含む[サポート形式](/docs/ja/interfaces/formats)のいずれかを使用してデータを返すことができます。

## GitHubリポジトリ

プロジェクトのGitHubリポジトリは[chdb-io/chdb](https://github.com/chdb-io/chdb)にあります。

## データ入力

以下のメソッドを使用して、オンディスクおよびインメモリのデータフォーマットにアクセス可能です。

### ファイル上でのクエリ (Parquet, CSV, JSON, Arrow, ORC など60+)

SQLを実行し、希望の形式でデータを返せます。

```python
import chdb
res = chdb.query('select version()', 'Pretty'); print(res)
```

**ParquetまたはCSVでの操作**

```python
# データ型の詳細は tests/format_output.py を参照
res = chdb.query('select * from file("data.parquet", Parquet)', 'JSON'); print(res)
res = chdb.query('select * from file("data.csv", CSV)', 'CSV');  print(res)
print(f"SQL read {res.rows_read()} rows, {res.bytes_read()} bytes, elapsed {res.elapsed()} seconds")
```

**Pandas dataframe出力**
```python
# 詳細はhttps://clickhouse.com/docs/ja/interfaces/formatsを参照
chdb.query('select * from file("data.parquet", Parquet)', 'Dataframe')
```

### テーブル上でのクエリ (Pandas DataFrame, Parquet file/bytes, Arrow bytes)

**Pandas DataFrame上でのクエリ**

```python
import chdb.dataframe as cdf
import pandas as pd
# 2つのDataFrameを結合
df1 = pd.DataFrame({'a': [1, 2, 3], 'b': ["one", "two", "three"]})
df2 = pd.DataFrame({'c': [1, 2, 3], 'd': ["①", "②", "③"]})
ret_tbl = cdf.query(sql="select * from __tbl1__ t1 join __tbl2__ t2 on t1.a = t2.c",
                  tbl1=df1, tbl2=df2)
print(ret_tbl)
# DataFrameテーブルでのクエリ
print(ret_tbl.query('select b, sum(a) from __table__ group by b'))
```

### 状態を持つセッションでのクエリ

セッションはクエリの状態を保持します。すべてのDDLおよびDMLの状態はディレクトリに保持されます。ディレクトリパスは引数として渡すことができます。指定されない場合、一時ディレクトリが作成されます。

パスが指定されない場合、セッションオブジェクトが削除されるときに一時ディレクトリが削除されます。そうでない場合、パスは保持されます。

デフォルトのデータベースは`_local`で、デフォルトのエンジンは`Memory`であるため、すべてのデータはメモリに保存されます。ディスクにデータを保存したい場合は、別のデータベースを作成する必要があります。

```python
from chdb import session as chs

## 一時セッションでDB、テーブル、ビューを作成し、セッションが削除されると自動的にクリーンアップします。
sess = chs.Session()
sess.query("CREATE DATABASE IF NOT EXISTS db_xxx ENGINE = Atomic")
sess.query("CREATE TABLE IF NOT EXISTS db_xxx.log_table_xxx (x String, y Int) ENGINE = Log;")
sess.query("INSERT INTO db_xxx.log_table_xxx VALUES ('a', 1), ('b', 3), ('c', 2), ('d', 5);")
sess.query(
    "CREATE VIEW db_xxx.view_xxx AS SELECT * FROM db_xxx.log_table_xxx LIMIT 4;"
)
print("ビューから選択:\n")
print(sess.query("SELECT * FROM db_xxx.view_xxx", "Pretty"))
```

関連情報: [test_stateful.py](https://github.com/chdb-io/chdb/blob/main/tests/test_stateful.py).

### Python DB-API 2.0でのクエリ

```python
import chdb.dbapi as dbapi
print("chdbドライバーバージョン: {0}".format(dbapi.get_client_info()))

conn1 = dbapi.connect()
cur1 = conn1.cursor()
cur1.execute('select version()')
print("説明: ", cur1.description)
print("データ: ", cur1.fetchone())
cur1.close()
conn1.close()
```

### UDF（ユーザー定義関数）でのクエリ

```python
from chdb.udf import chdb_udf
from chdb import query

@chdb_udf()
def sum_udf(lhs, rhs):
    return int(lhs) + int(rhs)

print(query("select sum_udf(12,22)"))
```

chDBのPython UDF（ユーザー定義関数）デコレーターに関する注意点。
1. 関数はステートレスであるべきです。UDF（ユーザー定義関数）のみがサポートされており、UDAF（ユーザー定義集約関数）はサポートされていません。
2. デフォルトの戻り値の型はStringです。戻り値の型を変更したい場合は、引数として渡すことができます。戻り値の型は[次のいずれか](/ja/sql-reference/data-types)である必要があります。
3. 関数はString型の引数を受け取るべきです。入力がタブ区切りであるため、すべての引数は文字列です。
4. 関数は入力の各行ごとに呼び出されるでしょう。例：
    ```python
    def sum_udf(lhs, rhs):
        return int(lhs) + int(rhs)

    for line in sys.stdin:
        args = line.strip().split('\t')
        lhs = args[0]
        rhs = args[1]
        print(sum_udf(lhs, rhs))
        sys.stdout.flush()
    ```
5. 関数は純粋なPython関数であるべきです。関数内で使用するすべてのPythonモジュールをインポートする必要があります。
    ```python
    def func_use_json(arg):
        import json
        ...
    ```
6. 使用されるPythonインタープリタは、スクリプトを実行しているものと同じです。`sys.executable`から取得できます。

関連情報: [test_udf.py](https://github.com/chdb-io/chdb/blob/main/tests/test_udf.py).

### Pythonテーブルエンジン

### Pandas DataFrameでのクエリ

```python
import chdb
import pandas as pd
df = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5, 6],
        "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
    }
)

chdb.query("SELECT b, sum(a) FROM Python(df) GROUP BY b ORDER BY b").show()
```

### Arrowテーブルでのクエリ

```python
import chdb
import pyarrow as pa
arrow_table = pa.table(
    {
        "a": [1, 2, 3, 4, 5, 6],
        "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
    }
)

chdb.query(
    "SELECT b, sum(a) FROM Python(arrow_table) GROUP BY b ORDER BY b", "debug"
).show()
```

### chdb.PyReaderクラスのインスタンスでのクエリ

1. chdb.PyReaderクラスを継承し、`read`メソッドを実装する必要があります。
2. `read`メソッドは以下を行う必要があります:
    1. リストのリストを返すこと。第一次元はカラム、第二次元は行です。カラムの順序は、`read`の最初の引数`col_names`と同じであるべきです。
    1. 読み疲れた時に空のリストを返すこと。
    1. 状態を持ち、カーソルは`read`メソッド内で更新されるべきです。
3. オプションで`get_schema`メソッドを実装してテーブルのスキーマを返すことができます。プロトタイプは`def get_schema(self) -> List[Tuple[str, str]]:`です。返り値はタプルのリストで、各タプルはカラム名とカラムの型を含みます。カラム型は[次のいずれか](/ja/sql-reference/data-types)である必要があります。

<br />

```python
import chdb

class myReader(chdb.PyReader):
    def __init__(self, data):
        self.data = data
        self.cursor = 0
        super().__init__(data)

    def read(self, col_names, count):
        print("Python func read", col_names, count, self.cursor)
        if self.cursor >= len(self.data["a"]):
            return []
        block = [self.data[col] for col in col_names]
        self.cursor += len(block[0])
        return block

reader = myReader(
    {
        "a": [1, 2, 3, 4, 5, 6],
        "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
    }
)

chdb.query(
    "SELECT b, sum(a) FROM Python(reader) GROUP BY b ORDER BY b"
).show()
```

関連情報: [test_query_py.py](https://github.com/chdb-io/chdb/blob/main/tests/test_query_py.py).

## 制限事項

1. サポートされているカラム型: pandas.Series, pyarrow.array, chdb.PyReader
1. サポートされているデータ型: Int, UInt, Float, String, Date, DateTime, Decimal
1. Pythonオブジェクト型はStringに変換される
1. Pandas DataFrameのパフォーマンスは最高で、Arrow TableはPyReaderよりも優れています。

<br />

より多くの例については、[examples](https://github.com/chdb-io/chdb/tree/main/examples)および[tests](https://github.com/chdb-io/chdb/tree/main/tests)を参照してください。
