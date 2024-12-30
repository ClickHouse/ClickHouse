---
slug: /ja/sql-reference/functions/udf
sidebar_position: 15
sidebar_label: UDF
---

# UDF ユーザー定義関数

## 実行可能ユーザー定義関数
ClickHouseは、データを処理するために任意の外部実行可能プログラムやスクリプトを呼び出すことができます。

実行可能ユーザー定義関数の設定は、1つ以上のxmlファイルに記述できます。設定へのパスは、[user_defined_executable_functions_config](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config) パラメータで指定します。

関数設定には以下の設定が含まれます:

- `name` - 関数名。
- `command` - 実行するスクリプト名または`execute_direct`がfalseの場合はコマンド。
- `argument` - 引数の説明には、`type`とオプションの引数`name`が含まれます。引数はそれぞれ個別に記述します。[Native](../../interfaces/formats.md#native) または [JSONEachRow](../../interfaces/formats.md#jsoneachrow) のようなユーザー定義関数形式のシリアル化の一部である場合、名前の指定が必要です。デフォルトの引数名の値は `c` + 引数番号です。
- `format` - コマンドに渡す引数の[形式](../../interfaces/formats.md)です。
- `return_type` - 返される値の型。
- `return_name` - 返される値の名前。[Native](../../interfaces/formats.md#native)や[JSONEachRow](../../interfaces/formats.md#jsoneachrow)といったユーザー定義関数形式のシリアル化の一部である場合、返される名前の指定が必要です。オプション。デフォルト値は `result` です。
- `type` - 実行可能なタイプ。`type`が `executable` に設定されている場合、単一のコマンドが開始されます。`executable_pool` に設定されている場合、コマンドのプールが作成されます。
- `max_command_execution_time` - データブロックを処理するための最大実行時間（秒）。この設定は `executable_pool` コマンドに対してのみ有効です。オプション。デフォルト値は `10` です。
- `command_termination_timeout` - パイプが閉じた後、コマンドが終了するまでの時間（秒）。その後、コマンドを実行するプロセスに `SIGTERM` が送信されます。オプション。デフォルト値は `10` です。
- `command_read_timeout` - コマンドの標準出力からデータを読み込むタイムアウト（ミリ秒）。デフォルト値は 10000。オプション。
- `command_write_timeout` - コマンドの標準入力にデータを書き込むタイムアウト（ミリ秒）。デフォルト値は 10000。オプション。
- `pool_size` - コマンドプールのサイズ。オプション。デフォルト値は `16`。
- `send_chunk_header` - 処理するデータチャンクを送信する前に行数を送信するかどうかを制御します。オプション。デフォルト値は `false`。
- `execute_direct` - `execute_direct` = `1` の場合、`command` は [user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path) で指定されたユーザースクリプトフォルダー内で検索されます。追加のスクリプト引数は空白区切りで指定できます。例: `script_name arg1 arg2`。`execute_direct` = `0` の場合、`command` は `bin/sh -c` の引数として渡されます。デフォルト値は `1`。オプション。
- `lifetime` - 関数のリロード間隔（秒）。`0` に設定されている場合、関数はリロードされません。デフォルト値は `0`。オプション。

コマンドは `STDIN` から引数を読み取り、`STDOUT` に結果を出力する必要があります。コマンドは引数を反復処理する必要があります。つまり、引数のチャンクを処理した後、次のチャンクを待たなければなりません。

**例**

XML設定を使用して `test_function` を作成します。
ファイル `test_function.xml`（デフォルトパス設定の場合 `/etc/clickhouse-server/test_function.xml`）。
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_python</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
            <name>value</name>
        </argument>
        <format>TabSeparated</format>
        <command>test_function.py</command>
    </function>
</functions>
```

`user_scripts` フォルダー内のスクリプトファイル `test_function.py`（デフォルトパス設定の場合 `/var/lib/clickhouse/user_scripts/test_function.py`）。

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

クエリ:

``` sql
SELECT test_function_python(toUInt64(2));
```

結果:

``` text
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

手動で `execute_direct` を `0` に指定して `test_function_sum` を作成します。
ファイル `test_function.xml`（デフォルトパス設定の場合 `/etc/clickhouse-server/test_function.xml`）。
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_sum</name>
        <return_type>UInt64</return_type>
        <argument>
            <type>UInt64</type>
            <name>lhs</name>
        </argument>
        <argument>
            <type>UInt64</type>
            <name>rhs</name>
        </argument>
        <format>TabSeparated</format>
        <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
        <execute_direct>0</execute_direct>
    </function>
</functions>
```

クエリ:

``` sql
SELECT test_function_sum(2, 2);
```

結果:

``` text
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

XML設定を使用し、名前付き引数と形式 [JSONEachRow](../../interfaces/formats.md#jsoneachrow) で `test_function_sum_json` を作成します。
ファイル `test_function.xml`（デフォルトパス設定の場合 `/etc/clickhouse-server/test_function.xml`）。
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_sum_json</name>
        <return_type>UInt64</return_type>
        <return_name>result_name</return_name>
        <argument>
            <type>UInt64</type>
            <name>argument_1</name>
        </argument>
        <argument>
            <type>UInt64</type>
            <name>argument_2</name>
        </argument>
        <format>JSONEachRow</format>
        <command>test_function_sum_json.py</command>
    </function>
</functions>
```

`user_scripts` フォルダー内のスクリプトファイル `test_function_sum_json.py`（デフォルトパス設定の場合 `/var/lib/clickhouse/user_scripts/test_function_sum_json.py`）。

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

クエリ:

``` sql
SELECT test_function_sum_json(2, 2);
```

結果:

``` text
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

実行可能なユーザー定義関数は、コマンド設定で構成された定数パラメータを受け取ることができます（`executable` タイプのユーザー定義関数のみで使用可能）。また、`execute_direct` オプションが必要です（シェル引数展開の脆弱性を防ぐため）。
ファイル `test_function_parameter_python.xml`（デフォルトパス設定の場合 `/etc/clickhouse-server/test_function_parameter_python.xml`）。
```xml
<functions>
    <function>
        <type>executable</type>
        <execute_direct>true</execute_direct>
        <name>test_function_parameter_python</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
        </argument>
        <format>TabSeparated</format>
        <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
    </function>
</functions>
```

`user_scripts` フォルダー内のスクリプトファイル `test_function_parameter_python.py`（デフォルトパス設定の場合 `/var/lib/clickhouse/user_scripts/test_function_parameter_python.py`）。

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

クエリ:

``` sql
SELECT test_function_parameter_python(1)(2);
```

結果:

``` text
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

## エラー処理

いくつかの関数はデータが無効な場合、例外を投げる可能性があります。この場合、クエリはキャンセルされ、エラーテキストがクライアントに返されます。分散処理の場合、サーバーの1つで例外が発生すると、他のサーバーもクエリを中止しようとします。

## 引数式の評価

ほとんどのプログラミング言語では、特定の演算子に対して引数の1つが評価されないことがあります。これは通常、演算子 `&&`、`||`、および `?:` です。しかし、ClickHouseでは関数（演算子）の引数は常に評価されます。これは、各行を個別に計算する代わりに、カラムの全体を一度に評価するためです。

## 分散クエリ処理用の関数の実行

分散クエリ処理の場合、可能な限り多くのクエリ処理段階がリモートサーバー上で実行され、残りの段階（中間結果のマージおよびそれ以降すべて）はリクエストサーバー上で実行されます。

これは、関数が異なるサーバーで実行されることを意味します。
例として、クエリ `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

- `distributed_table` に少なくとも2つのシャードがある場合、関数「g」と「h」はリモートサーバー上で実行され、「f」はリクエストサーバー上で実行されます。
- `distributed_table` に1つのシャードしかない場合、すべての「f」、「g」、および「h」関数はこのシャードのサーバー上で実行されます。

関数の結果は通常、実行されるサーバーに依存しません。しかし、時にはこれは重要です。
たとえばDictionaryを操作する関数は、それが実行されているサーバー上に存在するDictionaryを使用します。別の例は、`hostName` 関数で、これは `SELECT` クエリでサーバーごとに `GROUP BY` を行うために実行されているサーバーの名前を返します。

クエリ内の関数がリクエストサーバー上で実行されているが、リモートサーバー上で実行する必要がある場合、`any` 集約関数でラップしたり、`GROUP BY` のキーに追加したりすることができます。

## SQL ユーザー定義関数

ラムダ式からカスタム関数を作成するには、[CREATE FUNCTION](../statements/create/function.md) ステートメントを使用します。これらの関数を削除するには、[DROP FUNCTION](../statements/drop.md#drop-function) ステートメントを使用します。

## 関連コンテンツ

### [ClickHouse Cloudのユーザー定義関数に関する記事](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
