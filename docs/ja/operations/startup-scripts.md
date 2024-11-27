---
slug: /ja/operations/startup-scripts
sidebar_label: スタートアップスクリプト
---

# スタートアップスクリプト

ClickHouse は、起動時にサーバー設定から任意の SQL クエリを実行できます。これは、マイグレーションや自動スキーマ作成に役立ちます。

```xml
<clickhouse>
    <startup_scripts>
        <scripts>
            <query>CREATE ROLE OR REPLACE test_role</query>
        </scripts>
        <scripts>
            <query>CREATE TABLE TestTable (id UInt64) ENGINE=TinyLog</query>
            <condition>SELECT 1;</condition>
        </scripts>
    </startup_scripts>
</clickhouse>
```

ClickHouse は `startup_scripts` からのすべてのクエリを指定された順序で順番に実行します。どのクエリが失敗しても、その後のクエリの実行は中断されません。

設定ファイル内で条件付きクエリを指定することができます。この場合、該当するクエリは条件クエリが `1` または `true` を返す場合にのみ実行されます。

:::note
条件クエリが `1` または `true` 以外の値を返した場合、結果は `false` と解釈され、該当するクエリは実行されません。
:::

