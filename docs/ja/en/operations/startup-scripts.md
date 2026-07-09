---
description: 'ClickHouse で SQL startup scripts を設定して使用し、起動時にスキーマの自動作成や移行を行うためのガイド'
sidebar_label: 'Startup scripts'
slug: /operations/startup-scripts
title: 'Startup scripts'
doc_type: 'guide'
---

ClickHouse では、起動時にサーバー設定から任意の SQL クエリを実行できます。これは、移行やスキーマの自動作成に役立ちます。

```xml
<clickhouse>
    <startup_scripts>
        <throw_on_error>false</throw_on_error>
        <scripts>
            <query>CREATE ROLE OR REPLACE test_role</query>
        </scripts>
        <scripts>
            <query>CREATE TABLE TestTable (id UInt64) ENGINE=TinyLog</query>
            <condition>SELECT 1;</condition>
        </scripts>
        <scripts>
            <query>CREATE DICTIONARY test_dict (...) SOURCE(CLICKHOUSE(...))</query>
            <user>default</user>
        </scripts>
    </startup_scripts>
</clickhouse>
```

ClickHouse は、`startup_scripts` 内のすべてのクエリを、指定された順序で順次実行します。いずれかのクエリが失敗しても、後続のクエリの実行は中断されません。ただし、`throw_on_error` が true に設定されている場合、
スクリプトの実行中にエラーが発生すると、サーバーは起動しません。

設定では条件付きクエリを指定できます。その場合、対応するクエリは、条件クエリが値 `1` または `true` を返したときにのみ実行されます。

:::note
条件クエリが `1` または `true` 以外の値を返した場合、その結果は `false` として解釈され、対応するクエリは実行されません。
:::