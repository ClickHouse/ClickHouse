---
slug: /en/operations/startup-scripts
sidebar_label: Startup Scripts
---

# Startup Scripts

ClickHouse can run arbitrary SQL queries from the server configuration during startup. This can be useful for migrations or automatic schema creation.

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

ClickHouse executes all queries from the `startup_scripts` sequentially in the specified order. If any of the queries fail, the execution of the following queries won't be interrupted.

You can specify a conditional query in the config. In that case, the corresponding query executes only when the condition query returns the value `1` or `true`.

:::note
If the condition query returns any other value than `1` or `true`, the result will be interpreted as `false`, and the corresponding won't be executed.
:::
