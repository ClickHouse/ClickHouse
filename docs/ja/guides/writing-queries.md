---
sidebar_position: 3
sidebar_label: SELECT クエリ
---

# ClickHouse の SELECT クエリ

ClickHouse は SQL データベースであり、データのクエリは既にお馴染みの `SELECT` クエリを書くことで行えます。例えば以下のように使用します：

```sql
SELECT *
FROM helloworld.my_first_table
ORDER BY timestamp
```

:::note
構文や利用可能な句、オプションの詳細については、[SQL リファレンス](../sql-reference/statements/select/index.md) を参照してください。
:::

応答はきれいなテーブル形式で返ってくることに注目してください：

```response
┌─user_id─┬─message────────────────────────────────────────────┬───────────timestamp─┬──metric─┐
│     102 │ Insert a lot of rows per batch                     │ 2022-03-21 00:00:00 │ 1.41421 │
│     102 │ Sort your data based on your commonly-used queries │ 2022-03-22 00:00:00 │   2.718 │
│     101 │ Hello, ClickHouse!                                 │ 2022-03-22 14:04:09 │      -1 │
│     101 │ Granules are the smallest chunks of data read      │ 2022-03-22 14:04:14 │ 3.14159 │
└─────────┴────────────────────────────────────────────────────┴─────────────────────┴─────────┘

4 rows in set. Elapsed: 0.008 sec.
```

`FORMAT` 句を追加して、ClickHouseでサポートされている多くの出力フォーマットの一つを指定することができます：

```sql
SELECT *
FROM helloworld.my_first_table
ORDER BY timestamp
FORMAT TabSeparated
```

上記のクエリでは、出力はタブ区切りで返されます：

```response
Query id: 3604df1c-acfd-4117-9c56-f86c69721121

102 Insert a lot of rows per batch	2022-03-21 00:00:00	1.41421
102 Sort your data based on your commonly-used queries	2022-03-22 00:00:00	2.718
101 Hello, ClickHouse!	2022-03-22 14:04:09	-1
101 Granules are the smallest chunks of data read	2022-03-22 14:04:14	3.14159

4 rows in set. Elapsed: 0.005 sec.
```

:::note
ClickHouse は 70 以上の入力および出力フォーマットをサポートしており、数千の関数とすべてのデータフォーマットとの組み合わせで、ClickHouse を使用して印象的で高速な ETL のようなデータ変換を実行できます。実際、データを変換するために ClickHouse サーバーを起動する必要すらありません。`clickhouse-local` ツールを使用できます。詳細は、[`clickhouse-local` のドキュメントページ](../operations/utilities/clickhouse-local.md)を参照してください。
:::
