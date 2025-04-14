---
slug: /ja/sql-reference/statements/select/having
sidebar_label: HAVING
---

# HAVING句

[GROUP BY](../../../sql-reference/statements/select/group-by.md)によって生成された集計結果をフィルタリングすることができます。これは[WHERE](../../../sql-reference/statements/select/where.md)句に似ていますが、`WHERE`が集計の前に実行されるのに対し、`HAVING`は集計の後に実行される点が異なります。

`HAVING`句では、`SELECT`句で使われたエイリアスを参照することで集計結果をフィルタリングすることができます。また、クエリ結果に返されない追加の集計結果を基にフィルタリングすることも可能です。

## 制限事項

集計が行われない場合、`HAVING`を使用することはできません。この場合は`WHERE`を使用してください。
