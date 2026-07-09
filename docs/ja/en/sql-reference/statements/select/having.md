---
description: 'HAVING 句のドキュメント'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'HAVING 句'
doc_type: 'reference'
---

[GROUP BY](/ja/sql-reference/statements/select/group-by) で生成された集約結果を絞り込むことができます。[WHERE](../../../sql-reference/statements/select/where.md) 句に似ていますが、`WHERE` は集約の前に実行されるのに対し、`HAVING` は集約の後に実行される点が異なります。

`SELECT` 句の集約結果は、そのエイリアスを使って `HAVING` 句から参照できます。また、`HAVING` 句では、クエリ結果には返されない追加の集約結果に対して絞り込みを行うこともできます。

<div id="example">
  ## 例
</div>

以下のような `sales` テーブルがある場合:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

以下のようにクエリできます。

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

これにより、担当地域での総売上が10,000を超える営業担当者の一覧が表示されます。

<div id="limitations">
  ## 制限事項
</div>

集計を行わない場合、`HAVING` は使用できません。代わりに `WHERE` を使用してください。