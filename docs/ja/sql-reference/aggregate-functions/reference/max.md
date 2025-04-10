---
slug: /ja/sql-reference/aggregate-functions/reference/max
sidebar_position: 162
title: max
---

値のグループから最大値を計算する集計関数。

例:

```
SELECT max(salary) FROM employees;
```

```
SELECT department, max(salary) FROM employees GROUP BY department;
```

集計関数ではない2つの値の最大を選択する必要がある場合は、`greatest`を参照してください:

```
SELECT greatest(a, b) FROM table;
```
