---
slug: /ja/sql-reference/aggregate-functions/reference/min
sidebar_position: 168
title: min
---

グループ内の値の中から最小値を計算する集約関数です。

例:

```
SELECT min(salary) FROM employees;
```

```
SELECT department, min(salary) FROM employees GROUP BY department;
```

2つの値から最小値を選択する非集約関数が必要な場合は、`least`を参照してください:

```
SELECT least(a, b) FROM table;
```
