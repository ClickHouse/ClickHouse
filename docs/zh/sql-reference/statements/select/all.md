# ALL 子句 {#select-all}

`SELECT ALL` 和 `SELECT` 不带 `DISTINCT` 是一样的。

- 如果指定了 `ALL` ，则忽略它。
- 如果同时指定了 `ALL` 和 `DISTINCT` ，则会抛出异常。

`ALL` 也可以在聚合函数中指定，具有相同的效果（空操作）。例如：

```sql
SELECT sum(ALL number) FROM numbers(10);
```
等于

```sql
SELECT sum(number) FROM numbers(10);
```
