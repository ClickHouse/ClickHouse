<a name="queries-insert"></a>

## INSERT

正在添加数据.

基本查询格式:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

此查询能够指定字段的列表来插入 `[(c1, c2, c3)]`. 在这种情况下, 剩下的字段用如下来填充:

- 从表定义中指定的 `DEFAULT` 表达式中计算出值.
- 空字符串, 如果 `DEFAULT` 表达式没有定义.

如果 [strict_insert_defaults=1](../operations/settings/settings.md#settings-strict_insert_defaults), 没有 `DEFAULT` 定义的字段必须在查询中列出.

在任何ClickHouse所支持的格式上 [format](../interfaces/formats.md#formats) 数据被传入到 INSERT中. 此格式必须被显式地指定在查询中:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

例如, 如下的查询格式与基本的 INSERT ... VALUES 版本相同:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse 在数据之前, 删除所有空格和换行(如果有). 当形成一个查询时, 我们推荐在查询操作符之后将数据放入新行(如果数据以空格开始, 这是重要的).

示例:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

你能够单独从查询中插入数据，通过命令行或 HTTP 接口. For more information, see the section "[Interfaces](../interfaces/index.md#interfaces)".

### Inserting The Results of `SELECT`

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Columns are mapped according to their position in the SELECT clause. However, their names in the SELECT expression and the table for INSERT may differ. If necessary, type casting is performed.

None of the data formats except Values allow setting values to expressions such as `now()`, `1 + 2`,  and so on. The Values format allows limited use of expressions, but this is not recommended, because in this case inefficient code is used for their execution.

Other queries for modifying data parts are not supported: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
However, you can delete old data using `ALTER TABLE ... DROP PARTITION`.

### Performance Considerations

`INSERT` sorts the input data by primary key and splits them into partitions by month. If you insert data for mixed months, it can significantly reduce the performance of the `INSERT` query. To avoid this:

- Add data in fairly large batches, such as 100,000 rows at a time.
- Group data by month before uploading it to ClickHouse.

Performance will not decrease if:

- Data is added in real time.
- You upload data that is usually sorted by time.

