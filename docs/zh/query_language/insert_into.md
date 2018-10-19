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

你能够单独从查询中插入数据，通过命令行或 HTTP 接口. 进一步信息, 参见 "[Interfaces](../interfaces/index.md#interfaces)".

### Inserting The Results of `SELECT`

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

在 SELECT语句中, 根据字段的位置来映射. 然而, 在SELECT表达式中的名称和表名可能不同. 如果必要, 可以进行类型转换.

除了值以外没有其他数据类型允许设置值到表达式中, 例如 `now()`, `1 + 2`, 等. 值格式允许使用有限制的表达式, 但是它并不推荐, 因为在这种情况下, 执行了低效的代码.

不支持修改数据分区的查询如下: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
然而, 你能够使用 `ALTER TABLE ... DROP PARTITION`来删除旧数据.

### Performance Considerations

`INSERT` 通过主键来排序数据, 并通过月份来拆分数据到每个分区中. 如果插入的数据有混合的月份, 会显著降低`INSERT` 插入的性能. 应该避免此类操作:

- 大批量地添加数据, 如每次 100,000 行.
- 在上传数据之前, 通过月份分组数据.

下面操作性能不会下降:

- 数据实时插入.
- 上传的数据通过时间来排序.

