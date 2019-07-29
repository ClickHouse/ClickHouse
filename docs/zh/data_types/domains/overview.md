# Domains

Domains是特殊用途的类型，它在现有的基础类型之上添加了一些额外的特性，能够让线上和磁盘上的表格式保持不变。目前，ClickHouse暂不支持自定义的domains.

您可以在任何地方使用domains，相应的基础类型的使用方式如下：

* 构建一列domain类型的数据
* 从/向domain列读/写数据
* 作为索引，如果基础类型能够被作为索引的话
* 以domain列的值作为参数调用函数
* 等等.

### Domains的额外特性

* 在使用`SHOW CREATE TABLE` 或 `DESCRIBE TABLE`时，明确地显示列类型名称
* 使用 `INSERT INTO domain_table(domain_column) VALUES(...)`实现人性化格式输入
* 使用`SELECT domain_column FROM domain_table`实现人性化格式输出 
* 使用 `INSERT INTO domain_table FORMAT CSV ...`实现外部源数据的人性化格式载入

### 缺陷

* 无法通过 `ALTER TABLE`将基础类型的索引转换为domain类型的索引.
* 当从其他列或表插入数据时，无法将string类型的值隐式地转换为domain类型的值.
* 无法对存储为domain类型的值添加约束.

[Original article](https://clickhouse.yandex/docs/en/data_types/domains/overview) <!--hide-->
