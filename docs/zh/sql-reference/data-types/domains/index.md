---
toc_folder_title: "\u57DF"
toc_title: "域"
toc_priority: 56
---

# 域 {#domains}

Domain类型是特定实现的类型，它总是与某个现存的基础类型保持二进制兼容的同时添加一些额外的特性，以能够在维持磁盘数据不变的情况下使用这些额外的特性。目前ClickHouse暂不支持自定义domain类型。

如果你可以在一个地方使用与Domain类型二进制兼容的基础类型，那么在相同的地方您也可以使用Domain类型，例如：

-   使用Domain类型作为表中列的类型
-   对Domain类型的列进行读/写数据
-   如果与Domain二进制兼容的基础类型可以作为索引，那么Domain类型也可以作为索引
-   将Domain类型作为参数传递给函数使用
-   其他

### Domains的额外特性 {#domainsde-e-wai-te-xing}

-   在执行SHOW CREATE TABLE 或 DESCRIBE TABLE时，其对应的列总是展示为Domain类型的名称
-   在INSERT INTO domain_table(domain_column) VALUES(…)中输入数据总是以更人性化的格式进行输入
-   在SELECT domain_column FROM domain_table中数据总是以更人性化的格式输出
-   在INSERT INTO domain_table FORMAT CSV …中，实现外部源数据以更人性化的格式载入

### Domains类型的限制 {#domainslei-xing-de-xian-zhi}

-   无法通过`ALTER TABLE`将基础类型的索引转换为Domain类型的索引。
-   当从其他列或表插入数据时，无法将string类型的值隐式地转换为Domain类型的值。
-   无法对存储为Domain类型的值添加约束。

[来源文章](https://clickhouse.com/docs/en/data_types/domains/overview) <!--hide-->


