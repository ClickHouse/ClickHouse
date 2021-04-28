# system.contributors {#system-contributors}

包含有关贡献者的信息。 该顺序在查询执行时是随机的。

列:

-   `name` (String) — 来自git log的贡献者（作者）名称。

**示例**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

要在表中查找自己，请使用查询:

``` sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/contributors) <!--hide-->
