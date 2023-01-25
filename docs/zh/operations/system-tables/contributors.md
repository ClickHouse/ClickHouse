# system.contributors {#system-contributors}

此系统表包含有关贡献者的信息。排列顺序是在查询执行时随机生成的。

列:

-   `name` (String) — git 日志中的贡献者 (作者) 名字。

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

要在表中找到你自己，请这样查询:

``` sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

[原文](https://clickhouse.com/docs/zh/operations/system-tables/contributors) <!--hide-->
