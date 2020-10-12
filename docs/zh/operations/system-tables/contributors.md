---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。贡献者 {#system-contributors}

包含有关贡献者的信息。 该顺序在查询执行时是随机的。

列:

-   `name` (String) — Contributor (author) name from git log.

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

要在表中找出自己，请使用查询:

``` sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
