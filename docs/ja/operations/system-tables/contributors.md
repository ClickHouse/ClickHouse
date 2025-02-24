---
slug: /ja/operations/system-tables/contributors
---
# contributors

寄稿者に関する情報を含んでいます。順序はクエリ実行時にランダムです。

カラム:

- `name` (String) — git logからの寄稿者（著者）の名前。

**例**

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

表で自分自身を見つけ出すには、次のクエリを使用します:

``` sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
