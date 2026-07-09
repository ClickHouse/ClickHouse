---
description: 'GROUPING集約関数のドキュメント。'
slug: /sql-reference/aggregate-functions/grouping_function
title: 'GROUPING'
doc_type: 'reference'
---

<div id="grouping">
  ## GROUPING
</div>

[ROLLUP](../statements/select/group-by.md/#rollup-modifier) と [CUBE](../statements/select/group-by.md/#cube-modifier) は、GROUP BY の修飾子です。どちらも小計を計算します。ROLLUP は、たとえば `(day, month, year)` のような順序付きのカラムのリストを受け取り、集約の各レベルで小計を計算し、最後に総計を計算します。CUBE は、指定されたカラムのすべての組み合わせに対して小計を計算します。GROUPING は、ROLLUP または CUBE が返す行のうち、どれが上位集計行で、どれが修飾子なしの GROUP BY で返される行かを識別します。

GROUPING 関数は複数のカラムを引数として受け取り、ビットマスクを返します。

* `1` は、`GROUP BY` に対する `ROLLUP` または `CUBE` 修飾子によって返された行が小計であることを示します
* `0` は、`ROLLUP` または `CUBE` によって返された行が小計ではないことを示します

<div id="grouping-sets">
  ## GROUPING SETS
</div>

デフォルトでは、CUBE 修飾子は、CUBE に渡されたカラムのあらゆる組み合わせに対して小計を計算します。GROUPING SETS を使うと、計算対象とする組み合わせを個別に指定できます。

階層データの分析は、ROLLUP、CUBE、GROUPING SETS 修飾子の代表的なユースケースです。ここでのサンプルは、2 つのデータセンターでどの Linux ディストリビューションとそのバージョンがインストールされているかに関するデータを含むテーブルです。こうしたデータは、ディストリビューション、バージョン、場所ごとに見ると有用です。

<div id="load-sample-data">
  ### サンプルデータを読み込む
</div>

```sql
CREATE TABLE servers ( datacenter VARCHAR(255),
                         distro VARCHAR(255) NOT NULL,
                         version VARCHAR(50) NOT NULL,
                         quantity INT
                       )
                        ORDER BY (datacenter, distro, version)
```

```sql
INSERT INTO servers(datacenter, distro, version, quantity)
VALUES ('Schenectady', 'Arch','2022.08.05',50),
       ('Westport', 'Arch','2022.08.05',40),
       ('Schenectady','Arch','2021.09.01',30),
       ('Westport', 'Arch','2021.09.01',20),
       ('Schenectady','Arch','2020.05.01',10),
       ('Westport', 'Arch','2020.05.01',5),
       ('Schenectady','RHEL','9',60),
       ('Westport','RHEL','9',70),
       ('Westport','RHEL','7',80),
       ('Schenectady','RHEL','7',80)
```

```sql
SELECT 
    *
FROM
    servers;
```

```response
┌─datacenter──┬─distro─┬─version────┬─quantity─┐
│ Schenectady │ Arch   │ 2020.05.01 │       10 │
│ Schenectady │ Arch   │ 2021.09.01 │       30 │
│ Schenectady │ Arch   │ 2022.08.05 │       50 │
│ Schenectady │ RHEL   │ 7          │       80 │
│ Schenectady │ RHEL   │ 9          │       60 │
│ Westport    │ Arch   │ 2020.05.01 │        5 │
│ Westport    │ Arch   │ 2021.09.01 │       20 │
│ Westport    │ Arch   │ 2022.08.05 │       40 │
│ Westport    │ RHEL   │ 7          │       80 │
│ Westport    │ RHEL   │ 9          │       70 │
└─────────────┴────────┴────────────┴──────────┘

10 rows in set. Elapsed: 0.409 sec.
```

<div id="simple-queries">
  ### シンプルなクエリ
</div>

各データセンターのサーバー数を ディストリビューション ごとに取得します:

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter,
    distro;
```

```response
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘

4 rows in set. Elapsed: 0.212 sec.
```

```sql
SELECT
    datacenter, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter;
```

```response
┌─datacenter──┬─qty─┐
│ Westport    │ 215 │
│ Schenectady │ 230 │
└─────────────┴─────┘

2 rows in set. Elapsed: 0.277 sec. 
```

```sql
SELECT
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    distro;
```

```response

┌─distro─┬─qty─┐
│ Arch   │ 155 │
│ RHEL   │ 290 │
└────────┴─────┘

2 rows in set. Elapsed: 0.352 sec. 
```

```sql
SELECT
    SUM(quantity) qty
FROM
    servers;
```

```response
┌─qty─┐
│ 445 │
└─────┘

1 row in set. Elapsed: 0.244 sec. 
```

<div id="comparing-multiple-group-by-statements-with-grouping-sets">
  ### 複数の GROUP BY ステートメントと GROUPING SETS の比較
</div>

CUBE、ROLLUP、または GROUPING SETS を使わずにデータを分解すると、次のようになります:

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter,
    distro
UNION ALL
SELECT
    datacenter, 
    null,
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter
UNION ALL
SELECT
    null,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    distro
UNION ALL
SELECT
    null,
    null,
    SUM(quantity) qty
FROM
    servers;
```

```response
┌─datacenter─┬─distro─┬─qty─┐
│ ᴺᵁᴸᴸ       │ ᴺᵁᴸᴸ   │ 445 │
└────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Westport    │ ᴺᵁᴸᴸ   │ 215 │
│ Schenectady │ ᴺᵁᴸᴸ   │ 230 │
└─────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│ ᴺᵁᴸᴸ       │ Arch   │ 155 │
│ ᴺᵁᴸᴸ       │ RHEL   │ 290 │
└────────────┴────────┴─────┘

9 rows in set. Elapsed: 0.527 sec. 
```

GROUPING SETSで同じ情報を取得する:

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    GROUPING SETS(
        (datacenter,distro),
        (datacenter),
        (distro),
        ()
    )
```

```response
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Westport    │        │ 215 │
│ Schenectady │        │ 230 │
└─────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│            │        │ 445 │
└────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│            │ Arch   │ 155 │
│            │ RHEL   │ 290 │
└────────────┴────────┴─────┘

9 rows in set. Elapsed: 0.427 sec.
```

<div id="comparing-cube-with-grouping-sets">
  ### CUBE と GROUPING SETS の比較
</div>

次のクエリの CUBE、`CUBE(datacenter,distro,version)` は、意味のない階層になる可能性があります。2 つのディストリビューションをまたいでバージョンを見ることには意味がありません (Arch と RHEL ではリリースサイクルもバージョン命名規則も異なるためです) 。この後に続く GROUPING SETS の例のほうが、`distro` と `version` を同じセットにまとめているため、より適切です。

```sql
SELECT
   datacenter,
   distro,
   version,
   SUM(quantity)
FROM
   servers
GROUP BY
   CUBE(datacenter,distro,version)
ORDER BY
   datacenter,
   distro;
```

```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│             │        │ 7          │           160 │
│             │        │ 2020.05.01 │            15 │
│             │        │ 2021.09.01 │            50 │
│             │        │ 2022.08.05 │            90 │
│             │        │ 9          │           130 │
│             │        │            │           445 │
│             │ Arch   │ 2021.09.01 │            50 │
│             │ Arch   │ 2022.08.05 │            90 │
│             │ Arch   │ 2020.05.01 │            15 │
│             │ Arch   │            │           155 │
│             │ RHEL   │ 9          │           130 │
│             │ RHEL   │ 7          │           160 │
│             │ RHEL   │            │           290 │
│ Schenectady │        │ 9          │            60 │
│ Schenectady │        │ 2021.09.01 │            30 │
│ Schenectady │        │ 7          │            80 │
│ Schenectady │        │ 2022.08.05 │            50 │
│ Schenectady │        │ 2020.05.01 │            10 │
│ Schenectady │        │            │           230 │
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
│ Schenectady │ Arch   │            │            90 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Schenectady │ RHEL   │            │           140 │
│ Westport    │        │ 9          │            70 │
│ Westport    │        │ 2020.05.01 │             5 │
│ Westport    │        │ 2022.08.05 │            40 │
│ Westport    │        │ 7          │            80 │
│ Westport    │        │ 2021.09.01 │            20 │
│ Westport    │        │            │           215 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Westport    │ Arch   │            │            65 │
│ Westport    │ RHEL   │ 9          │            70 │
│ Westport    │ RHEL   │ 7          │            80 │
│ Westport    │ RHEL   │            │           150 │
└─────────────┴────────┴────────────┴───────────────┘

39 rows in set. Elapsed: 0.355 sec. 
```

:::note
上記の例では、バージョンがディストリビューションに紐付いていない場合、その値だけではあまり意味を持たない可能性があります。追跡対象がカーネルのバージョンであれば、カーネルのバージョンはどちらのディストリビューションにも関連付けられるため、意味を持つかもしれません。このようなケースでは、次の例のように GROUPING SETS を使用するほうが適切な場合があります。
:::

```sql
SELECT
    datacenter,
    distro,
    version,
    SUM(quantity)
FROM servers
GROUP BY
    GROUPING SETS (
        (datacenter, distro, version),
        (datacenter, distro))
```

```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│ Westport    │ RHEL   │ 9          │            70 │
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
└─────────────┴────────┴────────────┴───────────────┘
┌─datacenter──┬─distro─┬─version─┬─sum(quantity)─┐
│ Schenectady │ RHEL   │         │           140 │
│ Westport    │ Arch   │         │            65 │
│ Schenectady │ Arch   │         │            90 │
│ Westport    │ RHEL   │         │           150 │
└─────────────┴────────┴─────────┴───────────────┘

14 rows in set. Elapsed: 1.036 sec. 
```