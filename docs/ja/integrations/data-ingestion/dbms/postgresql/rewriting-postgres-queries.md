---
slug: /ja/migrations/postgresql/rewriting-queries
title: PostgreSQL クエリの書き換え
keywords: [postgres, postgresql, rewriting queries, rewrite query]
---

# ClickHouseでのPostgreSQLクエリの書き換え

> これはPostgreSQLからClickHouseへの移行ガイドの**第4部**です。このコンテンツは導入として考えられ、ユーザーがClickHouseのベストプラクティスに従った初期の機能的なシステムを展開するのを助けることを目的としています。このガイドは複雑なトピックを避け、完全に最適化されたスキーマを生み出すわけではなく、ユーザーがプロダクションシステムを構築し、学習を基にするための堅実な基盤を提供します。

以下は、PostgreSQLとClickHouseを比較するクエリの例を提供します。このリストは、ClickHouseの機能を活用してクエリを大幅に簡略化する方法を示すことを目的としています。これらのクエリはほとんどの場合、ClickHouseでより迅速に実行されます。ここでの例は、PostgreSQLとClickHouseの同等のリソース（8コア、32GiB RAM）での完全な[Stack Overflow データセット](https://clickhouse.com/docs/ja/getting-started/example-datasets/stackoverflow)（2024年4月まで）の使用に基づいています。

> ここでのカウントは若干異なります。Postgresデータは外部キーの参照整合性を満たす行のみが含まれているためです。ClickHouseはそのような制約を課さず、匿名ユーザーを含むフルデータセットを持っています。

最もビューを獲得した質問が10件を超えるユーザー:

```sql
-- ClickHouse
SELECT OwnerDisplayName, sum(ViewCount) AS total_views
FROM stackoverflow.posts
WHERE (PostTypeId = 'Question') AND (OwnerDisplayName != '')
GROUP BY OwnerDisplayName
HAVING count() > 10
ORDER BY total_views DESC
LIMIT 5

┌─OwnerDisplayName────────┬─total_views─┐
│ Joan Venge          	│	25520387 │
│ Ray Vega            	│	21576470 │
│ anon                	│	19814224 │
│ Tim                 	│	19028260 │
│ John                	│	17638812 │
└─────────────────────────┴─────────────┘

5 rows in set. Elapsed: 0.360 sec. Processed 24.37 million rows, 140.45 MB (67.73 million rows/s., 390.38 MB/s.)
Peak memory usage: 510.71 MiB.
```

```sql
--Postgres
SELECT OwnerDisplayName, SUM(ViewCount) AS total_views
FROM public.posts
WHERE (PostTypeId = 1) AND (OwnerDisplayName != '')
GROUP BY OwnerDisplayName
HAVING COUNT(*) > 10
ORDER BY total_views DESC
LIMIT 10;

	ownerdisplayname 	| total_views
-------------------------+-------------
 Joan Venge          	|	25520387
 Ray Vega            	|	21576470
 Tim                 	|	18283579
 J. Pablo Fern&#225;ndez |	12446818
 Matt                	|	12298764

Time: 107620.508 ms (01:47.621)
```

最もビューを獲得した`タグ`:

```sql
--ClickHouse
SELECT arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', Tags))) AS tags,
	sum(ViewCount) AS views
FROM posts
GROUP BY tags
ORDER BY views DESC
LIMIT 5

┌─tags───────┬──────views─┐
│ javascript │ 8190916894 │
│ python 	│ 8175132834 │
│ java   	│ 7258379211 │
│ c#     	│ 5476932513 │
│ android	│ 4258320338 │
└────────────┴────────────┘

5 rows in set. Elapsed: 0.908 sec. Processed 59.82 million rows, 1.45 GB (65.87 million rows/s., 1.59 GB/s.)
```

```sql
--Postgres
WITH tags_exploded AS (
	SELECT
    	unnest(string_to_array(Tags, '|')) AS tag,
    	ViewCount
	FROM public.posts
),
filtered_tags AS (
	SELECT
    	tag,
    	ViewCount
	FROM tags_exploded
	WHERE tag <> ''
)
SELECT tag AS tags,
	SUM(ViewCount) AS views
FROM filtered_tags
GROUP BY tag
ORDER BY views DESC
LIMIT 5;

	tags	|   views
------------+------------
 javascript | 7974880378
 python 	| 7972340763
 java   	| 7064073461
 c#     	| 5308656277
 android	| 4186216900
(5 rows)

Time: 112508.083 ms (01:52.508)
```

**集計関数**

可能な場合、ユーザーはClickHouseの集計関数を活用するべきです。以下は、毎年最もビューを獲得した質問を計算するための[argMax](/ja/sql-reference/aggregate-functions/reference/argmax)関数の使用を示しています。

```sql
--ClickHouse
SELECT  toYear(CreationDate) AS Year,
	argMax(Title, ViewCount) AS MostViewedQuestionTitle,
	max(ViewCount) AS MaxViewCount
FROM stackoverflow.posts
WHERE PostTypeId = 'Question'
GROUP BY Year
ORDER BY Year ASC
FORMAT Vertical
Row 1:
──────
Year:                	2008
MostViewedQuestionTitle: How to find the index for a given item in a list?
MaxViewCount:        	6316987

Row 2:
──────
Year:                	2009
MostViewedQuestionTitle: How do I undo the most recent local commits in Git?
MaxViewCount:        	13962748

...

Row 16:
───────
Year:                	2023
MostViewedQuestionTitle: How do I solve "error: externally-managed-environment" every time I use pip 3?
MaxViewCount:        	506822

Row 17:
───────
Year:                	2024
MostViewedQuestionTitle: Warning "Third-party cookie will be blocked. Learn more in the Issues tab"
MaxViewCount:        	66975

17 rows in set. Elapsed: 0.677 sec. Processed 24.37 million rows, 1.86 GB (36.01 million rows/s., 2.75 GB/s.)
Peak memory usage: 554.31 MiB.
```

これはPostgresクエリの同等のものよりも大幅に簡単（かつ速い）です:

```sql
--Postgres
WITH yearly_views AS (
	SELECT
    	EXTRACT(YEAR FROM CreationDate) AS Year,
    	Title,
    	ViewCount,
    	ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM CreationDate) ORDER BY ViewCount DESC) AS rn
	FROM public.posts
	WHERE PostTypeId = 1
)
SELECT
	Year,
	Title AS MostViewedQuestionTitle,
	ViewCount AS MaxViewCount
FROM yearly_views
WHERE rn = 1
ORDER BY Year;
 year |                                            	mostviewedquestiontitle                                            	| maxviewcount
------+-----------------------------------------------------------------------------------------------------------------------+--------------
 2008 | How to find the index for a given item in a list?                                                                 	|  	6316987
 2009 | How do I undo the most recent local commits in Git?                                                               	| 	13962748

...

 2023 | How do I solve "error: externally-managed-environment" every time I use pip 3?                                    	|   	506822
 2024 | Warning "Third-party cookie will be blocked. Learn more in the Issues tab"                                        	|    	66975
(17 rows)

Time: 125822.015 ms (02:05.822)
```

**条件文と配列**

条件文と配列関数を使用すると、クエリが大幅に簡略化されます。以下のクエリは、2022年から2023年にかけて10000件以上の発生があったタグの中から、最大の割合増加を計算します。以下のClickHouseクエリでは、条件文、配列関数、およびHAVINGおよびSELECT句でのエイリアスの再利用が可能であることがわかります。

```sql
--ClickHouse
SELECT  arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', Tags))) AS tag,
	countIf(toYear(CreationDate) = 2023) AS count_2023,
	countIf(toYear(CreationDate) = 2022) AS count_2022,
	((count_2023 - count_2022) / count_2022) * 100 AS percent_change
FROM stackoverflow.posts
WHERE toYear(CreationDate) IN (2022, 2023)
GROUP BY tag
HAVING (count_2022 > 10000) AND (count_2023 > 10000)
ORDER BY percent_change DESC
LIMIT 5

┌─tag─────────┬─count_2023─┬─count_2022─┬──────percent_change─┐
│ next.js 	│  	13788 │  	10520 │   31.06463878326996 │
│ spring-boot │  	16573 │  	17721 │  -6.478189718413183 │
│ .net    	│  	11458 │  	12968 │ -11.644046884639112 │
│ azure   	│  	11996 │  	14049 │ -14.613139725247349 │
│ docker  	│  	13885 │  	16877 │  -17.72826924216389 │
└─────────────┴────────────┴────────────┴─────────────────────┘

5 rows in set. Elapsed: 0.247 sec. Processed 5.08 million rows, 155.73 MB (20.58 million rows/s., 630.61 MB/s.)
Peak memory usage: 403.04 MiB.
```

```sql
--Postgres
SELECT
	tag,
	SUM(CASE WHEN year = 2023 THEN count ELSE 0 END) AS count_2023,
	SUM(CASE WHEN year = 2022 THEN count ELSE 0 END) AS count_2022,
	((SUM(CASE WHEN year = 2023 THEN count ELSE 0 END) - SUM(CASE WHEN year = 2022 THEN count ELSE 0 END))
 	/ SUM(CASE WHEN year = 2022 THEN count ELSE 0 END)::float) * 100 AS percent_change
FROM (
	SELECT
    	unnest(string_to_array(Tags, '|')) AS tag,
    	EXTRACT(YEAR FROM CreationDate) AS year,
    	COUNT(*) AS count
	FROM public.posts
	WHERE EXTRACT(YEAR FROM CreationDate) IN (2022, 2023)
  	AND Tags <> ''
	GROUP BY tag, year
) AS yearly_counts
GROUP BY tag
HAVING SUM(CASE WHEN year = 2022 THEN count ELSE 0 END) > 10000
   AND SUM(CASE WHEN year = 2023 THEN count ELSE 0 END) > 10000
ORDER BY percent_change DESC
LIMIT 5;

 	tag 	| count_2023 | count_2022 |   percent_change
-------------+------------+------------+---------------------
 next.js 	|  	13712 |  	10370 |   32.22757955641273
 spring-boot |  	16482 |  	17474 |  -5.677005837243905
 .net    	|  	11376 |  	12750 | -10.776470588235295
 azure   	|  	11938 |  	13966 | -14.520979521695546
 docker  	|  	13832 |  	16701 | -17.178612059158134
(5 rows)

Time: 116750.131 ms (01:56.750)
```

これで、PostgresからClickHouseへの移行を検討しているユーザー向けの基本ガイドは終了です。Postgresから移行するユーザーには、[ClickHouseでのモデルデータの設計ガイド](/ja/data-modeling/schema-design)を読み、ClickHouseの高度な機能について学ぶことをお勧めします。
