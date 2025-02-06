---
slug: /ja/getting-started/example-datasets/brown-benchmark
sidebar_label: ブラウン大学ベンチマーク
description: 機械生成ログデータの新しい分析ベンチマーク
title: "ブラウン大学ベンチマーク"
---

`MgBench`は、機械生成ログデータの新しい分析ベンチマークです。[Andrew Crotty](http://cs.brown.edu/people/acrotty/)。

データをダウンロードします:
```
wget https://datasets.clickhouse.com/mgbench{1..3}.csv.xz
```

データを展開します:
```
xz -v -d mgbench{1..3}.csv.xz
```

データベースとテーブルを作成します:
```sql
CREATE DATABASE mgbench;
```

```sql
USE mgbench;
```

```sql
CREATE TABLE mgbench.logs1 (
  log_time      DateTime,
  machine_name  LowCardinality(String),
  machine_group LowCardinality(String),
  cpu_idle      Nullable(Float32),
  cpu_nice      Nullable(Float32),
  cpu_system    Nullable(Float32),
  cpu_user      Nullable(Float32),
  cpu_wio       Nullable(Float32),
  disk_free     Nullable(Float32),
  disk_total    Nullable(Float32),
  part_max_used Nullable(Float32),
  load_fifteen  Nullable(Float32),
  load_five     Nullable(Float32),
  load_one      Nullable(Float32),
  mem_buffers   Nullable(Float32),
  mem_cached    Nullable(Float32),
  mem_free      Nullable(Float32),
  mem_shared    Nullable(Float32),
  swap_free     Nullable(Float32),
  bytes_in      Nullable(Float32),
  bytes_out     Nullable(Float32)
)
ENGINE = MergeTree()
ORDER BY (machine_group, machine_name, log_time);
```


```sql
CREATE TABLE mgbench.logs2 (
  log_time    DateTime,
  client_ip   IPv4,
  request     String,
  status_code UInt16,
  object_size UInt64
)
ENGINE = MergeTree()
ORDER BY log_time;
```


```sql
CREATE TABLE mgbench.logs3 (
  log_time     DateTime64,
  device_id    FixedString(15),
  device_name  LowCardinality(String),
  device_type  LowCardinality(String),
  device_floor UInt8,
  event_type   LowCardinality(String),
  event_unit   FixedString(1),
  event_value  Nullable(Float32)
)
ENGINE = MergeTree()
ORDER BY (event_type, log_time);
```

データを挿入します:

```
clickhouse-client --query "INSERT INTO mgbench.logs1 FORMAT CSVWithNames" < mgbench1.csv
clickhouse-client --query "INSERT INTO mgbench.logs2 FORMAT CSVWithNames" < mgbench2.csv
clickhouse-client --query "INSERT INTO mgbench.logs3 FORMAT CSVWithNames" < mgbench3.csv
```

## ベンチマーククエリを実行:

```sql
USE mgbench;
```

```sql
-- Q1.1: 各ウェブサーバのCPU/ネットワーク使用率は、今日の午前0時以降どのくらいですか？

SELECT machine_name,
       MIN(cpu) AS cpu_min,
       MAX(cpu) AS cpu_max,
       AVG(cpu) AS cpu_avg,
       MIN(net_in) AS net_in_min,
       MAX(net_in) AS net_in_max,
       AVG(net_in) AS net_in_avg,
       MIN(net_out) AS net_out_min,
       MAX(net_out) AS net_out_max,
       AVG(net_out) AS net_out_avg
FROM (
  SELECT machine_name,
         COALESCE(cpu_user, 0.0) AS cpu,
         COALESCE(bytes_in, 0.0) AS net_in,
         COALESCE(bytes_out, 0.0) AS net_out
  FROM logs1
  WHERE machine_name IN ('anansi','aragog','urd')
    AND log_time >= TIMESTAMP '2017-01-11 00:00:00'
) AS r
GROUP BY machine_name;
```


```sql
-- Q1.2: コンピューターラボのマシンのうち、過去1日間にオフラインになっていたものはどれですか？

SELECT machine_name,
       log_time
FROM logs1
WHERE (machine_name LIKE 'cslab%' OR
       machine_name LIKE 'mslab%')
  AND load_one IS NULL
  AND log_time >= TIMESTAMP '2017-01-10 00:00:00'
ORDER BY machine_name,
         log_time;
```

```sql
-- Q1.3: 特定のワークステーションの過去10日間の毎時平均メトリクスはどのくらいですか？

SELECT dt,
       hr,
       AVG(load_fifteen) AS load_fifteen_avg,
       AVG(load_five) AS load_five_avg,
       AVG(load_one) AS load_one_avg,
       AVG(mem_free) AS mem_free_avg,
       AVG(swap_free) AS swap_free_avg
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         EXTRACT(HOUR FROM log_time) AS hr,
         load_fifteen,
         load_five,
         load_one,
         mem_free,
         swap_free
  FROM logs1
  WHERE machine_name = 'babbage'
    AND load_fifteen IS NOT NULL
    AND load_five IS NOT NULL
    AND load_one IS NOT NULL
    AND mem_free IS NOT NULL
    AND swap_free IS NOT NULL
    AND log_time >= TIMESTAMP '2017-01-01 00:00:00'
) AS r
GROUP BY dt,
         hr
ORDER BY dt,
         hr;
```

```sql
-- Q1.4: 1か月間で、各サーバーがディスクI/Oでブロックされた頻度はどのくらいですか？

SELECT machine_name,
       COUNT(*) AS spikes
FROM logs1
WHERE machine_group = 'Servers'
  AND cpu_wio > 0.99
  AND log_time >= TIMESTAMP '2016-12-01 00:00:00'
  AND log_time < TIMESTAMP '2017-01-01 00:00:00'
GROUP BY machine_name
ORDER BY spikes DESC
LIMIT 10;
```

```sql
-- Q1.5: メモリ不足になった外部アクセス可能なVMはどれですか？

SELECT machine_name,
       dt,
       MIN(mem_free) AS mem_free_min
FROM (
  SELECT machine_name,
         CAST(log_time AS DATE) AS dt,
         mem_free
  FROM logs1
  WHERE machine_group = 'DMZ'
    AND mem_free IS NOT NULL
) AS r
GROUP BY machine_name,
         dt
HAVING MIN(mem_free) < 10000
ORDER BY machine_name,
         dt;
```

```sql
-- Q1.6: ファイルサーバー全体の毎時ネットワークトラフィックの合計はどのくらいですか？

SELECT dt,
       hr,
       SUM(net_in) AS net_in_sum,
       SUM(net_out) AS net_out_sum,
       SUM(net_in) + SUM(net_out) AS both_sum
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         EXTRACT(HOUR FROM log_time) AS hr,
         COALESCE(bytes_in, 0.0) / 1000000000.0 AS net_in,
         COALESCE(bytes_out, 0.0) / 1000000000.0 AS net_out
  FROM logs1
  WHERE machine_name IN ('allsorts','andes','bigred','blackjack','bonbon',
      'cadbury','chiclets','cotton','crows','dove','fireball','hearts','huey',
      'lindt','milkduds','milkyway','mnm','necco','nerds','orbit','peeps',
      'poprocks','razzles','runts','smarties','smuggler','spree','stride',
      'tootsie','trident','wrigley','york')
) AS r
GROUP BY dt,
         hr
ORDER BY both_sum DESC
LIMIT 10;
```

```sql
-- Q2.1: 過去2週間にサーバーエラーを引き起こしたリクエストはどれですか？

SELECT *
FROM logs2
WHERE status_code >= 500
  AND log_time >= TIMESTAMP '2012-12-18 00:00:00'
ORDER BY log_time;
```

```sql
-- Q2.2: 特定の2週間の期間中にユーザーパスワードファイルが漏洩しましたか？

SELECT *
FROM logs2
WHERE status_code >= 200
  AND status_code < 300
  AND request LIKE '%/etc/passwd%'
  AND log_time >= TIMESTAMP '2012-05-06 00:00:00'
  AND log_time < TIMESTAMP '2012-05-20 00:00:00';
```


```sql
-- Q2.3: 過去1か月のトップレベルリクエストの平均パス深さはどのくらいですか？

SELECT top_level,
       AVG(LENGTH(request) - LENGTH(REPLACE(request, '/', ''))) AS depth_avg
FROM (
  SELECT SUBSTRING(request FROM 1 FOR len) AS top_level,
         request
  FROM (
    SELECT POSITION(SUBSTRING(request FROM 2), '/') AS len,
           request
    FROM logs2
    WHERE status_code >= 200
      AND status_code < 300
      AND log_time >= TIMESTAMP '2012-12-01 00:00:00'
  ) AS r
  WHERE len > 0
) AS s
WHERE top_level IN ('/about','/courses','/degrees','/events',
                    '/grad','/industry','/news','/people',
                    '/publications','/research','/teaching','/ugrad')
GROUP BY top_level
ORDER BY top_level;
```


```sql
-- Q2.4: 過去3か月間で過剰なリクエストを行ったクライアントはどれですか？

SELECT client_ip,
       COUNT(*) AS num_requests
FROM logs2
WHERE log_time >= TIMESTAMP '2012-10-01 00:00:00'
GROUP BY client_ip
HAVING COUNT(*) >= 100000
ORDER BY num_requests DESC;
```


```sql
-- Q2.5: 日別のユニークビジター数はどれだけですか？

SELECT dt,
       COUNT(DISTINCT client_ip)
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         client_ip
  FROM logs2
) AS r
GROUP BY dt
ORDER BY dt;
```


```sql
-- Q2.6: 平均および最大データ転送速度 (Gbps) はどれだけですか？

SELECT AVG(transfer) / 125000000.0 AS transfer_avg,
       MAX(transfer) / 125000000.0 AS transfer_max
FROM (
  SELECT log_time,
         SUM(object_size) AS transfer
  FROM logs2
  GROUP BY log_time
) AS r;
```


```sql
-- Q3.1: 室内温度が週末に氷点下になったことがありますか？

SELECT *
FROM logs3
WHERE event_type = 'temperature'
  AND event_value <= 32.0
  AND log_time >= '2019-11-29 17:00:00.000';
```


```sql
-- Q3.4: 過去6ヶ月間で各ドアが開かれた頻度はどのくらいですか？

SELECT device_name,
       device_floor,
       COUNT(*) AS ct
FROM logs3
WHERE event_type = 'door_open'
  AND log_time >= '2019-06-01 00:00:00.000'
GROUP BY device_name,
         device_floor
ORDER BY ct DESC;
```

以下のクエリ3.5ではUNIONを使用しています。SELECTクエリ結果を結合するモードを設定します。この設定は、明示的にUNION ALLまたはUNION DISTINCTを指定していない場合に、UNIONと共有されているときにのみ使用されます。
```sql
SET union_default_mode = 'DISTINCT'
```

```sql
-- Q3.5: 冬と夏で大きな温度変動が発生するのは建物のどこですか？

WITH temperature AS (
  SELECT dt,
         device_name,
         device_type,
         device_floor
  FROM (
    SELECT dt,
           hr,
           device_name,
           device_type,
           device_floor,
           AVG(event_value) AS temperature_hourly_avg
    FROM (
      SELECT CAST(log_time AS DATE) AS dt,
             EXTRACT(HOUR FROM log_time) AS hr,
             device_name,
             device_type,
             device_floor,
             event_value
      FROM logs3
      WHERE event_type = 'temperature'
    ) AS r
    GROUP BY dt,
             hr,
             device_name,
             device_type,
             device_floor
  ) AS s
  GROUP BY dt,
           device_name,
           device_type,
           device_floor
  HAVING MAX(temperature_hourly_avg) - MIN(temperature_hourly_avg) >= 25.0
)
SELECT DISTINCT device_name,
       device_type,
       device_floor,
       'WINTER'
FROM temperature
WHERE dt >= DATE '2018-12-01'
  AND dt < DATE '2019-03-01'
UNION
SELECT DISTINCT device_name,
       device_type,
       device_floor,
       'SUMMER'
FROM temperature
WHERE dt >= DATE '2019-06-01'
  AND dt < DATE '2019-09-01';
```


```sql
-- Q3.6: 各デバイスカテゴリについて、月別の電力消費メトリクスはどのくらいですか？

SELECT yr,
       mo,
       SUM(coffee_hourly_avg) AS coffee_monthly_sum,
       AVG(coffee_hourly_avg) AS coffee_monthly_avg,
       SUM(printer_hourly_avg) AS printer_monthly_sum,
       AVG(printer_hourly_avg) AS printer_monthly_avg,
       SUM(projector_hourly_avg) AS projector_monthly_sum,
       AVG(projector_hourly_avg) AS projector_monthly_avg,
       SUM(vending_hourly_avg) AS vending_monthly_sum,
       AVG(vending_hourly_avg) AS vending_monthly_avg
FROM (
  SELECT dt,
         yr,
         mo,
         hr,
         AVG(coffee) AS coffee_hourly_avg,
         AVG(printer) AS printer_hourly_avg,
         AVG(projector) AS projector_hourly_avg,
         AVG(vending) AS vending_hourly_avg
  FROM (
    SELECT CAST(log_time AS DATE) AS dt,
           EXTRACT(YEAR FROM log_time) AS yr,
           EXTRACT(MONTH FROM log_time) AS mo,
           EXTRACT(HOUR FROM log_time) AS hr,
           CASE WHEN device_name LIKE 'coffee%' THEN event_value END AS coffee,
           CASE WHEN device_name LIKE 'printer%' THEN event_value END AS printer,
           CASE WHEN device_name LIKE 'projector%' THEN event_value END AS projector,
           CASE WHEN device_name LIKE 'vending%' THEN event_value END AS vending
    FROM logs3
    WHERE device_type = 'meter'
  ) AS r
  GROUP BY dt,
           yr,
           mo,
           hr
) AS s
GROUP BY yr,
         mo
ORDER BY yr,
         mo;
```

このデータは、[Playground](https://sql.clickhouse.com)、[例](https://sql.clickhouse.com?query_id=1MXMHASDLEQIP4P1D1STND)でもインタラクティブなクエリに利用できます。
