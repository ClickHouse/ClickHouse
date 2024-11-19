---
slug: /ja/getting-started/example-datasets/menus
sidebar_label: ニューヨーク公共図書館「What's on the Menu?」データセット
title: "ニューヨーク公共図書館「What's on the Menu?」データセット"
---

このデータセットはニューヨーク公共図書館によって作成されました。ホテル、レストラン、カフェのメニューに関する歴史的データで、料理名とそれに対応する価格が含まれています。

出典: http://menus.nypl.org/data  
このデータはパブリックドメインにあります。

このデータは図書館のアーカイブからのものであり、統計分析には不完全で困難かもしれません。しかし、それでもとても魅力的なデータです。メニューに記載されている料理のレコードは約130万件あり、ClickHouseに適しているほど小さなデータ量ですが、良い例です。

## データセットをダウンロードする {#download-dataset}

以下のコマンドを実行します:

```bash
wget https://s3.amazonaws.com/menusdata.nypl.org/gzips/2021_08_01_07_01_17_data.tgz
# オプション: チェックサムを検証する
md5sum 2021_08_01_07_01_17_data.tgz
# チェックサムは次と一致する必要があります: db6126724de939a5481e3160a2d67d15
```

必要に応じて、最新のリンクを http://menus.nypl.org/data から取得してリンクを置き換えてください。ダウンロードサイズは約35MBです。

## データセットを解凍する {#unpack-dataset}

```bash
tar xvf 2021_08_01_07_01_17_data.tgz
```

解凍後のサイズは約150MBです。

データは正規化されており、4つのテーブルで構成されています:
- `Menu` — メニューに関する情報: レストラン名、メニューが表示された日付など。
- `Dish` — 料理に関する情報: 料理名といくつかの特徴。
- `MenuPage` — メニューのページ情報。すべてのページは特定のメニューに属します。
- `MenuItem` — メニュー項目。あるメニューページ上の料理とその価格: 料理とメニューページへのリンク。

## テーブルを作成する {#create-tables}

価格を保存するために[Decimal](../../sql-reference/data-types/decimal.md)データ型を使用します。

```sql
CREATE TABLE dish
(
    id UInt32,
    name String,
    description String,
    menus_appeared UInt32,
    times_appeared Int32,
    first_appeared UInt16,
    last_appeared UInt16,
    lowest_price Decimal64(3),
    highest_price Decimal64(3)
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE menu
(
    id UInt32,
    name String,
    sponsor String,
    event String,
    venue String,
    place String,
    physical_description String,
    occasion String,
    notes String,
    call_number String,
    keywords String,
    language String,
    date String,
    location String,
    location_type String,
    currency String,
    currency_symbol String,
    status String,
    page_count UInt16,
    dish_count UInt16
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE menu_page
(
    id UInt32,
    menu_id UInt32,
    page_number UInt16,
    image_id String,
    full_height UInt16,
    full_width UInt16,
    uuid UUID
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE menu_item
(
    id UInt32,
    menu_page_id UInt32,
    price Decimal64(3),
    high_price Decimal64(3),
    dish_id UInt32,
    created_at DateTime,
    updated_at DateTime,
    xpos Float64,
    ypos Float64
) ENGINE = MergeTree ORDER BY id;
```

## データをインポートする {#import-data}

ClickHouse にデータをアップロードするには、以下を実行します:

```bash
clickhouse-client --format_csv_allow_single_quotes 0 --input_format_null_as_default 0 --query "INSERT INTO dish FORMAT CSVWithNames" < Dish.csv
clickhouse-client --format_csv_allow_single_quotes 0 --input_format_null_as_default 0 --query "INSERT INTO menu FORMAT CSVWithNames" < Menu.csv
clickhouse-client --format_csv_allow_single_quotes 0 --input_format_null_as_default 0 --query "INSERT INTO menu_page FORMAT CSVWithNames" < MenuPage.csv
clickhouse-client --format_csv_allow_single_quotes 0 --input_format_null_as_default 0 --date_time_input_format best_effort --query "INSERT INTO menu_item FORMAT CSVWithNames" < MenuItem.csv
```

データはヘッダ付きのCSVで表されているため、[CSVWithNames](../../interfaces/formats.md#csvwithnames)形式を使用します。

データフィールドには二重引用符しか使用されず、一重引用符は値の中に含まれる可能性があり、CSVパーサを混乱させないように`format_csv_allow_single_quotes`を無効にします。

[NULL](../../sql-reference/syntax.md#null-literal)を持たないため、[input_format_null_as_default](../../operations/settings/settings-formats.md#settings-input-format-null-as-default)を無効にします。これにより、ClickHouseは`\N` シーケンスを解析しようとし、データ内の`\`で混乱することがあります。

設定[date_time_input_format best_effort](../../operations/settings/settings-formats.md#settings-date_time_input_format)は、幅広い形式で[DateTime](../../sql-reference/data-types/datetime.md)フィールドを解析することを可能にします。例えば、'2000-01-01 01:02'のような秒なしのISO-8601形式も認識されます。この設定がないと、固定されたDateTime形式のみが許可されます。

## データを非正規化する {#denormalize-data}

データは複数のテーブルに分けて[正規化](https://en.wikipedia.org/wiki/Database_normalization#Normal_forms)されています。つまり、例えばメニュー項目から料理名をクエリするには、[JOIN](../../sql-reference/statements/select/join.md#select-join)を実行する必要があります。典型的な分析タスクでは、毎回`JOIN`をする代わりに、事前にJOINされたデータを扱う方がはるかに効率的です。これを「非正規化」データと呼びます。

すべてのデータをJOINして含む`menu_item_denorm`というテーブルを作成します:

```sql
CREATE TABLE menu_item_denorm
ENGINE = MergeTree ORDER BY (dish_name, created_at)
AS SELECT
    price,
    high_price,
    created_at,
    updated_at,
    xpos,
    ypos,
    dish.id AS dish_id,
    dish.name AS dish_name,
    dish.description AS dish_description,
    dish.menus_appeared AS dish_menus_appeared,
    dish.times_appeared AS dish_times_appeared,
    dish.first_appeared AS dish_first_appeared,
    dish.last_appeared AS dish_last_appeared,
    dish.lowest_price AS dish_lowest_price,
    dish.highest_price AS dish_highest_price,
    menu.id AS menu_id,
    menu.name AS menu_name,
    menu.sponsor AS menu_sponsor,
    menu.event AS menu_event,
    menu.venue AS menu_venue,
    menu.place AS menu_place,
    menu.physical_description AS menu_physical_description,
    menu.occasion AS menu_occasion,
    menu.notes AS menu_notes,
    menu.call_number AS menu_call_number,
    menu.keywords AS menu_keywords,
    menu.language AS menu_language,
    menu.date AS menu_date,
    menu.location AS menu_location,
    menu.location_type AS menu_location_type,
    menu.currency AS menu_currency,
    menu.currency_symbol AS menu_currency_symbol,
    menu.status AS menu_status,
    menu.page_count AS menu_page_count,
    menu.dish_count AS menu_dish_count
FROM menu_item
    JOIN dish ON menu_item.dish_id = dish.id
    JOIN menu_page ON menu_item.menu_page_id = menu_page.id
    JOIN menu ON menu_page.menu_id = menu.id;
```

## データを検証する {#validate-data}

クエリ:

```sql
SELECT count() FROM menu_item_denorm;
```

結果:

```text
┌─count()─┐
│ 1329175 │
└─────────┘
```

## クエリを実行する {#run-queries}

### 料理の平均歴史価格 {#query-averaged-historical-prices}

クエリ:

```sql
SELECT
    round(toUInt32OrZero(extract(menu_date, '^\\d{4}')), -1) AS d,
    count(),
    round(avg(price), 2),
    bar(avg(price), 0, 100, 100)
FROM menu_item_denorm
WHERE (menu_currency = 'Dollars') AND (d > 0) AND (d < 2022)
GROUP BY d
ORDER BY d ASC;
```

結果:

```text
┌────d─┬─count()─┬─round(avg(price), 2)─┬─bar(avg(price), 0, 100, 100)─┐
│ 1850 │     618 │                  1.5 │ █▍                           │
│ 1860 │    1634 │                 1.29 │ █▎                           │
│ 1870 │    2215 │                 1.36 │ █▎                           │
│ 1880 │    3909 │                 1.01 │ █                            │
│ 1890 │    8837 │                  1.4 │ █▍                           │
│ 1900 │  176292 │                 0.68 │ ▋                            │
│ 1910 │  212196 │                 0.88 │ ▊                            │
│ 1920 │  179590 │                 0.74 │ ▋                            │
│ 1930 │   73707 │                  0.6 │ ▌                            │
│ 1940 │   58795 │                 0.57 │ ▌                            │
│ 1950 │   41407 │                 0.95 │ ▊                            │
│ 1960 │   51179 │                 1.32 │ █▎                           │
│ 1970 │   12914 │                 1.86 │ █▋                           │
│ 1980 │    7268 │                 4.35 │ ████▎                        │
│ 1990 │   11055 │                 6.03 │ ██████                       │
│ 2000 │    2467 │                11.85 │ ███████████▋                 │
│ 2010 │     597 │                25.66 │ █████████████████████████▋   │
└──────┴─────────┴──────────────────────┴──────────────────────────────┘
```

結果を慎重に解釈してください。

### バーガーの価格 {#query-burger-prices}

クエリ:

```sql
SELECT
    round(toUInt32OrZero(extract(menu_date, '^\\d{4}')), -1) AS d,
    count(),
    round(avg(price), 2),
    bar(avg(price), 0, 50, 100)
FROM menu_item_denorm
WHERE (menu_currency = 'Dollars') AND (d > 0) AND (d < 2022) AND (dish_name ILIKE '%burger%')
GROUP BY d
ORDER BY d ASC;
```

結果:

```text
┌────d─┬─count()─┬─round(avg(price), 2)─┬─bar(avg(price), 0, 50, 100)───────────┐
│ 1880 │       2 │                 0.42 │ ▋                                     │
│ 1890 │       7 │                 0.85 │ █▋                                    │
│ 1900 │     399 │                 0.49 │ ▊                                     │
│ 1910 │     589 │                 0.68 │ █▎                                    │
│ 1920 │     280 │                 0.56 │ █                                     │
│ 1930 │      74 │                 0.42 │ ▋                                     │
│ 1940 │     119 │                 0.59 │ █▏                                    │
│ 1950 │     134 │                 1.09 │ ██▏                                   │
│ 1960 │     272 │                 0.92 │ █▋                                    │
│ 1970 │     108 │                 1.18 │ ██▎                                   │
│ 1980 │      88 │                 2.82 │ █████▋                                │
│ 1990 │     184 │                 3.68 │ ███████▎                              │
│ 2000 │      21 │                 7.14 │ ██████████████▎                       │
│ 2010 │       6 │                18.42 │ ████████████████████████████████████▋ │
└──────┴─────────┴──────────────────────┴───────────────────────────────────────┘
```

### ウォッカ {#query-vodka}

クエリ:

```sql
SELECT
    round(toUInt32OrZero(extract(menu_date, '^\\d{4}')), -1) AS d,
    count(),
    round(avg(price), 2),
    bar(avg(price), 0, 50, 100)
FROM menu_item_denorm
WHERE (menu_currency IN ('Dollars', '')) AND (d > 0) AND (d < 2022) AND (dish_name ILIKE '%vodka%')
GROUP BY d
ORDER BY d ASC;
```

結果:

```text
┌────d─┬─count()─┬─round(avg(price), 2)─┬─bar(avg(price), 0, 50, 100)─┐
│ 1910 │       2 │                    0 │                             │
│ 1920 │       1 │                  0.3 │ ▌                           │
│ 1940 │      21 │                 0.42 │ ▋                           │
│ 1950 │      14 │                 0.59 │ █▏                          │
│ 1960 │     113 │                 2.17 │ ████▎                       │
│ 1970 │      37 │                 0.68 │ █▎                          │
│ 1980 │      19 │                 2.55 │ █████                       │
│ 1990 │      86 │                  3.6 │ ███████▏                    │
│ 2000 │       2 │                 3.98 │ ███████▊                    │
└──────┴─────────┴──────────────────────┴─────────────────────────────┘
```

ウォッカを手に入れるには`ILIKE '%vodka%'`と記述する必要があり、これは確かに主張しています。

### キャビア {#query-caviar}

キャビアの価格を表示しましょう。また、キャビアを含む任意の料理名を表示します。

クエリ:

```sql
SELECT
    round(toUInt32OrZero(extract(menu_date, '^\\d{4}')), -1) AS d,
    count(),
    round(avg(price), 2),
    bar(avg(price), 0, 50, 100),
    any(dish_name)
FROM menu_item_denorm
WHERE (menu_currency IN ('Dollars', '')) AND (d > 0) AND (d < 2022) AND (dish_name ILIKE '%caviar%')
GROUP BY d
ORDER BY d ASC;
```

結果:

```text
┌────d─┬─count()─┬─round(avg(price), 2)─┬─bar(avg(price), 0, 50, 100)──────┬─any(dish_name)──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 1090 │       1 │                    0 │                                  │ Caviar                                                                                                                              │
│ 1880 │       3 │                    0 │                                  │ Caviar                                                                                                                              │
│ 1890 │      39 │                 0.59 │ █▏                               │ Butter and caviar                                                                                                                   │
│ 1900 │    1014 │                 0.34 │ ▋                                │ Anchovy Caviar on Toast                                                                                                             │
│ 1910 │    1588 │                 1.35 │ ██▋                              │ 1/1 Brötchen Caviar                                                                                                                 │
│ 1920 │     927 │                 1.37 │ ██▋                              │ ASTRAKAN CAVIAR                                                                                                                     │
│ 1930 │     289 │                 1.91 │ ███▋                             │ Astrachan caviar                                                                                                                    │
│ 1940 │     201 │                 0.83 │ █▋                               │ (SPECIAL) Domestic Caviar Sandwich                                                                                                  │
│ 1950 │      81 │                 2.27 │ ████▌                            │ Beluga Caviar                                                                                                                       │
│ 1960 │     126 │                 2.21 │ ████▍                            │ Beluga Caviar                                                                                                                       │
│ 1970 │     105 │                 0.95 │ █▊                               │ BELUGA MALOSSOL CAVIAR AMERICAN DRESSING                                                                                            │
│ 1980 │      12 │                 7.22 │ ██████████████▍                  │ Authentic Iranian Beluga Caviar the world's finest black caviar presented in ice garni and a sampling of chilled 100° Russian vodka │
│ 1990 │      74 │                14.42 │ ████████████████████████████▋    │ Avocado Salad, Fresh cut avocado with caviare                                                                                       │
│ 2000 │       3 │                 7.82 │ ███████████████▋                 │ Aufgeschlagenes Kartoffelsueppchen mit Forellencaviar                                                                               │
│ 2010 │       6 │                15.58 │ ███████████████████████████████▏ │ "OYSTERS AND PEARLS" "Sabayon" of Pearl Tapioca with Island Creek Oysters and Russian Sevruga Caviar                                │
└──────┴─────────┴──────────────────────┴──────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

少なくともウォッカと一緒にキャビアがあります。とても素晴らしいです。

## オンラインプレイグラウンド {#playground}

このデータはClickHouseプレイグラウンドにアップロードされています。[例](https://sql.clickhouse.com?query_id=KB5KQJJFNBKHE5GBUJCP1B)。
