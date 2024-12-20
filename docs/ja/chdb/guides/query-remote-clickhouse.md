---
title: リモートClickHouseサーバーへのクエリ方法
sidebar_label: リモートClickHouseへのクエリ
slug: /ja/chdb/guides/query-remote-clickhouse
description: このガイドでは、chDBからリモートClickHouseサーバーにクエリを実行する方法を学習します。
keywords: [chdb, clickhouse]
---

このガイドでは、chDBからリモートClickHouseサーバーにクエリを実行する方法を学習します。

## セットアップ

まず仮想環境を作成しましょう：

```bash
python -m venv .venv
source .venv/bin/activate
```

次にchDBをインストールします。
バージョン2.0.2以上であることを確認してください：

```bash
pip install "chdb>=2.0.2"
```

次にpandasとipythonをインストールします：

```bash
pip install pandas ipython
```

このガイドの残りの部分でコマンドを実行するために`ipython`を使用します。以下のコマンドで起動できます：

```bash
ipython
```

また、このコードをPythonスクリプトやお好みのノートブックで使用することもできます。

## ClickPyの紹介

これからクエリを実行するリモートClickHouseサーバーは[ClickPy](https://clickpy.clickhouse.com)です。
ClickPyはPyPiパッケージのダウンロード数を追跡し、UIを通じてパッケージの統計情報を探ることができます。基礎となるデータベースは`play`ユーザーを使用してクエリを実行できます。

ClickPyの詳細は[そのGitHubリポジトリ](https://github.com/ClickHouse/clickpy)で確認できます。

## ClickPy ClickHouseサービスへのクエリ

chDBをインポートしましょう：

```python
import chdb
```

`remoteSecure`関数を使用してClickPyにクエリを実行します。この関数には少なくともホスト名、テーブル名、ユーザー名が必要です。

[`openai`パッケージ](https://clickpy.clickhouse.com/dashboard/openai)の1日ごとのダウンロード数をPandas DataFrameとして取得するためのクエリは以下の通りです：
 
```python
query = """
SELECT
    toStartOfDay(date)::Date32 AS x,
    sum(count) AS y
FROM remoteSecure(
  'clickpy-clickhouse.clickhouse.com', 
  'pypi.pypi_downloads_per_day', 
  'play'
)
WHERE project = 'openai'
GROUP BY x
ORDER BY x ASC
"""

openai_df = chdb.query(query, "DataFrame")
openai_df.sort_values(by=["x"], ascending=False).head(n=10)
```

```text
               x        y
2392  2024-10-02  1793502
2391  2024-10-01  1924901
2390  2024-09-30  1749045
2389  2024-09-29  1177131
2388  2024-09-28  1157323
2387  2024-09-27  1688094
2386  2024-09-26  1862712
2385  2024-09-25  2032923
2384  2024-09-24  1901965
2383  2024-09-23  1777554
```

次に[`scikit-learn`](https://clickpy.clickhouse.com/dashboard/scikit-learn)のダウンロード数を取得するために同じことを行ってみましょう：

```python
query = """
SELECT
    toStartOfDay(date)::Date32 AS x,
    sum(count) AS y
FROM remoteSecure(
  'clickpy-clickhouse.clickhouse.com', 
  'pypi.pypi_downloads_per_day', 
  'play'
)
WHERE project = 'scikit-learn'
GROUP BY x
ORDER BY x ASC
"""

sklearn_df = chdb.query(query, "DataFrame")
sklearn_df.sort_values(by=["x"], ascending=False).head(n=10)
```

```text
               x        y
2392  2024-10-02  1793502
2391  2024-10-01  1924901
2390  2024-09-30  1749045
2389  2024-09-29  1177131
2388  2024-09-28  1157323
2387  2024-09-27  1688094
2386  2024-09-26  1862712
2385  2024-09-25  2032923
2384  2024-09-24  1901965
2383  2024-09-23  1777554
```

## Pandas DataFrameのマージ

現在、2つのDataFrameを持っています。これを日付（`x`カラム）を基にしてマージすることができます：

```python
df = openai_df.merge(
  sklearn_df, 
  on="x", 
  suffixes=("_openai", "_sklearn")
)
df.head(n=5)
```

```text
            x  y_openai  y_sklearn
0  2018-02-26        83      33971
1  2018-02-27        31      25211
2  2018-02-28         8      26023
3  2018-03-01         8      20912
4  2018-03-02         5      23842
```

次に、このようにしてOpenAIのダウンロード数とscikit-learnのダウンロード数の比率を計算することができます：

```python
df['ratio'] = df['y_openai'] / df['y_sklearn']
df.head(n=5)
```

```text
            x  y_openai  y_sklearn     ratio
0  2018-02-26        83      33971  0.002443
1  2018-02-27        31      25211  0.001230
2  2018-02-28         8      26023  0.000307
3  2018-03-01         8      20912  0.000383
4  2018-03-02         5      23842  0.000210
```

## Pandas DataFrameのクエリ

次に、最も良い比率と最悪の比率の日付を探したいとします。
chDBに戻ってこれらの値を計算します：

```python
chdb.query("""
SELECT max(ratio) AS bestRatio,
       argMax(x, ratio) AS bestDate,
       min(ratio) AS worstRatio,
       argMin(x, ratio) AS worstDate
FROM Python(df)
""", "DataFrame")
```

```text
   bestRatio    bestDate  worstRatio   worstDate
0   0.693855  2024-09-19    0.000003  2020-02-09
```

Pandas DataFrameへのクエリについてさらに学びたい場合は、[Pandas DataFrames 開発者ガイド](querying-pandas.md)をご覧ください。
