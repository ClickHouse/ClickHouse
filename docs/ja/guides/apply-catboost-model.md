---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "CatBoost\u30E2\u30C7\u30EB\u306E\u9069\u7528"
---

# ClickHouseでのCatboostモデルの適用 {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) で開発された無料でオープンソースの勾配昇圧ライブラリです [Yandex](https://yandex.com/company/) 機械学習のために。

この手順では、SQLからモデル推論を実行して、ClickHouseで事前に訓練されたモデルを適用する方法を学習します。

ClickHouseでCatBoostモデルを適用するには:

1.  [テーブルの作成](#create-table).
2.  [テーブルにデータを挿入します](#insert-data-to-table).
3.  [ClickHouseにCatBoostを統合する](#integrate-catboost-into-clickhouse) （任意ステップ）。
4.  [SQLからモデル推論を実行する](#run-model-inference).

CatBoostモデルのトレーニングの詳細については、 [モデルの学習と適用](https://catboost.ai/docs/features/training.html#training).

## 前提条件 {#prerequisites}

あなたが持っていない場合 [ドッカー](https://docs.docker.com/install/) まだ、それを取付けなさい。

!!! note "注"
    [ドッカー](https://www.docker.com) CatBoostとClickHouseのインストールをシステムの残りの部分から分離するコンテナを作成できるソフトウェアプラットフォームです。

CatBoostモデルを適用する前に:

**1.** プル [ドッカー画像](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) レジストリから:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

このDockerイメージには、CatBoostとClickHouseを実行するために必要なコード、ランタイム、ライブラリ、環境変数、設定ファイルがすべて含まれています。

**2.** Dockerイメージが正常にプルされたことを確認します:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** 起Dockerコンテナに基づくこのイメージ:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. テーブルの作成 {#create-table}

トレーニングサンプルのClickHouseテーブルを作成するには:

**1.** 対話モードでClickHouse consoleクライアントを起動する:

``` bash
$ clickhouse client
```

!!! note "注"
    ClickHouseサーバーはDockerコンテナ内で既に実行されています。

**2.** コマンドを使用して表を作成します:

``` sql
:) CREATE TABLE amazon_train
(
    date Date MATERIALIZED today(),
    ACTION UInt8,
    RESOURCE UInt32,
    MGR_ID UInt32,
    ROLE_ROLLUP_1 UInt32,
    ROLE_ROLLUP_2 UInt32,
    ROLE_DEPTNAME UInt32,
    ROLE_TITLE UInt32,
    ROLE_FAMILY_DESC UInt32,
    ROLE_FAMILY UInt32,
    ROLE_CODE UInt32
)
ENGINE = MergeTree ORDER BY date
```

**3.** ClickHouse consoleクライアントからの終了:

``` sql
:) exit
```

## 2. テーブルにデータを挿入します {#insert-data-to-table}

データを挿入するには:

**1.** 次のコマンドを実行します:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** 対話モードでClickHouse consoleクライアントを起動する:

``` bash
$ clickhouse client
```

**3.** データがアップロードされたことを確認:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. ClickHouseにCatBoostを統合する {#integrate-catboost-into-clickhouse}

!!! note "注"
    **任意ステップ。** Dockerイメージには、CatBoostとClickHouseを実行するために必要なすべてが含まれています。

ClickhouseにCatBoostを統合するには:

**1.** 評価ライブラリを構築します。

CatBoostモデルを評価する最速の方法はコンパイルです `libcatboostmodel.<so|dll|dylib>` 図書館 に関する詳細については、図書館を参照 [CatBoostドキュメント](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** 新しいディレクトリを任意の場所に作成し、任意の名前で作成します。, `data` 作成したライブラリをその中に入れます。 のDocker画像がすでに含まれている図書館 `data/libcatboostmodel.so`.

**3.** Config modelの新しいディレクトリを任意の場所に、任意の名前で作成します。, `models`.

**4.** 任意の名前のモデル構成ファイルを作成します。, `models/amazon_model.xml`.

**5.** モデル構成の説明:

``` xml
<models>
    <model>
        <!-- Model type. Now catboost only. -->
        <type>catboost</type>
        <!-- Model name. -->
        <name>amazon</name>
        <!-- Path to trained model. -->
        <path>/home/catboost/tutorial/catboost_model.bin</path>
        <!-- Update interval. -->
        <lifetime>0</lifetime>
    </model>
</models>
```

**6.** CatBoostへのパスとモデル設定をClickHouse設定に追加します:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. SQLからモデル推論を実行する {#run-model-inference}

試験モデルのClickHouseト `$ clickhouse client`.

モデルが動作していることを確認しましょう:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) > 0 AS prediction,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "注"
    関数 [モデル評価](../sql-reference/functions/other-functions.md#function-modelevaluate) マルチクラスモデルのクラスごとの生の予測を持つタプルを返します。

のは、確率を予測してみましょう:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) AS prediction,
    1. / (1 + exp(-prediction)) AS probability,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "注"
    詳細について [exp()](../sql-reference/functions/math-functions.md) 機能。

サンプルのLogLossを計算しましょう:

``` sql
:) SELECT -avg(tg * log(prob) + (1 - tg) * log(1 - prob)) AS logloss
FROM
(
    SELECT
        modelEvaluate('amazon',
                    RESOURCE,
                    MGR_ID,
                    ROLE_ROLLUP_1,
                    ROLE_ROLLUP_2,
                    ROLE_DEPTNAME,
                    ROLE_TITLE,
                    ROLE_FAMILY_DESC,
                    ROLE_FAMILY,
                    ROLE_CODE) AS prediction,
        1. / (1. + exp(-prediction)) AS prob,
        ACTION AS tg
    FROM amazon_train
)
```

!!! note "注"
    詳細について [avg()](../sql-reference/aggregate-functions/reference.md#agg_function-avg) と [ログ()](../sql-reference/functions/math-functions.md) 機能。

[元の記事](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
