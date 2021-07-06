---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u5E94\u7528CatBoost\u6A21\u578B"
---

# 在ClickHouse中应用Catboost模型 {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) 是一个由[Yandex](https://yandex.com/company/)开发的开源免费机器学习库。


通过这篇指导，您将学会如何用SQL建模，使用ClickHouse预先训练好的模型来推断数据。


在ClickHouse中应用CatBoost模型的一般过程:

1.  [创建数据表](#create-table).
2.  [将数据插入到表中](#insert-data-to-table).
3.  [将CatBoost集成到ClickHouse中](#integrate-catboost-into-clickhouse) （可跳过）。
4.  [从SQL运行模型推断](#run-model-inference).

有关训练CatBoost模型的详细信息，请参阅 [训练和模型应用](https://catboost.ai/docs/features/training.html#training).

## 先决条件 {#prerequisites}

请先安装 [Docker](https://docs.docker.com/install/)。

!!! note "注"
    [Docker](https://www.docker.com) 是一个软件平台，用户可以用来创建独立于其余系统、集成CatBoost和ClickHouse的容器。

在应用CatBoost模型之前:

**1.** 从容器仓库拉取docker映像 (https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) :

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

此Docker映像包含运行CatBoost和ClickHouse所需的所有内容：代码、运行环境、库、环境变量和配置文件。

**2.** 确保已成功拉取Docker映像:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** 基于此映像启动一个Docker容器:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. 创建数据表 {#create-table}

为训练样本创建ClickHouse表:

**1.** 在交互模式下启动ClickHouse控制台客户端:

``` bash
$ clickhouse client
```

!!! note "注"
    ClickHouse服务器已经在Docker容器内运行。

**2.** 使用以下命令创建表:

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

**3.** 从ClickHouse控制台客户端退出:

``` sql
:) exit
```

## 2. 将数据插入到表中 {#insert-data-to-table}

插入数据:

**1.** 运行以下命令:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** 在交互模式下启动ClickHouse控制台客户端:

``` bash
$ clickhouse client
```

**3.** 确保数据已上传:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. 将CatBoost集成到ClickHouse中 {#integrate-catboost-into-clickhouse}

!!! note "注"
    **可跳过。** Docker映像包含运行CatBoost和ClickHouse所需的所有内容。

CatBoost集成到ClickHouse步骤:

**1.** 构建评估库。

评估CatBoost模型的最快方法是编译 `libcatboostmodel.<so|dll|dylib>` 库文件. 

有关如何构建库文件的详细信息，请参阅 [CatBoost文件](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** 创建一个新目录（位置与名称可随意指定）, 如 `data` 并将创建的库文件放入其中。 Docker映像已经包含了库 `data/libcatboostmodel.so`.

**3.** 创建一个新目录来放配置模型, 如 `models`.

**4.** 创建一个模型配置文件，如 `models/amazon_model.xml`.

**5.** 描述模型配置:

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

**6.** 将CatBoost库文件的路径和模型配置添加到ClickHouse配置:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. 运行从SQL推断的模型 {#run-model-inference}

测试模型是否正常，运行ClickHouse客户端 `$ clickhouse client`.

让我们确保模型能正常工作:

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
    函数 [modelEvaluate](../sql-reference/functions/other-functions.md#function-modelevaluate) 返回带有多类模型的每类原始预测的元组。

执行预测:

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
    查看函数说明 [exp()](../sql-reference/functions/math-functions.md) 。

让我们计算样本的LogLoss:

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
    查看函数说明 [avg()](../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg) 和 [log()](../sql-reference/functions/math-functions.md) 。

[原始文章](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
