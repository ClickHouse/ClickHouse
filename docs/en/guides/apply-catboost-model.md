---
toc_priority: 41
toc_title: Applying CatBoost Models
---

# Applying a Catboost Model in ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) is a free and open-source gradient boosting library developed at [Yandex](https://yandex.com/company/) for machine learning.

With this instruction, you will learn to apply pre-trained models in ClickHouse by running model inference from SQL.

To apply a CatBoost model in ClickHouse:

1.  [Create a Table](#create-table).
2.  [Insert the Data to the Table](#insert-data-to-table).
3.  [Integrate CatBoost into ClickHouse](#integrate-catboost-into-clickhouse) (Optional step).
4.  [Run the Model Inference from SQL](#run-model-inference).

For more information about training CatBoost models, see [Training and applying models](https://catboost.ai/docs/features/training.html#training).

## Prerequisites {#prerequisites}

If you don’t have the [Docker](https://docs.docker.com/install/) yet, install it.

!!! note "Note"
    [Docker](https://www.docker.com) is a software platform that allows you to create containers that isolate a CatBoost and ClickHouse installation from the rest of the system.

Before applying a CatBoost model:

**1.** Pull the [Docker image](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) from the registry:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

This Docker image contains everything you need to run CatBoost and ClickHouse: code, runtime, libraries, environment variables, and configuration files.

**2.** Make sure the Docker image has been successfully pulled:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** Start a Docker container based on this image:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. Create a Table {#create-table}

To create a ClickHouse table for the training sample:

**1.** Start ClickHouse console client in the interactive mode:

``` bash
$ clickhouse client
```

!!! note "Note"
    The ClickHouse server is already running inside the Docker container.

**2.** Create the table using the command:

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

**3.** Exit from ClickHouse console client:

``` sql
:) exit
```

## 2. Insert the Data to the Table {#insert-data-to-table}

To insert the data:

**1.** Run the following command:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** Start ClickHouse console client in the interactive mode:

``` bash
$ clickhouse client
```

**3.** Make sure the data has been uploaded:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. Integrate CatBoost into ClickHouse {#integrate-catboost-into-clickhouse}

!!! note "Note"
    **Optional step.** The Docker image contains everything you need to run CatBoost and ClickHouse.

To integrate CatBoost into ClickHouse:

**1.** Build the evaluation library.

The fastest way to evaluate a CatBoost model is compile `libcatboostmodel.<so|dll|dylib>` library. For more information about how to build the library, see [CatBoost documentation](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** Create a new directory anywhere and with any name, for example, `data` and put the created library in it. The Docker image already contains the library `data/libcatboostmodel.so`.

**3.** Create a new directory for config model anywhere and with any name, for example, `models`.

**4.** Create a model configuration file with any name, for example, `models/amazon_model.xml`.

**5.** Describe the model configuration:

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

**6.** Add the path to CatBoost and the model configuration to the ClickHouse configuration:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. Run the Model Inference from SQL {#run-model-inference}

For test model run the ClickHouse client `$ clickhouse client`.

Let’s make sure that the model is working:

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

!!! note "Note"
    Function [modelEvaluate](../sql-reference/functions/other-functions.md#function-modelevaluate) returns tuple with per-class raw predictions for multiclass models.

Let’s predict the probability:

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

!!! note "Note"
    More info about [exp()](../sql-reference/functions/math-functions.md) function.

Let’s calculate LogLoss on the sample:

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

!!! note "Note"
    More info about [avg()](../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg) and [log()](../sql-reference/functions/math-functions.md) functions.

[Original article](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
