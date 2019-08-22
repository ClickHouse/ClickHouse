# Applying a CatBoost model in ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) — is a free and open-source gradient boosting library for machine learning.

To apply a CatBoost model in ClickHouse:

1. [Create a table](#create-table).
2. [Insert the data to the table](#insert-the-data-to-the-table).
3. [Configure the model](#configure-the-model).
4. [Run the model inference from SQL](#run-the-model-inference).

For more information about training CatBoost models, see [Training and applying models](https://catboost.ai/docs/features/training.html#training).

## Before you start {#before-you-start}

If you don't have the [Docker](https://docs.docker.com/install/) yet, install it.

> **Note:** [Docker](https://www.docker.com) uses containers to create virtual environments that isolate a CatBoost and ClickHouse installation from the rest of the system. CatBoost and ClickHouse programs are run within this virtual environment.

Before applying a CatBoost model:

**1.** Pull the [Docker image](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) from the registry:

```bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

This Docker image contains everything you need to run an application: code, runtime, libraries, environment variables, and configuration files.

**3.** Make sure the Docker image has been pulled:

```bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              3e5ad9fae997        19 months ago       1.58GB
```

**2.** Start the Docker-configured image:

```bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

> **Note:** Example running a Jupyter Notebook with this manual materials to [http://localhost:8888](http://localhost:8888).

## 1. Create a table {#create-table}

To create a ClickHouse table for the train sample:

**1.** Start a ClickHouse client:

```bash
$ clickhouse client
```

> **Note:** The ClickHouse server is already running inside the Docker container.

**2.** Create the table using the command:

```sql
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
ENGINE = MergeTree(date, date, 8192)
```

## 2. Insert the data to the table {#insert-the-data-to-the-table}

To insert the data:

**1.** Exit from ClickHouse:

```sql
:) exit
```

**2.** Upload the data:

```bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**3.** Make sure the data has been uploaded:

```sql
$ clickhouse client
:) SELECT count() FROM amazon_train
SELECT count()
FROM amazon_train

+-count()-+
|   32769 |
+---------+
```

## 3. Configure the model to work with the trained model {#configure-the-model}

This step is optional: the Docker container contains all configuration files. 

Create a config file (for example, `config_model.xml`) with the model configuration:

```xml
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

> **Note:** To show contents of the config file in the Docker container, run `cat models/amazon_model.xml`.

The ClickHouse config file should already have this setting:

```xml
<models_config>/home/catboost/models/*_model.xml</models_config>
```

To check it, run `tail ../../etc/clickhouse-server/config.xml`.

## 4. Run the model inference from SQL {#run-the-model-inference}

For test run the ClickHouse client `$ clickhouse client`.

- Let's make sure that the model is working:

```sql
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

> **Note:** Function [modelEvaluate](../query_language/functions/other_functions.md#function-modelevaluate) returns tuple with per-class raw predictions for multiclass models.

- Let's predict probability:

```sql
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

-  Let's calculate LogLoss on the sample:

```sql
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