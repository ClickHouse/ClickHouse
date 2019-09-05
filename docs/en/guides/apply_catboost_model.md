# Applying a Catboost Model in ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) — is a free and open-source gradient boosting library developed at [Yandex](https://yandex.com/company/) for machine learning.

With this instruction, you will learn to apply pre-trained models in ClickHouse: as a result, you run the model inference from SQL.

To apply a CatBoost model in ClickHouse:

1. [Create a Table](#create-table).
2. [Insert the Data to the Table](#insert-the-data-to-the-table).
3. [Configure the Model](#configure-the-model).
4. [Run the Model Inference from SQL](#run-the-model-inference).

For more information about training CatBoost models, see [Training and applying models](https://catboost.ai/docs/features/training.html#training).

## Prerequisites {#prerequisites}

If you don't have the [Docker](https://docs.docker.com/install/) yet, install it.

!!! note "Note"
    [Docker](https://www.docker.com) is a software platform that allows you to create containers that isolate a CatBoost and ClickHouse installation from the rest of the system.

Before applying a CatBoost model:

**1.** Pull the [Docker image](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) from the registry:

```bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

This Docker image contains everything you need to run CatBoost and ClickHouse: code, runtime, libraries, environment variables, and configuration files.

**2.** Make sure the Docker image has been successfully pulled:

```bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              3e5ad9fae997        19 months ago       1.58GB
```

**3.** Start a Docker container based on this image:

```bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

**4.** Remove old ClickHouse versions:

```bash
$ sudo apt-get purge clickhouse-server-base
$ sudo apt-get purge clickhouse-server-common
$ sudo apt-get autoremove
```

**5.** Проверьте успешность удаления:

```bash
dpkg -l | grep clickhouse-server
```

**6.** Install packages:

```bash
$ sudo apt-get install dirmngr
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

$ echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
$ sudo apt-get update

$ sudo apt-get install -y clickhouse-server clickhouse-client

$ sudo service clickhouse-server start
$ clickhouse-client
```

For more information, see [Quick Start](https://clickhouse.yandex/#quick-start).

## 1. Create a Table {#create-table}

To create a ClickHouse table for the train sample:

**1.** Start a ClickHouse client:

```bash
$ clickhouse client
```

!!! note "Note"
    The ClickHouse server is already running inside the Docker container.

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

## 2. Insert the Data to the Table {#insert-the-data-to-the-table}

To insert the data:

**1.** Exit from ClickHouse console client:

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

## 3. Configure the Model to Work with the Trained Model {#configure-the-model}

This step is optional: the Docker container contains all configuration files. 

Create a config file in the `models` folder (for example, `models/config_model.xml`) with the model configuration:

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

!!! note "Note"
    To show contents of the config file in the Docker container, run `cat models/amazon_model.xml`.

The ClickHouse config file should already have this setting:

```xml
// ../../etc/clickhouse-server/config.xml
<models_config>/home/catboost/models/*_model.xml</models_config>
```

To check it, run `tail ../../etc/clickhouse-server/config.xml`.

## 4. Run the Model Inference from SQL {#run-the-model-inference}

For test run the ClickHouse client `$ clickhouse client`.

Let's make sure that the model is working:

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

!!! note "Note"
    Function [modelEvaluate](../query_language/functions/other_functions.md#function-modelevaluate) returns tuple with per-class raw predictions for multiclass models.

Let's predict probability:

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

!!! note "Note"
    More info about [exp()](../query_language/functions/math_functions.md) function.

Let's calculate LogLoss on the sample:

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

!!! note "Note"
    More info about [avg()](../query_language/agg_functions/reference.md#agg_function-avg) and [log()](../query_language/functions/math_functions.md) functions.