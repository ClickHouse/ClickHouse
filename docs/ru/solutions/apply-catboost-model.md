# Применение модели CatBoost в ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) — открытая программная библиотека для машинного обучения, использующая схему градиентного бустинга.

Чтобы применить модель CatBoost в ClickHouse:

1. [Создайте таблицу для обучающей выборки](#create-a-table).
1. [Вставьте данные в таблицу](#insert-the-data-to-the-table).
1. [Настройте конфигурацию модели](#configure-the-model).
1. [Протестируйте обученную модель](#test-the-trained-model).

## Подготовка к работе {#before-you-start}

Если у вас еще нет [Docker](https://docs.docker.com/install/), установите его.

> **Примечание.** [Docker](https://www.docker.com) использует контейнеры для создания виртуальных сред, которые изолируют установку CatBoost и ClickHouse от остальной части системы. Программы CatBoost и ClickHouse выполняются в этой виртуальной среде.

Перед применением модели CatBoost в ClickHouse:

**1.** Скачайте [Docker-образ](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) из реестра:

```bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

Данный Docker-образ содержит все необходимое для запуска приложения: код, среду выполнения, библиотеки, переменные окружения и файлы конфигурации.

**2.** Проверьте, что Docker-образ действительно скачался:

```bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              3e5ad9fae997        19 months ago       1.58GB
```

**3.** Запустите Docker-образ:

```bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

> **Примечание.** После запуска по адресу [http://localhost:8888](http://localhost:8888) будет доступен Jupyter Notebook с материалами данной инструкции.

## 1. Создайте таблицу {#create-a-table}

Чтобы создать таблицу в ClickHouse для обучающей выборки:

**1.** Запустите ClickHouse-клиент:

```bash
$ clickhouse client
```

> **Примечание.** ClickHouse-сервер уже запущен внутри Docker-контейнера.

**2.** Создайте таблицу в ClickHouse с помощью следующей команды:

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

## 2. Вставьте данные в таблицу {#insert-the-data-to-the-table}

Чтобы вставить данные:

**1.** Выйдите из клиента ClickHouse:

```sql
:) exit
```

**2.** Загрузите данные:

```bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**3.** Проверьте, что данные действительно загрузились:

```sql
$ clickhouse client
:) SELECT count() FROM amazon_train
SELECT count()
FROM amazon_train

+-count()-+
|   32769 |
+---------+
```

## 3. Настройте конфигурацию модели {#configure-the-model}

Опциональный шаг: Docker-контейнер содержит все необходимые файлы конфигурации.

**1.** Создайте файл с конфигурацией модели (например, `config_model.xml`):

```xml
<models>
    <model>
        <!-- Тип модели. В настоящий момент ClickHouse предоставляет только модель catboost. -->
        <type>catboost</type>
        <!-- Имя модели. -->
        <name>amazon</name>
        <!-- Путь к обученной модели. -->
        <path>/home/catboost/tutorial/catboost_model.bin</path>
        <!-- Интервал обновления. -->
        <lifetime>0</lifetime>
    </model>
</models>
```

> **Примечание.** Чтобы посмотреть конфигурационный файл в Docker-контейнере, выполните команду `cat models/amazon_model.xml`.

**2.** Добавьте следующие строки в файл `/etc/clickhouse-server/config.xml`:

```xml
<catboost_dynamic_library_path>/home/catboost/.data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```
 
> **Примечание.** Чтобы посмотреть конфигурационный файл ClickHouse в Docker-контейнере, выполните команду `cat ../../etc/clickhouse-server/config.xml`.

**3.** Перезапустите ClickHouse-сервер:

```bash
$ sudo service clickhouse-server restart
```

## 4. Протестируйте обученную модель {#test-the-trained-model}

Для тестирования запустите ClickHouse-клиент `$ clickhouse client`.

- Проверьте, что модель работает:

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

> **Примечание.** Функция `modelEvaluate` возвращает кортежи (tuple) с исходными прогнозами по классам для моделей с несколькими классами.

- Спрогнозируйте вероятность:

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

-  Посчитайте логистическую функцию потерь (LogLoss) на всей выборке:

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

