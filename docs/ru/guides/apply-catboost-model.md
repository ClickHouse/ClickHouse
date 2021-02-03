---
toc_priority: 41
toc_title: "\u041f\u0440\u0438\u043c\u0435\u043d\u0435\u043d\u0438\u0435\u0020\u043c\u043e\u0434\u0435\u043b\u0438\u0020\u0043\u0061\u0074\u0042\u006f\u006f\u0073\u0074\u0020\u0432\u0020\u0043\u006c\u0069\u0063\u006b\u0048\u006f\u0075\u0073\u0065"
---

# Применение модели CatBoost в ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) — открытая программная библиотека разработанная компанией [Яндекс](https://yandex.ru/company/) для машинного обучения, которая использует схему градиентного бустинга.

С помощью этой инструкции вы научитесь применять предобученные модели в ClickHouse: в результате вы запустите вывод модели из SQL.

Чтобы применить модель CatBoost в ClickHouse:

1.  [Создайте таблицу](#create-table).
2.  [Вставьте данные в таблицу](#insert-data-to-table).
3.  [Интегрируйте CatBoost в ClickHouse](#integrate-catboost-into-clickhouse) (Опциональный шаг).
4.  [Запустите вывод модели из SQL](#run-model-inference).

Подробнее об обучении моделей в CatBoost, см. [Обучение и применение моделей](https://catboost.ai/docs/features/training.html#training).

## Перед началом работы {#prerequisites}

Если у вас еще нет [Docker](https://docs.docker.com/install/), установите его.

!!! note "Примечание"
    [Docker](https://www.docker.com) – это программная платформа для создания контейнеров, которые изолируют установку CatBoost и ClickHouse от остальной части системы.

Перед применением модели CatBoost:

**1.** Скачайте [Docker-образ](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) из реестра:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

Данный Docker-образ содержит все необходимое для запуска CatBoost и ClickHouse: код, среду выполнения, библиотеки, переменные окружения и файлы конфигурации.

**2.** Проверьте, что Docker-образ успешно скачался:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** Запустите Docker-контейнер основанный на данном образе:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. Создайте таблицу {#create-table}

Чтобы создать таблицу для обучающей выборки:

**1.** Запустите клиент ClickHouse:

``` bash
$ clickhouse client
```

!!! note "Примечание"
    Сервер ClickHouse уже запущен внутри Docker-контейнера.

**2.** Создайте таблицу в ClickHouse с помощью следующей команды:

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

**3.** Выйдите из клиента ClickHouse:

``` sql
:) exit
```

## 2. Вставьте данные в таблицу {#insert-data-to-table}

Чтобы вставить данные:

**1.** Выполните следующую команду:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** Запустите клиент ClickHouse:

``` bash
$ clickhouse client
```

**3.** Проверьте, что данные успешно загрузились:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+---------+
```

## 3. Интегрируйте CatBoost в ClickHouse {#integrate-catboost-into-clickhouse}

!!! note "Примечание"
    **Опциональный шаг.** Docker-образ содержит все необходимое для запуска CatBoost и ClickHouse.

Чтобы интегрировать CatBoost в ClickHouse:

**1.** Создайте библиотеку для оценки модели.

Наиболее быстрый способ оценить модель CatBoost — это скомпилировать библиотеку `libcatboostmodel.<so|dll|dylib>`. Подробнее о том, как скомпилировать библиотеку, читайте в [документации CatBoost](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** Создайте в любом месте новую директорию с произвольным названием, например `data` и поместите в нее созданную библиотеку. Docker-образ уже содержит библиотеку `data/libcatboostmodel.so`.

**3.** Создайте в любом месте новую директорию для конфигурации модели с произвольным названием, например `models`.

**4.** Создайте файл конфигурации модели с произвольным названием, например `models/amazon_model.xml`.

**5.** Опишите конфигурацию модели:

``` xml
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

**6.** Добавьте в конфигурацию ClickHouse путь к CatBoost и конфигурации модели:

``` xml
<!-- Файл etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. Запустите вывод модели из SQL {#run-model-inference}

Для тестирования модели запустите клиент ClickHouse `$ clickhouse client`.

Проверьте, что модель работает:

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

!!! note "Примечание"
    Функция [modelEvaluate](../sql-reference/functions/other-functions.md#function-modelevaluate) возвращает кортежи (tuple) с исходными прогнозами по классам для моделей с несколькими классами.

Спрогнозируйте вероятность:

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

!!! note "Примечание"
    Подробнее про функцию [exp()](../sql-reference/functions/math-functions.md).

Посчитайте логистическую функцию потерь (LogLoss) на всей выборке:

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

!!! note "Примечание"
    Подробнее про функции [avg()](../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg), [log()](../sql-reference/functions/math-functions.md).
