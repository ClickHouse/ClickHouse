---
toc_priority: 28
toc_title: "\u0412\u0438\u0437\u0443\u0430\u043b\u044c\u043d\u044b\u0435\u0020\u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u044b\u0020\u043e\u0442\u0020\u0441\u0442\u043e\u0440\u043e\u043d\u043d\u0438\u0445\u0020\u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a\u043e\u0432"
---


# Визуальные интерфейсы от сторонних разработчиков {#vizualnye-interfeisy-ot-storonnikh-razrabotchikov}

## С открытым исходным кодом {#s-otkrytym-iskhodnym-kodom}

### Tabix {#tabix}

Веб-интерфейс для ClickHouse в проекте [Tabix](https://github.com/tabixio/tabix).

Основные возможности:

-   Работает с ClickHouse напрямую из браузера, без необходимости установки дополнительного ПО;
-   Редактор запросов с подсветкой синтаксиса;
-   Автодополнение команд;
-   Инструменты графического анализа выполнения запросов;
-   Цветовые схемы на выбор.

[Документация Tabix](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) — UI/IDE для OSX, Linux и Windows.

Основные возможности:

-   Построение запросов с подсветкой синтаксиса;
-   Просмотр ответа в табличном или JSON представлении;
-   Экспортирование результатов запроса в формате CSV или JSON;
-   Список процессов с описанием;
-   Режим записи;
-   Возможность остановки (`KILL`) запроса;
-   Граф базы данных. Показывает все таблицы и их столбцы с дополнительной информацией;
-   Быстрый просмотр размера столбца;
-   Конфигурирование сервера.

Планируется разработка следующих возможностей:

-   Управление базами;
-   Управление пользователями;
-   Анализ данных в режиме реального времени;
-   Мониторинг кластера;
-   Управление кластером;
-   Мониторинг реплицированных и Kafka таблиц.

### LightHouse {#lighthouse}

[LightHouse](https://github.com/VKCOM/lighthouse) — это легковесный веб-интерфейс для ClickHouse.

Основные возможности:

-   Список таблиц с фильтрацией и метаданными;
-   Предварительный просмотр таблицы с фильтрацией и сортировкой;
-   Выполнение запросов только для чтения.

### Redash {#redash}

[Redash](https://github.com/getredash/redash) — платформа для отображения данных.

Поддерживает множество источников данных, включая ClickHouse. Redash может объединять результаты запросов из разных источников в финальный набор данных.

Основные возможности:

-   Мощный редактор запросов.
-   Проводник по базе данных.
-   Инструменты визуализации, позволяющие представить данные в различных формах.

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - универсальный desktop клиент баз данных с поддержкой ClickHouse.

Основные возможности:

-   Построение запросов с подсветкой синтаксиса;
-   Просмотр таблиц;
-   Автодополнение команд;
-   Полнотекстовый поиск.

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) - это альтернативный клиент командной строки для ClickHouse, написанный на Python 3.

Основные возможности:

-   Автодополнение;
-   Подсветка синтаксиса для запросов и вывода данных;
-   Поддержка постраничного просмотра для результирующих данных;
-   Дополнительные PostgreSQL-подобные команды.

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) — специализированный инструмент для визуализации `system.trace_log` в виде [flamegraph](http://www.brendangregg.com/flamegraphs.html).

### clickhouse-plantuml {#clickhouse-plantuml}

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) — скрипт, генерирующий [PlantUML](https://plantuml.com/) диаграммы схем таблиц.

### xeus-clickhouse {#xeus-clickhouse}

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) — это ядро Jupyter для ClickHouse, которое поддерживает запрос ClickHouse-данных с использованием SQL в Jupyter.

## Коммерческие {#kommercheskie}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) — это IDE для баз данных о JetBrains с выделенной поддержкой ClickHouse. Он также встроен в другие инструменты на основе IntelliJ: PyCharm, IntelliJ IDEA, GoLand, PhpStorm и другие.

Основные возможности:

-   Очень быстрое дополнение кода.
-   Подсветка синтаксиса для SQL диалекта ClickHouse.
-   Поддержка функций, специфичных для ClickHouse, например вложенных столбцов, движков таблиц.
-   Редактор данных.
-   Рефакторинги.
-   Поиск и навигация.

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens) — cервис визуализации и анализа данных.

Основные возможности:

-   Широкий выбор инструментов визуализации, от простых столбчатых диаграмм до сложных дашбордов.
-   Возможность опубликовать дашборды на широкую аудиторию.
-   Поддержка множества источников данных, включая ClickHouse.
-   Хранение материализованных данных в кластере ClickHouse DataLens.

Для небольших проектов DataLens [доступен бесплатно](https://cloud.yandex.ru/docs/datalens/pricing), в том числе и для коммерческого использования.

-   [Документация DataLens](https://cloud.yandex.ru/docs/datalens/).
-   [Пособие по визуализации данных из ClickHouse](https://cloud.yandex.ru/docs/solutions/datalens/data-from-ch-visualization).

### Holistics Software {#holistics-software}

[Holistics](https://www.holistics.io/) — full-stack платформа для обработки данных и бизнес-аналитики.

Основные возможности:

-   Автоматизированные отчёты на почту, Slack, и Google Sheet.
-   Редактор SQL c визуализацией, контролем версий, автодополнением, повторным использованием частей запроса и динамическими фильтрами.
-   Встроенные инструменты анализа отчётов и всплывающие (iframe) дашборды.
-   Подготовка данных и возможности ETL.
-   Моделирование данных с помощью SQL для их реляционного отображения.

[Оригинальная статья](https://clickhouse.tech/docs/ru/interfaces/third-party/gui/) <!--hide-->

### Looker {#looker}

[Looker](https://looker.com) — платформа для обработки данных и бизнес-аналитики. Поддерживает более 50 диалектов баз данных, включая ClickHouse. Looker можно установить самостоятельно или воспользоваться готовой платформой SaaS.

Просмотр данных, построение отображений и дашбордов, планирование отчётов и обмен данными с коллегами доступны с помощью браузера. Также, Looker предоставляет ряд инструментов, позволяющих встраивать сервис в другие приложения и API для обмена данными.

Основные возможности:

-   Язык LookML, поддерживающий [моделирование данных](https://looker.com/platform/data-modeling).
-   Интеграция с различными системами с помощью [Data Actions](https://looker.com/platform/actions).
-   Инструменты для встраивания сервиса в приложения.
-   API.

[Как сконфигурировать ClickHouse в Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

### SeekTable {#seektable}

[SeekTable](https://www.seektable.com) — это Self-Service BI для визуализации данных и генерации отчетов. SeekTable работает как облачный сервис или как сервис, запущенный на вашем сервере. Отчеты, сгенерированные в SeekTable, можно встроить в любое веб-приложение.

Особенности:

-   Удобный пользовательский интерфейс генератора отчетов.
-   Гибкая настройка параметров фильтрации SQL-запросов, создание уникальных запросов для отдельных отчетов.
-   Подключение к ClickHouse либо посредством TCP/IP, либо через HTTP(S) интерфейс (два различных драйвера).
-   Возможность использовать всю мощь SQL-диалекта ClickHouse для задания измерений (dimensions) и мер (measures) в OLAP. 
-   Для автоматизированной генерации отчетов используется [Web API](https://www.seektable.com/help/web-api-integration).
-   Поддерживает работу с отчетами, содержащими данные учетных записей — есть функция [резервного копирования](https://www.seektable.com/help/self-hosted-backup-restore). Модели данных (кубы) и конфигурации отчетов хранятся в читаемом формате XML, есть возможность хранить их в системе контроля версий.

Для персонального использования SeekTable можно использовать [бесплатно](https://www.seektable.com/help/cloud-pricing).

[Как сконфигурировать соединение ClickHouse в SeekTable.](https://www.seektable.com/help/clickhouse-pivot-table)

### Chadmin {#chadmin}

[Chadmin](https://github.com/bun4uk/chadmin) — простой графический интерфейс для визуализации запущенных запросов на вашем кластере ClickHouse. Он отображает информацию о запросах и дает возможность их завершать.

[Original article](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->