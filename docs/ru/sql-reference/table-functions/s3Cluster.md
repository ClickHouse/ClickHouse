---
sidebar_position: 55
sidebar_label: s3Cluster
---

# Табличная функция s3Cluster {#s3Cluster-table-function}

Позволяет обрабатывать файлы из [Amazon S3](https://aws.amazon.com/s3/) параллельно из многих узлов в указанном кластере. На узле-инициаторе функция создает соединение со всеми узлами в кластере, заменяет символы '*' в пути к файлу S3 и динамически отправляет каждый файл. На рабочем узле функция запрашивает у инициатора следующую задачу и обрабатывает ее. Это повторяется до тех пор, пока все задачи не будут завершены.

**Синтаксис**

``` sql
s3Cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
```

**Аргументы**

-   `cluster_name` — имя кластера, используемое для создания набора адресов и параметров подключения к удаленным и локальным серверам.
-   `source` — URL файла или нескольких файлов. Поддерживает следующие символы подстановки: `*`, `?`, `{'abc','def'}` и `{N..M}`, где `N`, `M` — числа, `abc`, `def` — строки. Подробнее смотрите в разделе [Символы подстановки](../../engines/table-engines/integrations/s3.md#wildcards-in-path).
-   `access_key_id` и `secret_access_key` — ключи, указывающие на учетные данные для использования с точкой приема запроса. Необязательные параметры.
-   `format` — [формат](../../interfaces/formats.md#formats) файла.
-   `structure` — структура таблицы. Формат `'column1_name column1_type, column2_name column2_type, ...'`.

**Возвращаемое значение**

Таблица с указанной структурой для чтения или записи данных в указанный файл.

**Примеры**

Вывод данных из всех файлов кластера `cluster_simple`:

``` sql
SELECT * FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') ORDER BY (name, value, polygon);
```

Подсчет общего количества строк во всех файлах кластера `cluster_simple`:

``` sql
SELECT count(*) FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))');
```

:::danger "Внимание"
    Если список файлов содержит диапазоны чисел с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры отдельно или используйте `?`.

**Смотрите также**

-   [Движок таблиц S3](../../engines/table-engines/integrations/s3.md)
-   [Табличная функция s3](../../sql-reference/table-functions/s3.md)
