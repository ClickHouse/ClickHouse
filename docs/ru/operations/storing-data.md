---
toc_priority: 68
toc_title: "Хранение данных на внешних дисках"
---

# Хранение данных на внешних дисках {#external-disks}

Данные, которые обрабатываются в ClickHouse, обычно хранятся в файловой системе локально, где развернут сервер ClickHouse. При этом для хранения данных требуются диски большого объема, которые могут быть довольно дорогостоящими. Решением проблемы может стать хранение данных отдельно от сервера — в распределенных файловых системах — [Amazon s3](https://aws.amazon.com/s3/) или Hadoop ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

Для работы с данными, хранящимися в файловой системе `Amazon s3`, используйте движок [s3](../engines/table-engines/integrations/s3.md), а для работы с данными в файловой системе Hadoop — движок [HDFS](../engines/table-engines/integrations/hdfs.md). 

## Репликация без копирования данных {#zero-copy}

Для дисков `s3` и `HDFS` в ClickHouse поддерживается репликация без копирования данных (zero-copy): если данные хранятся на нескольких репликах, то при синхронизации пересылаются только метаданные (пути к кускам данных), а сами данные не копируются.

## Использование сервиса HDFS для хранения данных {#table_engine-mergetree-hdfs}

[HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) — это распределенная файловая система для удаленного хранения данных.

Таблицы семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) могут хранить данные в сервисе HDFS при использовании диска типа `HDFS`.

Пример конфигурации:
``` xml
<yandex>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
            </hdfs>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>

    <merge_tree>
        <min_bytes_for_wide_part>0</min_bytes_for_wide_part>
    </merge_tree>
</yandex>
```

Обязательные параметры:

-   `endpoint` — URL точки приема запроса на стороне HDFS в формате `path`. URL точки должен содержать путь к корневой директории на сервере, где хранятся данные.

Необязательные параметры:

-   `min_bytes_for_seek` — минимальное количество байтов, которые используются для операций поиска вместо последовательного чтения. Значение по умолчанию: `1 МБайт`.
