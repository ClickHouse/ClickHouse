---
toc_priority: 68
toc_title: "Хранение данных на внешних дисках"
---

# Хранение данных на внешних дисках {#external-disks}

Данные, которые обрабатываются в ClickHouse, обычно хранятся в файловой системе локально, где развернут сервер ClickHouse. При этом для хранения данных требуются диски большого объема, которые могут быть довольно дорогостоящими. Решением проблемы может стать хранение данных отдельно от сервера — в распределенных файловых системах — [Amazon S3](https://aws.amazon.com/s3/) или Hadoop ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

Для работы с данными, хранящимися в файловой системе `Amazon S3`, используйте движок [S3](../engines/table-engines/integrations/s3.md), а для работы с данными в файловой системе Hadoop — движок [HDFS](../engines/table-engines/integrations/hdfs.md). 

## Репликация без копирования данных {#zero-copy}

Для дисков `S3` и `HDFS` в ClickHouse поддерживается репликация без копирования данных (zero-copy): если данные хранятся на нескольких репликах, то при синхронизации пересылаются только метаданные (пути к кускам данных), а сами данные не копируются.

## Использование сервиса HDFS для хранения данных {#table_engine-mergetree-hdfs}

Таблицы семейств [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) и [Log](../engines/table-engines/log-family/log.md) могут хранить данные в сервисе HDFS при использовании диска типа `HDFS`.

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

## Использование виртуальной файловой системы для шифрования данных {#encrypted-virtual-file-system}

Вы можете зашифровать данные, сохраненные на внешних дисках [S3](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3) или [HDFS](#table_engine-mergetree-hdfs) или на локальном диске. Чтобы включить режим шифрования, в конфигурационном файле вы должны указать диск с типом `encrypted` и тип диска, на котором будут сохранены данные. Диск типа `encrypted` шифрует данные "на лету", то есть при чтении файлов с этого диска расшифровка происходит автоматически. Таким образом, вы можете работать с диском типа `encrypted` как с обычным.

Пример конфигурации:

``` xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

Например, когда ClickHouse записывает данные из какой-либо таблицы в файл `store/all_1_1_0/data.bin` на `disk1`, то на самом деле этот файл будет записан на физический диск по пути `/path1/store/all_1_1_0/data.bin`.

При записи того же файла на диск `disk2` он будет записан на физический диск в зашифрованном виде по пути `/path1/path2/store/all_1_1_0/data.bin`.

Обязательные параметры:

-   `type` — `encrypted`. Иначе зашифрованный диск создан не будет.
-   `disk` — тип диска для хранения данных.
-   `key` — ключ для шифрования и расшифровки. Тип: [Uint64](../sql-reference/data-types/int-uint.md). Вы можете использовать параметр `key_hex` для шифрования в шестнадцатеричной форме.
    Вы можете указать несколько ключей, используя атрибут `id` (смотрите пример выше).

Необязательные параметры:

-   `path` — путь к месту на диске, где будут сохранены данные. Если не указан, данные будут сохранены в корневом каталоге.
-   `current_key_id` — ключ, используемый для шифрования. Все указанные ключи могут быть использованы для расшифровки, и вы всегда можете переключиться на другой ключ, сохраняя доступ к ранее зашифрованным данным.
-   `algorithm` — [алгоритм](../sql-reference/statements/create/table.md#create-query-encryption-codecs) шифрования данных. Возможные значения: `AES_128_CTR`, `AES_192_CTR` или `AES_256_CTR`. Значение по умолчанию: `AES_128_CTR`. Длина ключа зависит от алгоритма: `AES_128_CTR` — 16 байт, `AES_192_CTR` — 24 байта, `AES_256_CTR` — 32 байта.

Пример конфигурации:

``` xml
<yandex>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</yandex>
```
