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
<clickhouse>
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
</clickhouse>
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
<clickhouse>
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
</clickhouse>
```

## Хранение данных на веб-сервере {#storing-data-on-webserver}

Существует утилита `clickhouse-static-files-uploader`, которая готовит каталог данных для данной таблицы (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`). Для каждой таблицы, необходимой вам, вы получаете каталог файлов. Эти файлы могут быть загружены, например, на веб-сервер в виде статических файлов. После этой подготовки вы можете загрузить эту таблицу на любой сервер ClickHouse через `DiskWeb`.

Этот тип диска используется только для чтения, то есть данные на нем неизменяемы. Новая таблица загружается с помощью запроса `ATTACH TABLE` (см. пример ниже) и при каждом чтении данные будут доставаться по заданному `url` через http запрос, то есть локально данные не хранятся. Любое изменение данных приведет к исключению, т.е. не допускаются следующие типы запросов: [CREATE TABLE](../sql-reference/statements/create/table.md), [ALTER TABLE](../sql-reference/statements/alter/index.md), [RENAME TABLE](../sql-reference/statements/rename.md#misc_operations-rename_table), [DETACH TABLE](../sql-reference/statements/detach.md) и [TRUNCATE TABLE](../sql-reference/statements/truncate.md).

Хранение данных на веб-сервере поддерживается только для табличных движков семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) и [Log](../engines/table-engines/log-family/log.md). Чтобы получить доступ к данным, хранящимся на диске `web`, при выполнении запроса используйте настройку [storage_policy](../engines/table-engines/mergetree-family/mergetree.md#terms). Например, `ATTACH TABLE table_web UUID '{}' (id Int32) ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy = 'web'`.

Готовый тестовый пример. Добавьте эту конфигурацию в config:

``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

А затем выполните этот запрос:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

Обязательные параметры:

-   `type` — `web`. Иначе диск создан не будет.
-   `endpoint` — URL точки приема запроса в формате `path`. URL точки должен содержать путь к корневой директории на сервере для хранения данных, куда эти данные были загружены.

Необязательные параметры:

-   `min_bytes_for_seek` — минимальное количество байтов, которое используются для операций поиска вместо последовательного чтения. Значение по умолчанию: `1` Mb.
-   `remote_fs_read_backoff_threashold` — максимальное время ожидания при попытке чтения данных с удаленного диска. Значение по умолчанию: `10000` секунд.
-   `remote_fs_read_backoff_max_tries` — максимальное количество попыток чтения данных с задержкой. Значение по умолчанию: `5`.

Если после выполнения запроса генерируется исключение `DB:Exception Unreachable URL`, то могут помочь настройки: [http_connection_timeout](../operations/settings/settings.md#http_connection_timeout), [http_receive_timeout](../operations/settings/settings.md#http_receive_timeout), [keep_alive_timeout](../operations/server-configuration-parameters/settings.md#keep-alive-timeout).

Чтобы получить файлы для загрузки, выполните:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` может быть получен в результате запроса `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

Файлы должны быть загружены по пути `<endpoint>/store/`, но конфигурация должна содержать только `endpoint`.

Если URL-адрес недоступен при загрузке на диск, когда сервер запускает таблицы, то все ошибки будут пойманы. Если в этом случае были ошибки, то таблицы можно перезагрузить (сделать видимыми) с помощью `DETACH TABLE table_name` -> `ATTACH TABLE table_name`. Если метаданные были успешно загружены при запуске сервера, то таблицы будут доступны сразу.

Чтобы ограничить количество попыток чтения данных во время одного HTTP-запроса, используйте настройку [http_max_single_read_retries](../operations/settings/settings.md#http-max-single-read-retries).
