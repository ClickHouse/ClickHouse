---
toc_priority: 41
toc_title: "Хранение словарей в памяти"
---

# Хранение словарей в памяти {#dicts-external-dicts-dict-layout}

Словари можно размещать в памяти множеством способов.

Рекомендуем [flat](#flat), [hashed](#dicts-external_dicts_dict_layout-hashed) и [complex_key_hashed](#complex-key-hashed). Скорость обработки словарей при этом максимальна.

Размещение с кэшированием не рекомендуется использовать из-за потенциально низкой производительности и сложностей в подборе оптимальных параметров. Читайте об этом подробнее в разделе [cache](#cache).

Повысить производительность словарей можно следующими способами:

-   Вызывать функцию для работы со словарём после `GROUP BY`.
-   Помечать извлекаемые атрибуты как инъективные. Атрибут называется инъективным, если разным ключам соответствуют разные значения атрибута. Тогда при использовании в `GROUP BY` функции, достающей значение атрибута по ключу, эта функция автоматически выносится из `GROUP BY`.

При ошибках работы со словарями ClickHouse генерирует исключения. Например, в следующих ситуациях:

-   При обращении к словарю, который не удалось загрузить.
-   При ошибке запроса к `cached`-словарю.

Список внешних словарей и их статус можно посмотреть в таблице `system.dictionaries`.

Общий вид конфигурации:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

Соответствущий [DDL-запрос](../../statements/create/dictionary.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## Способы размещения словарей в памяти {#ways-to-store-dictionaries-in-memory}

-   [flat](#flat)
-   [hashed](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [cache](#cache)
-   [ssd_cache](#ssd-cache)
-   [ssd_complex_key_cache](#complex-key-ssd-cache)
-   [direct](#direct)
-   [range_hashed](#range-hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_cache](#complex-key-cache)
-   [complex_key_direct](#complex-key-direct)
-   [ip_trie](#ip-trie)

### flat {#flat}

Словарь полностью хранится в оперативной памяти в виде плоских массивов. Объём памяти, занимаемой словарём, пропорционален размеру самого большого ключа (по объему).

Ключ словаря имеет тип [UInt64](../../../sql-reference/data-types/int-uint.md) и его величина ограничена параметром `max_array_size` (значение по умолчанию — 500 000). Если при создании словаря обнаружен ключ больше, то ClickHouse бросает исключение и не создает словарь. Начальный размер плоских массивов словарей контролируется параметром initial_array_size (по умолчанию - 1024).

Поддерживаются все виды источников. При обновлении данные (из файла или из таблицы) считываются целиком.

Это метод обеспечивает максимальную производительность среди всех доступных способов размещения словаря.

Пример конфигурации:

``` xml
<layout>
  <flat>
    <initial_array_size>50000</initial_array_size>
    <max_array_size>5000000</max_array_size>
  </flat>
</layout>
```

или

``` sql
LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
```

### hashed {#dicts-external_dicts_dict_layout-hashed}

Словарь полностью хранится в оперативной памяти в виде хэш-таблиц. Словарь может содержать произвольное количество элементов с произвольными идентификаторами. На практике количество ключей может достигать десятков миллионов элементов.

Если `preallocate` имеет значение `true` (по умолчанию `false`), хеш-таблица будет предварительно определена (это ускорит загрузку словаря). Используйте этот метод только в случае, если:

- Источник поддерживает произвольное количество элементов (пока поддерживается только источником `ClickHouse`).
- В данных нет дубликатов (иначе это может увеличить объем используемой памяти хеш-таблицы).

Поддерживаются все виды источников. При обновлении данные (из файла, из таблицы) читаются целиком.

Пример конфигурации:

``` xml
<layout>
   <hashed>
    <preallocate>0</preallocate>
  </hashed>
</layout>
```

или

``` sql
LAYOUT(HASHED(PREALLOCATE 0))
```

### sparse_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

Аналогичен `hashed`, но при этом занимает меньше места в памяти и генерирует более высокую загрузку CPU.

Для этого типа размещения также можно задать `preallocate` в значении `true`. В данном случае это более важно, чем для типа `hashed`.

Пример конфигурации:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

или

``` sql
LAYOUT(SPARSE_HASHED([PREALLOCATE 0]))
```

### complex_key_hashed {#complex-key-hashed}

Тип размещения предназначен для использования с составными [ключами](external-dicts-dict-structure.md). Аналогичен `hashed`.

Пример конфигурации:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

или

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range_hashed {#range-hashed}

Словарь хранится в оперативной памяти в виде хэш-таблицы с упорядоченным массивом диапазонов и соответствующих им значений.

Этот способ размещения работает также как и hashed и позволяет дополнительно к ключу использовать дипазоны по дате/времени (произвольному числовому типу).

Пример: таблица содержит скидки для каждого рекламодателя в виде:

``` text
+---------------+---------------------+-------------------+--------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------------+---------------------+-------------------+--------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------------+---------------------+-------------------+--------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------------+---------------------+-------------------+--------+
```

Чтобы использовать выборку по диапазонам дат, необходимо в [structure](external-dicts-dict-structure.md) определить элементы `range_min`, `range_max`. В этих элементах должны присутствовать элементы `name` и `type` (если `type` не указан, будет использован тип по умолчанию – Date). `type` может быть любым численным типом (Date/DateTime/UInt64/Int32/др.).

Пример:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

или

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

Для работы с такими словарями в функцию `dictGetT` необходимо передавать дополнительный аргумент, для которого подбирается диапазон:

      dictGetT('dict_name', 'attr_name', id, date)

Функция возвращает значение для заданных `id` и диапазона дат, в который входит переданная дата.

Особенности алгоритма:

-   Если не найден `id` или для найденного `id` не найден диапазон, то возвращается значение по умолчанию для словаря.
-   Если есть перекрывающиеся диапазоны, то возвращается значение из любого (случайного) подходящего диапазона.
-   Если граница диапазона `NULL` или некорректная дата (1900-01-01), то диапазон считается открытым. Диапазон может быть открытым с обеих сторон.

Пример конфигурации:

``` xml
<yandex>
        <dictionary>

                ...

                <layout>
                        <range_hashed />
                </layout>

                <structure>
                        <id>
                                <name>Abcdef</name>
                        </id>
                        <range_min>
                                <name>StartTimeStamp</name>
                                <type>UInt64</type>
                        </range_min>
                        <range_max>
                                <name>EndTimeStamp</name>
                                <type>UInt64</type>
                        </range_max>
                        <attribute>
                                <name>XXXType</name>
                                <type>String</type>
                                <null_value />
                        </attribute>
                </structure>

        </dictionary>
</yandex>
```

или

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

### cache {#cache}

Словарь хранится в кэше, состоящем из фиксированного количества ячеек. Ячейки содержат часто используемые элементы.

При поиске в словаре сначала просматривается кэш. На каждый блок данных, все не найденные в кэше или устаревшие ключи запрашиваются у источника с помощью `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Затем, полученные данные записываются в кэш.

Для cache-словарей может быть задано время устаревания [lifetime](external-dicts-dict-lifetime.md) данных в кэше. Если от загрузки данных в ячейке прошло больше времени, чем `lifetime`, то значение не используется, и будет запрошено заново при следующей необходимости его использовать.

Это наименее эффективный из всех способов размещения словарей. Скорость работы кэша очень сильно зависит от правильности настройки и сценария использования. Словарь типа cache показывает высокую производительность лишь при достаточно больших hit rate-ах (рекомендуется 99% и выше). Посмотреть средний hit rate можно в таблице `system.dictionaries`.

Чтобы увеличить производительность кэша, используйте подзапрос с `LIMIT`, а снаружи вызывайте функцию со словарём.

Поддерживаются [источники](external-dicts-dict-sources.md): MySQL, ClickHouse, executable, HTTP.

Пример настройки:

``` xml
<layout>
    <cache>
        <!-- Размер кэша в количестве ячеек. Округляется вверх до степени двух. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

или

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

Укажите достаточно большой размер кэша. Количество ячеек следует подобрать экспериментальным путём:

1.  Выставить некоторое значение.
2.  Запросами добиться полной заполненности кэша.
3.  Оценить потребление оперативной памяти с помощью таблицы `system.dictionaries`.
4.  Увеличивать/уменьшать количество ячеек до получения требуемого расхода оперативной памяти.

!!! warning "Warning"
    Не используйте в качестве источника ClickHouse, поскольку он медленно обрабатывает запросы со случайным чтением.

### complex_key_cache {#complex-key-cache}

Тип размещения предназначен для использования с составными [ключами](external-dicts-dict-structure.md). Аналогичен `cache`.

### ssd_cache {#ssd-cache}

Похож на `cache`, но хранит данные на SSD и индекс в оперативной памяти.

``` xml
<layout>
    <ssd_cache>
        <!-- Size of elementary read block in bytes. Recommended to be equal to SSD's page size. -->
        <block_size>4096</block_size>
        <!-- Max cache file size in bytes. -->
        <file_size>16777216</file_size>
        <!-- Size of RAM buffer in bytes for reading elements from SSD. -->
        <read_buffer_size>131072</read_buffer_size>
        <!-- Size of RAM buffer in bytes for aggregating elements before flushing to SSD. -->
        <write_buffer_size>1048576</write_buffer_size>
        <!-- Path where cache file will be stored. -->
        <path>/var/lib/clickhouse/clickhouse_dictionaries/test_dict</path>
    </ssd_cache>
</layout>
```

или

``` sql
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH ./user_files/test_dict))
```

### complex_key_ssd_cache {#complex-key-ssd-cache}

Тип размещения предназначен для использования с составными [ключами](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Похож на `ssd_cache`.

### direct {#direct}

Словарь не хранит данные локально и взаимодействует с источником непосредственно в момент запроса.

Ключ словаря имеет тип `UInt64`.

Поддерживаются все виды [источников](external-dicts-dict-sources.md), кроме локальных файлов.

Пример конфигурации:

``` xml
<layout>
  <direct />
</layout>
```

или

``` sql
LAYOUT(DIRECT())
```

### complex_key_direct {#complex-key-direct}

Тип размещения предназначен для использования с составными [ключами](external-dicts-dict-structure.md). Аналогичен `direct`.

### ip_trie {#ip-trie}

Тип размещения предназначен для сопоставления префиксов сети (IP адресов) с метаданными, такими как ASN.

Пример: таблица содержит префиксы сети и соответствующие им номера AS и коды стран:

``` text
  +-----------------+-------+--------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------------+-------+--------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------------+-------+--------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------------+-------+--------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------------+-------+--------+
```

При использовании такого макета структура должна иметь составной ключ.

Пример:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
</structure>
<layout>
    <ip_trie>
        <!-- Ключевой аттрибут `prefix` будет доступен через dictGetString -->
        <!-- Эта опция увеличивает потреблямую память -->
        <access_to_key_from_attributes>true</access_to_key_from_attributes>
    </ip_trie>
</layout>
```

или

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

Этот ключ должен иметь только один атрибут типа `String`, содержащий допустимый префикс IP. Другие типы еще не поддерживаются.

Для запросов необходимо использовать те же функции (`dictGetT` с кортежем), что и для словарей с составными ключами:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

Функция принимает либо `UInt32` для IPv4, либо `FixedString(16)` для IPv6:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Никакие другие типы не поддерживаются. Функция возвращает атрибут для префикса, соответствующего данному IP-адресу. Если есть перекрывающиеся префиксы, возвращается наиболее специфический.

Данные должны полностью помещаться в оперативной памяти.
