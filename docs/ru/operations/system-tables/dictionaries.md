# system.dictionaries {#system_tables-dictionaries}

Содержит информацию о [внешних словарях](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится словарь, созданный с помощью DDL-запроса. Пустая строка для других словарей.
-   `name` ([String](../../sql-reference/data-types/string.md)) — [имя словаря](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — уникальный UUID словаря.
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — статус словаря. Возможные значения:
    -   `NOT_LOADED` — словарь не загружен, потому что не использовался.
    -   `LOADED` — словарь загружен успешно.
    -   `FAILED` — словарь не загружен в результате ошибки.
    -   `LOADING` — словарь в процессе загрузки.
    -   `LOADED_AND_RELOADING` — словарь загружен успешно, сейчас перезагружается (частые причины: запрос [SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary), таймаут, изменение настроек словаря).
    -   `FAILED_AND_RELOADING` — словарь не загружен в результате ошибки, сейчас перезагружается.
-   `origin` ([String](../../sql-reference/data-types/string.md)) — путь к конфигурационному файлу, описывающему словарь.
-   `type` ([String](../../sql-reference/data-types/string.md)) — тип размещения словаря. [Хранение словарей в памяти](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — массив [имен ключей](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key), предоставляемых словарем.
-   `key.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — соответствующий массив [типов ключей](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key), предоставляемых словарем.
-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — массив [имен атрибутов](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых словарем.
-   `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — соответствующий массив [типов атрибутов](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых словарем.
-   `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объем оперативной памяти, используемый словарем.
-   `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество запросов с момента загрузки словаря или с момента последней успешной перезагрузки.
-   `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — для cache-словарей — процент закэшированных значений.
-   `found_rate` ([Float64](../../sql-reference/data-types/float.md)) — процент обращений к словарю, при которых значение было найдено.
-   `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество элементов, хранящихся в словаре.
-   `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — процент заполнения словаря (для хэшированного словаря — процент заполнения хэш-таблицы).
-   `source` ([String](../../sql-reference/data-types/string.md)) — текст, описывающий [источник данных](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) для словаря.
-   `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — минимальное [время обновления](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — максимальное [время обновления](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала загрузки словаря.
-   `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — время, затраченное на загрузку словаря.
-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — текст ошибки, возникающей при создании или перезагрузке словаря, если словарь не удалось создать.
-   `comment` ([String](../../sql-reference/data-types/string.md)) — текст комментария к словарю.

**Пример**

Настройте словарь:

``` sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Убедитесь, что словарь загружен.

``` sql
SELECT * FROM system.dictionaries LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:                    default
name:                        dictionary_with_comment
uuid:                        4654d460-0d03-433a-8654-d4600d03d33a
status:                      NOT_LOADED
origin:                      4654d460-0d03-433a-8654-d4600d03d33a
type:
key.names:                   ['id']
key.types:                   ['UInt64']
attribute.names:             ['value']
attribute.types:             ['String']
bytes_allocated:             0
query_count:                 0
hit_rate:                    0
found_rate:                  0
element_count:               0
load_factor:                 0
source:
lifetime_min:                0
lifetime_max:                0
loading_start_time:          1970-01-01 00:00:00
last_successful_update_time: 1970-01-01 00:00:00
loading_duration:            0
last_exception:
comment:                     The temporary dictionary
```
