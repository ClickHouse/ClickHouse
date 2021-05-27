# system.dictionaries {#system_tables-dictionaries}

Содержит информацию о [внешних словарях](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Имя базы данных, в которой находится словарь, созданный с помощью DDL-запроса. Пустая строка для других словарей.
-   `name` ([String](../../sql-reference/data-types/string.md)) — [Имя словаря](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Статус словаря. Возможные значения:
    -   `NOT_LOADED` — Словарь не загружен, потому что не использовался.
    -   `LOADED` — Словарь загружен успешно.
    -   `FAILED` — Словарь не загружен в результате ошибки.
    -   `LOADING` — Словарь в процессе загрузки.
    -   `LOADED_AND_RELOADING` — Словарь загружен успешно, сейчас перезагружается (частые причины: запрос [SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary), таймаут, изменение настроек словаря).
    -   `FAILED_AND_RELOADING` — Словарь не загружен в результате ошибки, сейчас перезагружается.
-   `origin` ([String](../../sql-reference/data-types/string.md)) — Путь к конфигурационному файлу, описывающему словарь.
-   `type` ([String](../../sql-reference/data-types/string.md)) — Тип размещения словаря. [Хранение словарей в памяти](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Тип ключа](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Числовой ключ ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) или Составной ключ ([String](../../sql-reference/data-types/string.md)) — строка вида “(тип 1, тип 2, …, тип n)”.
-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Массив [имен атрибутов](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых справочником.
-   `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Соответствующий массив [типов атрибутов](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых справочником.
-   `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Объем оперативной памяти, используемый словарем.
-   `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество запросов с момента загрузки словаря или с момента последней успешной перезагрузки.
-   `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — Для cache-словарей — процент закэшированных значений.
-   `found_rate` ([Float64](../../sql-reference/data-types/float.md)) — Процент обращений к словарю, при которых значение было найдено.
-   `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество элементов, хранящихся в словаре.
-   `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — Процент заполнения словаря (для хэшированного словаря — процент заполнения хэш-таблицы).
-   `source` ([String](../../sql-reference/data-types/string.md)) — Текст, описывающий [источник данных](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) для словаря.
-   `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Минимальное [время обновления](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Максимальное [время обновления](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Время начала загрузки словаря.
-   `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — Время, затраченное на загрузку словаря.
-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — Текст ошибки, возникающей при создании или перезагрузке словаря, если словарь не удалось создать.

**Пример**

Настройте словарь.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Убедитесь, что словарь загружен.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```
