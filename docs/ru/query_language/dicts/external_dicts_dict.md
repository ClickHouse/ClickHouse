# Настройка внешнего словаря {#dicts-external_dicts_dict}

XML-конфигурация словаря имеет следующую структуру:

```xml
<dictionary>
    <name>dict_name</name>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>
</dictionary>
```

Соответствующий [DDL-запрос](../create.md#create-dictionary-query) имеет следующий вид:

```sql
CREATE DICTIONARY dict_name
(
    ... -- attributes
)
PRIMARY KEY ... -- complex or single key configuration
SOURCE(...) -- Source configuration
LAYOUT(...) -- Memory layout configuration
LIFETIME(...) -- Lifetime of dictionary in memory
```

- `name` — Идентификатор, под которым словарь будет доступен для использования. Используйте символы `[a-zA-Z0-9_\-]`.
- [source](external_dicts_dict_sources.md) — Источник словаря.
- [layout](external_dicts_dict_layout.md) — Размещение словаря в памяти.
- [structure](external_dicts_dict_structure.md) — Структура словаря. Ключ и атрибуты, которые можно получить по ключу.
- [lifetime](external_dicts_dict_lifetime.md) — Периодичность обновления словарей.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/external_dicts_dict/) <!--hide-->
