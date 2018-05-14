<a name="dicts-external_dicts_dict"></a>

# Настройка внешнего словаря

Конфигурация словаря имеет следующую структуру:

```xml
<dictionary>
    <name>dict_name</name>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>
</dictionary>
```

-   name - Идентификатор, под которым словарь будет доступен для использования. Используйте символы `[a-zA-Z0-9_\-]`.
-  [source](external_dicts_dict_sources.md#dicts-external_dicts_dict_sources) - Источник словаря.
-  [layout](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout) - Размещение словаря в памяти.
-  [structure](external_dicts_dict_structure.md#dicts-external_dicts_dict_structure) - Структура словаря. Ключ и атрибуты, которые можно получить по ключу.
-  [lifetime](external_dicts_dict_lifetime.md#dicts-external_dicts_dict_lifetime) - Периодичность обновления словарей.
