# Внешние словари {#dicts-external_dicts}

Существует возможность подключать собственные словари из различных источников данных. Источником данных для словаря может быть локальный текстовый/исполняемый файл, HTTP(s) ресурс или другая СУБД. Подробнее смотрите в разделе "[Источники внешних словарей](external_dicts_dict_sources.md)".

ClickHouse:
- Полностью или частично хранит словари в оперативной памяти.
- Периодически обновляет их и динамически подгружает отсутствующие значения.
- Позволяет создавать внешние словари с помощью xml-файлов или [DDL-запросов](../create.md#create-dictionary-query).

Конфигурация внешних словарей может находится в одном или нескольких xml-файлах. Путь к конфигурации указывается в параметре [dictionaries_config](../../operations/server_settings/settings.md).

Словари могут загружаться при старте сервера или при первом использовании, в зависимости от настройки [dictionaries_lazy_load](../../operations/server_settings/settings.md).

Конфигурационный файл словарей имеет вид:

```xml
<yandex>
    <comment>Необязательный элемент с любым содержимым. Игнорируется сервером ClickHouse.</comment>

    <!--Необязательный элемент, имя файла с подстановками-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Конфигурация словаря -->
    </dictionary>

    ...

    <dictionary>
        <!-- Конфигурация словаря -->
    </dictionary>
</yandex>
```

В одном файле можно [сконфигурировать](external_dicts_dict.md) произвольное количество словарей.

Если вы создаёте внешние словари [DDL-запросами](../create.md#create-dictionary-query), то не задавайте конфигурацию словаря в конфигурации сервера.

!!! attention "Внимание"
    Можно преобразовывать значения по небольшому словарю, описав его в запросе `SELECT` (см. функцию [transform](../functions/other_functions.md)). Эта функциональность не связана с внешними словарями.


## Смотрите также {#ext-dicts-see-also}

- [Настройка внешнего словаря](external_dicts_dict.md)
- [Хранение словарей в памяти](external_dicts_dict_layout.md)
- [Обновление словарей](external_dicts_dict_lifetime.md)
- [Источники внешних словарей](external_dicts_dict_sources.md)
- [Ключ и поля словаря](external_dicts_dict_structure.md)
- [Функции для работы с внешними словарями](../functions/ext_dict_functions.md#ext_dict_functions)

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/external_dicts/) <!--hide-->
