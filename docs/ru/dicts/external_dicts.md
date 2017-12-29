<a name="dicts-external_dicts"></a>

# Внешние словари

Существует возможность подключать собственные словари из различных источников данных. Источником данных для словаря может быть локальный текстовый/исполняемый файл, HTTP(s) ресурс или другая СУБД. Подробнее смотрите в разделе "[Источники внешних словарей](external_dicts_dict_sources.md#dicts-external_dicts_dict_sources)".

ClickHouse:

> -   Полностью или частично хранит словари в оперативной памяти.
> -   Периодически обновляет их и динамически подгружает отсутствующие значения. Т.е. словари можно подгружать динамически.

Конфигурация внешних словарей находится в одном или нескольких файлах. Путь к конфигурации указывается в параметре [dictionaries_config](../operations/server_settings/settings.md#server_settings-dictionaries_config).

Словари могут загружаться при старте сервера или при первом использовании, в зависимости от настройки [dictionaries_lazy_load](../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load).

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

В одном файле можно [сконфигурировать](external_dicts_dict.md#dicts-external_dicts_dict) произвольное количество словарей. Формат файла сохраняется даже если словарь один (т.е. `<yandex><dictionary> <!--configuration--> </dictionary></yandex>`).

Смотрите также "[Функции для работы с внешними словарями](../functions/ext_dict_functions.md#ext_dict_functions)" .

<div class="admonition attention">

Вы можете преобразовывать значения по небольшому словарю, описав его в запросе `SELECT` (см. функцию [transform](../functions/other_functions.md#other_functions-transform)). Эта функциональность не связана с внешними словарями.

</div>

```eval_rst
.. toctree::
    :glob:
   
    external_dicts_dict*
```
