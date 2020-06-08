# Внешние словари {#dicts-external-dicts}

Существует возможность подключать собственные словари из различных источников данных. Источником данных для словаря может быть локальный текстовый/исполняемый файл, HTTP(s) ресурс или другая СУБД. Подробнее смотрите в разделе «[Источники внешних словарей](external-dicts-dict-sources.md)».

ClickHouse:
- Полностью или частично хранит словари в оперативной памяти.
- Периодически обновляет их и динамически подгружает отсутствующие значения.
- Позволяет создавать внешние словари с помощью xml-файлов или [DDL-запросов](../../statements/create.md#create-dictionary-query).

Конфигурация внешних словарей может находится в одном или нескольких xml-файлах. Путь к конфигурации указывается в параметре [dictionaries\_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config).

Словари могут загружаться при старте сервера или при первом использовании, в зависимости от настройки [dictionaries\_lazy\_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load).

Системная таблица [system.dictionaries](../../../operations/system-tables.md#system_tables-dictionaries) содержит информацию о словарях, сконфигурированных на сервере. Для каждого словаря там можно найти:

- Статус словаря.
- Конфигурационные параметры.
- Метрики, наподобие количества занятой словарём RAM или количества запросов к словарю с момента его успешной загрузки.

Конфигурационный файл словарей имеет вид:

``` xml
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

В одном файле можно [сконфигурировать](external-dicts-dict.md) произвольное количество словарей.

Если вы создаёте внешние словари [DDL-запросами](../../statements/create.md#create-dictionary-query), то не задавайте конфигурацию словаря в конфигурации сервера.

!!! attention "Внимание"
    Можно преобразовывать значения по небольшому словарю, описав его в запросе `SELECT` (см. функцию [transform](../../../sql-reference/functions/other-functions.md)). Эта функциональность не связана с внешними словарями.

## Смотрите также {#ext-dicts-see-also}

-   [Настройка внешнего словаря](external-dicts-dict.md)
-   [Хранение словарей в памяти](external-dicts-dict-layout.md)
-   [Обновление словарей](external-dicts-dict-lifetime.md)
-   [Источники внешних словарей](external-dicts-dict-sources.md)
-   [Ключ и поля словаря](external-dicts-dict-structure.md)
-   [Функции для работы с внешними словарями](../../../sql-reference/functions/ext-dict-functions.md)

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/external_dicts/) <!--hide-->
