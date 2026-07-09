---
description: 'Типы структур словарей для их хранения в памяти'
sidebar_label: 'Обзор'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Структуры словарей'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## Типы структур словарей
</div>

Словари можно хранить в памяти разными способами; каждый из них предполагает свой компромисс между использованием CPU и оперативной памяти.

| Структура                                                                                                  | Описание                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [flat](./flat.md)                                                                                          | Хранит данные в плоских массивах, индексируемых по ключу. Самая быстрая структура, но ключи должны иметь тип `UInt64` и не превышать `max_array_size`. |
| [hashed](./hashed.md)                                                                                      | Хранит данные в хеш-таблице. Ограничений на размер ключа нет, поддерживается любое количество элементов.                                               |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | Как `hashed`, но снижает использование памяти ценой CPU.                                                                                               |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | Как `hashed`, но для составных ключей.                                                                                                                 |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | Как `sparse_hashed`, но для составных ключей.                                                                                                          |
| [hashed&#95;array](./hashed-array.md)                                                                      | Атрибуты хранятся в массивах, а хеш-таблица сопоставляет ключи с индексами массивов. Эффективно использует память при большом количестве атрибутов.    |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | Как `hashed_array`, но для составных ключей.                                                                                                           |
| [range&#95;hashed](./range-hashed.md)                                                                      | Хеш-таблица с упорядоченными диапазонами. Поддерживает поиск по ключу и диапазону даты/времени.                                                        |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | Как `range_hashed`, но для составных ключей.                                                                                                           |
| [cache](./cache.md)                                                                                        | Кэш фиксированного размера в оперативной памяти. Хранятся только часто запрашиваемые ключи.                                                            |
| [complex&#95;key&#95;cache](/ru/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | Как `cache`, но для составных ключей.                                                                                                                  |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | Как `cache`, но хранит данные на SSD, а индекс — в памяти.                                                                                             |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | Как `ssd_cache`, но для составных ключей.                                                                                                              |
| [direct](./direct.md)                                                                                      | Без хранения в памяти — источник запрашивается напрямую при каждом запросе.                                                                            |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | Как `direct`, но для составных ключей.                                                                                                                 |
| [ip&#95;trie](./ip-trie.md)                                                                                | Структура trie для быстрого поиска IP-префиксов (на основе CIDR).                                                                                      |

:::tip Рекомендуемые структуры
[flat](./flat.md), [hashed](./hashed.md) и [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) обеспечивают лучшую производительность запросов.
Структуры с кэшированием не рекомендуются из-за возможной низкой производительности и сложности настройки параметров — подробности см. в [cache](./cache.md).
:::

<div id="specify-dictionary-layout">
  ## Укажите структуру словаря
</div>

<CloudDetails />

Вы можете настроить структуру словаря с помощью выражения `LAYOUT` (для DDL) или параметра `layout` в определениях файла конфигурации.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- настройки структуры
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- настройки структуры -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

См. также [CREATE DICTIONARY](../overview.md) для полного синтаксиса DDL.

Словари, в структуре которых нет слова `complex-key*`, имеют ключ типа [UInt64](/ru/sql-reference/data-types/int-uint.md), а словари `complex-key*` — составной ключ (сложный, с произвольными типами).

**Пример числового ключа** (столбец key&#95;column имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**Пример составного ключа** (ключ содержит один элемент типа [String](/ru/sql-reference/data-types/string.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## Как повысить производительность словаря
</div>

Есть несколько способов повысить производительность словаря:

* Вызывайте функцию, работающую со словарём, после `GROUP BY`.
* Помечайте извлекаемые атрибуты как инъективные.
  Атрибут называется инъективным, если разным ключам соответствуют разные значения атрибута.
  Поэтому, когда в `GROUP BY` используется функция, которая получает значение атрибута по ключу, эта функция автоматически выносится из `GROUP BY`.

ClickHouse генерирует исключение при ошибках, связанных со словарями.
Например, это могут быть следующие ошибки:

* Не удалось загрузить словарь, к которому выполняется обращение.
* Ошибка при выполнении запроса к словарю `cached`.

Вы можете просмотреть список словарей и их состояния в таблице [system.dictionaries](/ru/operations/system-tables/dictionaries.md).