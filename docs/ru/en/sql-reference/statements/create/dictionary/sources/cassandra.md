---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'Источник для словаря Cassandra'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: 'Настройте Cassandra в качестве источника для словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CASSANDRA(
        host 'localhost'
        port 9042
        user 'username'
        password 'qwerty123'
        keyspace 'database_name'
        column_family 'table_name'
        allow_filtering 1
        partition_key_prefix 1
        consistency 'One'
        where '"SomeColumn" = 42'
        max_threads 8
        query 'SELECT id, value_1, value_2 FROM database_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
        <cassandra>
            <host>localhost</host>
            <port>9042</port>
            <user>username</user>
            <password>qwerty123</password>
            <keyspase>database_name</keyspase>
            <column_family>table_name</column_family>
            <allow_filtering>1</allow_filtering>
            <partition_key_prefix>1</partition_key_prefix>
            <consistency>One</consistency>
            <where>"SomeColumn" = 42</where>
            <max_threads>8</max_threads>
            <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
        </cassandra>
    </source>
    ```
  </TabItem>
</Tabs>

Поля настроек:

| Настройка              | Описание                                                                                                                                                                                                                                                                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | Хост Cassandra или список хостов, разделённых запятыми.                                                                                                                                                                                                                                                                                        |
| `port`                 | Порт серверов Cassandra. Если не указан, используется порт по умолчанию `9042`.                                                                                                                                                                                                                                                                |
| `user`                 | Имя пользователя Cassandra.                                                                                                                                                                                                                                                                                                                    |
| `password`             | Пароль пользователя Cassandra.                                                                                                                                                                                                                                                                                                                 |
| `keyspace`             | Имя keyspace (базы данных).                                                                                                                                                                                                                                                                                                                    |
| `column_family`        | Имя column family (таблицы).                                                                                                                                                                                                                                                                                                                   |
| `allow_filtering`      | Флаг, разрешающий или запрещающий потенциально ресурсоёмкие условия для столбцов ключа кластеризации. Значение по умолчанию — `1`.                                                                                                                                                                                                             |
| `partition_key_prefix` | Количество столбцов ключа партиционирования в первичном ключе таблицы Cassandra. Обязательно для словарей с составным ключом. Порядок столбцов ключа в определении словаря должен совпадать с порядком в Cassandra. Значение по умолчанию — `1` (первый столбец ключа является ключом партиционирования, а остальные — ключами кластеризации). |
| `consistency`          | Уровень согласованности. Возможные значения: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Значение по умолчанию — `One`.                                                                                                                                                          |
| `where`                | Необязательные критерии выборки.                                                                                                                                                                                                                                                                                                               |
| `max_threads`          | Максимальное количество потоков, используемых для загрузки данных из нескольких партиций в словарях с составным ключом.                                                                                                                                                                                                                        |
| `query`                | Пользовательский запрос. Необязательное поле.                                                                                                                                                                                                                                                                                                  |

:::note
Поля `column_family` и `where` нельзя использовать вместе с полем `query`. При этом должно быть указано либо поле `column_family`, либо поле `query`.
:::