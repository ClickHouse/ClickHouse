---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'Источник данных для словаря из PostgreSQL'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: 'Настройте PostgreSQL как источник данных для словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgresql-hostname'
        user 'postgres_user'
        password 'postgres_password'
        db 'db_name'
        table 'table_name'
        replica(host 'example01-1' port 5432 priority 1)
        replica(host 'example01-2' port 5432 priority 2)
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
      <postgresql>
          <host>postgresql-hostname</hoat>
          <port>5432</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </postgresql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Поля настроек:

| Настройка              | Описание                                                                                                                                      |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | Хост сервера PostgreSQL. Его можно указать для всех реплик или для каждой отдельно (внутри `<replica>`).                                      |
| `port`                 | Порт сервера PostgreSQL. Его можно указать для всех реплик или для каждой отдельно (внутри `<replica>`).                                      |
| `user`                 | Имя пользователя PostgreSQL. Его можно указать для всех реплик или для каждой отдельно (внутри `<replica>`).                                  |
| `password`             | Пароль пользователя PostgreSQL. Его можно указать для всех реплик или для каждой отдельно (внутри `<replica>`).                               |
| `replica`              | Раздел с конфигурацией реплик. Таких разделов может быть несколько.                                                                           |
| `replica/host`         | Хост PostgreSQL.                                                                                                                              |
| `replica/port`         | Порт PostgreSQL.                                                                                                                              |
| `replica/priority`     | Приоритет реплики. При попытке подключения ClickHouse перебирает реплики в порядке приоритета. Чем меньше число, тем выше приоритет.          |
| `db`                   | Имя базы данных.                                                                                                                              |
| `table`                | Имя таблицы.                                                                                                                                  |
| `where`                | Условие выборки. Синтаксис условий такой же, как у предложения `WHERE` в PostgreSQL. Например, `id > 10 AND id < 20`. Необязательно.          |
| `invalidate_query`     | Запрос для проверки состояния словаря. Необязательно. Подробнее см. в разделе [Обновление данных словаря с помощью LIFETIME](../lifetime.md). |
| `background_reconnect` | Повторно подключаться к реплике в фоновом режиме, если соединение прервалось. Необязательно.                                                  |
| `query`                | Пользовательский запрос. Необязательно.                                                                                                       |

:::note
Поля `table` и `where` нельзя использовать вместе с полем `query`. При этом должно быть указано либо поле `table`, либо поле `query`.
:::