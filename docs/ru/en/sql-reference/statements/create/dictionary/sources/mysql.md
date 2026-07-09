---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'Источник словаря MySQL'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'Настройка MySQL в качестве источника словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Поля настроек:

| Настройка                 | Описание                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | Порт на сервере MySQL. Его можно указать для всех реплик или отдельно для каждой из них (внутри `<replica>`).                                                                                                                                                                                                                                                  |
| `user`                    | Имя пользователя MySQL. Его можно указать для всех реплик или отдельно для каждой из них (внутри `<replica>`).                                                                                                                                                                                                                                                 |
| `password`                | Пароль пользователя MySQL. Его можно указать для всех реплик или отдельно для каждой из них (внутри `<replica>`).                                                                                                                                                                                                                                              |
| `replica`                 | Раздел с конфигурацией реплик. Таких разделов может быть несколько.                                                                                                                                                                                                                                                                                            |
| `replica/host`            | Хост MySQL.                                                                                                                                                                                                                                                                                                                                                    |
| `replica/priority`        | Приоритет реплики. При попытке подключения ClickHouse перебирает реплики в порядке приоритета. Чем меньше число, тем выше приоритет.                                                                                                                                                                                                                           |
| `db`                      | Имя базы данных.                                                                                                                                                                                                                                                                                                                                               |
| `table`                   | Имя таблицы.                                                                                                                                                                                                                                                                                                                                                   |
| `where`                   | Критерий отбора. Синтаксис условий такой же, как в предложении `WHERE` в MySQL, например `id > 10 AND id < 20`. Необязательно.                                                                                                                                                                                                                                 |
| `invalidate_query`        | Запрос для проверки состояния словаря. Необязательно. Подробнее см. в разделе [Refreshing dictionary data using LIFETIME](../lifetime.md).                                                                                                                                                                                                                     |
| `fail_on_connection_loss` | Управляет поведением сервера при потере соединения. Если `true`, при потере соединения между клиентом и сервером немедленно генерируется исключение. Если `false`, сервер как минимум трижды пытается снова получить данные, прежде чем сообщить об ошибке. Обратите внимание, что повторные попытки увеличивают время ответа. Значение по умолчанию: `false`. |
| `query`                   | Пользовательский запрос. Необязательно.                                                                                                                                                                                                                                                                                                                        |
| `enable_compression`      | Включает zlib-сжатие для соединения по протоколу MySQL. Если установлено значение `1`, ClickHouse запрашивает у сервера MySQL сжатие на уровне протокола. Также может быть задано отдельно для каждой реплики внутри `<replica>`. Значение по умолчанию: `0`.                                                                                                  |

:::note
Поля `table` и `where` нельзя использовать вместе с полем `query`. При этом должно быть указано либо поле `table`, либо `query`.
:::

:::note
Явного параметра `secure` нет. При установлении SSL-соединения защита обязательна.
:::

К MySQL на локальном хосте можно подключаться через сокеты. Для этого задайте `host` и `socket`.

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>