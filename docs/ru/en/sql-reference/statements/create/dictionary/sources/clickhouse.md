---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'Источник словаря ClickHouse'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'Настройка таблицы ClickHouse в качестве источника словаря.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Поля настройки:

| Setting            | Description                                                                                                                                                                                                                                             |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`             | Хост ClickHouse. Если это локальный хост, запрос обрабатывается без сетевого взаимодействия. Для повышения отказоустойчивости можно создать таблицу [Distributed](/ru/engines/table-engines/special/distributed) и указать ее в последующих конфигурациях. |
| `port`             | Порт ClickHouse server.                                                                                                                                                                                                                                 |
| `user`             | Имя пользователя ClickHouse.                                                                                                                                                                                                                            |
| `password`         | Пароль пользователя ClickHouse.                                                                                                                                                                                                                         |
| `db`               | Имя базы данных.                                                                                                                                                                                                                                        |
| `table`            | Имя таблицы.                                                                                                                                                                                                                                            |
| `where`            | Условие выборки. Необязательно.                                                                                                                                                                                                                         |
| `invalidate_query` | Запрос для проверки состояния словаря. Необязательно. Подробнее см. в разделе [Обновление данных словаря с помощью LIFETIME](../lifetime.md).                                                                                                           |
| `secure`           | Использовать SSL для соединения.                                                                                                                                                                                                                        |
| `query`            | Пользовательский запрос. Необязательно.                                                                                                                                                                                                                 |

:::note
Поля `table` и `where` нельзя использовать вместе с полем `query`. При этом должно быть указано либо поле `table`, либо поле `query`.
:::