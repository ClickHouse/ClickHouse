---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'Источник словаря HTTP(S)'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'Настройте конечную точку HTTP или HTTPS в качестве источника словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Работа с HTTP(S)-сервером зависит от [способа хранения словаря в памяти](../layouts/). Если словарь использует `cache` или `complex_key_cache`, ClickHouse запрашивает необходимые ключи, отправляя запрос методом `POST`.

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Конфигурационный файл">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Чтобы ClickHouse мог получить доступ к ресурсу HTTPS, необходимо [настроить openSSL](/ru/operations/server-configuration-parameters/settings#openssl) в конфигурации сервера.

Поля настройки:

| Setting       | Description                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------ |
| `url`         | URL источника.                                                                                   |
| `format`      | Формат файла. Поддерживаются все форматы, описанные в разделе [Formats](/ru/sql-reference/formats). |
| `credentials` | Basic HTTP-аутентификация. Необязательно.                                                        |
| `user`        | Имя пользователя, необходимое для аутентификации.                                                |
| `password`    | Пароль, необходимый для аутентификации.                                                          |
| `headers`     | Все пользовательские HTTP-заголовки, используемые в HTTP-запросе. Необязательно.                 |
| `header`      | Один HTTP-заголовок.                                                                             |
| `name`        | Имя заголовка, отправляемого в запросе.                                                          |
| `value`       | Значение, заданное для указанного имени заголовка.                                               |

При создании словаря с помощью команды DDL (`CREATE DICTIONARY ...`) удалённые хосты для HTTP-словарей сверяются с содержимым секции `remote_url_allow_hosts` в конфигурации, чтобы пользователи базы данных не могли получать доступ к произвольным HTTP-серверам.