---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'Локальный файл как источник словаря'
sidebar_position: 2
sidebar_label: 'Local File'
description: 'Настройте локальный файл как источник словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Локальный файловый источник загружает данные словаря из файла в локальной файловой системе. Это удобно для небольших статических таблиц соответствий, которые можно хранить в виде плоских файлов в форматах TSV, CSV или любом другом [поддерживаемом формате](/ru/sql-reference/formats).

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Поля настройки:

| Настройка | Описание                                                                                 |
| --------- | ---------------------------------------------------------------------------------------- |
| `path`    | Абсолютный путь к файлу.                                                                 |
| `format`  | Формат файла. Поддерживаются все форматы, описанные в [Formats](/ru/sql-reference/formats). |

Если словарь с источником `FILE` создаётся DDL-командой (`CREATE DICTIONARY ...`), файл-источник должен находиться в каталоге `user_files`, чтобы пользователи базы данных не могли получать доступ к произвольным файлам на узле ClickHouse.

**См. также**

* [Функция dictionary](/ru/sql-reference/table-functions/dictionary)