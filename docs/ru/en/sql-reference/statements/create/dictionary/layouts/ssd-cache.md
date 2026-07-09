---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'Типы структур словаря ssd_cache'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'Храните данные словаря на SSD, а индекс — в памяти: типы ssd_cache и complex_key_ssd_cache'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

Аналогично `cache`, но данные хранятся на SSD, а индекс — в оперативной памяти. Все настройки словаря `cache`, связанные с очередью обновления, также применимы к словарям с кэшем на SSD.

Ключ словаря имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md).

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
        <ssd_cache>
            <!-- Размер элементарного блока чтения в байтах. Рекомендуется, чтобы он был равен размеру страницы SSD. -->
            <block_size>4096</block_size>
            <!-- Максимальный размер файла кэша в байтах. -->
            <file_size>16777216</file_size>
            <!-- Размер буфера оперативной памяти в байтах для чтения элементов с SSD. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- Размер буфера оперативной памяти в байтах для накопления элементов перед записью на SSD. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- Путь, где будет храниться файл кэша. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

Этот тип хранилища используется с составными [ключами](../attributes.md#composite-key). Аналогичен `ssd_cache`.