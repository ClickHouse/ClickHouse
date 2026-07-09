---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'типы hashed-структур словарей'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'Хранение словаря в памяти с помощью хеш-таблиц: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

Словарь полностью хранится в памяти в виде хеш-таблицы. Словарь может содержать любое количество элементов с любыми идентификаторами. На практике количество ключей может достигать десятков миллионов.

Ключ словаря имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md).

Поддерживаются все типы источников. При обновлении данные (из файла или таблицы) считываются целиком.

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Пример конфигурации с настройками:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <hashed>
        <!-- Если shards больше 1 (по умолчанию — `1`), словарь будет загружать
             данные параллельно. Это полезно, если один
             словарь содержит очень много элементов. -->
        <shards>10</shards>

        <!-- Размер очереди ожидания для блоков при параллельной загрузке.

             Поскольку узким местом при параллельной загрузке является рехеширование,
             чтобы избежать простоев, когда поток занят рехешированием,
             нужен некоторый запас очереди.

             10000 — хороший баланс между использованием памяти и скоростью.
             Этого достаточно даже для 10e10 элементов и позволяет обрабатывать всю нагрузку без простоев. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- Максимальный коэффициент заполнения хеш-таблицы; при более высоких значениях
             память используется эффективнее (меньше памяти расходуется впустую), но производительность чтения
             может снизиться.

             Допустимые значения: [0.5, 0.99]
             Значение по умолчанию: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

Аналогичен `hashed`, но потребляет меньше памяти за счёт более высокой нагрузки на CPU.

Ключ словаря имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md).

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Для словарей этого типа также можно использовать `shards`, причём для `sparse_hashed` это даже важнее, чем для `hashed`, поскольку `sparse_hashed` работает медленнее.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

Этот тип хранилища предназначен для использования с составными [ключами](../attributes.md#composite-key). Аналогичен `hashed`.

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

Этот тип хранилища используется с составными [ключами](../attributes.md#composite-key). Аналогичен [sparse&#95;hashed](#sparse_hashed).

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />