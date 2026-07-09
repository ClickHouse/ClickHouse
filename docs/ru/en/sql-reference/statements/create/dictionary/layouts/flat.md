---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'плоская структура словаря'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Хранение словаря в памяти в виде плоских массивов.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

При структуре `flat` словарь полностью хранится в памяти в виде плоских массивов.
Объём используемой памяти пропорционален значению наибольшего ключа (по занимаемому пространству).

:::tip
Этот тип структуры обеспечивает наилучшую производительность среди всех доступных способов хранения словаря.
:::

Ключ словаря имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md), а значение ключа ограничено `max_array_size` (по умолчанию — 500,000).
Если при создании словаря обнаруживается ключ с бо́льшим значением, ClickHouse генерирует исключение и не создаёт словарь.
Начальный размер плоских массивов словаря задаётся настройкой `initial_array_size` (по умолчанию — 1024).

Поддерживаются все типы источников данных.
При обновлении словаря данные (из файла или из таблицы) считываются целиком.

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />