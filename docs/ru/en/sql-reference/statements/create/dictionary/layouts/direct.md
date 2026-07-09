---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'структура словаря direct'
sidebar_label: 'direct'
sidebar_position: 9
description: 'Структура словаря, при которой данные запрашиваются напрямую из источника без кэширования.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

Словарь не хранится в памяти и при обработке запроса обращается непосредственно к источнику.

Ключ словаря имеет тип [UInt64](/ru/sql-reference/data-types/int-uint.md).

Поддерживаются все типы [источников](../sources/#dictionary-sources), кроме локальных файлов.

Пример конфигурации:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

Этот тип хранилища используется с составными [ключами](../attributes.md#composite-key). Аналогичен `direct`.