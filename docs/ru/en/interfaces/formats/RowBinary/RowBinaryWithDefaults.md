---
alias: []
description: 'Документация по формату RowBinaryWithDefaults'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✗     |           |

<div id="description">
  ## Описание
</div>

Подобно формату [`RowBinary`](./RowBinary.md), но с дополнительным байтом перед каждым столбцом, который указывает, следует ли использовать значение по умолчанию.

<div id="example-usage">
  ## Пример использования
</div>

Примеры:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* Для столбца `x` есть только один байт `01`, который указывает, что следует использовать значение по умолчанию, и после этого байта никаких других данных не передаётся.
* Для столбца `y` данные начинаются с байта `00`, который указывает, что столбец содержит фактическое значение, которое следует считывать из последующих данных `01000000`.

<div id="format-settings">
  ## Настройки формата
</div>

<RowBinaryFormatSettings />