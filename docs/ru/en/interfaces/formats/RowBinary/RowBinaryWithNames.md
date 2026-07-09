---
description: 'Документация по формату RowBinaryWithNames'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Аналогично формату [`RowBinary`](./RowBinary.md), но с добавленным заголовком:

* Количество столбцов (N), закодированное с помощью [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* N значений `String`, задающих имена столбцов.

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<RowBinaryFormatSettings />

:::note

* Если параметр [`input_format_with_names_use_header`](/ru/operations/settings/settings-formats.md/#input_format_with_names_use_header) установлен в `1`, столбцы входных данных будут сопоставлены со столбцами таблицы по именам, а столбцы с неизвестными именами будут пропущены.
* Если параметр [`input_format_skip_unknown_fields`](/ru/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) установлен в `1`.
  В противном случае первая строка будет пропущена.
  :::