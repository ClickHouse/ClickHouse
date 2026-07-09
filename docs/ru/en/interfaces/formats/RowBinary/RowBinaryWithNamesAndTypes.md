---
alias: []
description: 'Документация по формату RowBinaryWithNamesAndTypes'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'справочник'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Аналогично формату [RowBinary](./RowBinary.md), но с добавленным заголовком:

* Число столбцов (N), закодированное с помощью [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* N строк `String`, содержащих имена столбцов.
* N строк `String`, содержащих типы столбцов.

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<RowBinaryFormatSettings />

:::note
Если для настройки [`input_format_with_names_use_header`](/ru/operations/settings/settings-formats.md/#input_format_with_names_use_header) установлено значение 1,
столбцы входных данных будут сопоставлены со столбцами таблицы по именам, а столбцы с неизвестными именами будут пропущены, если для настройки [input&#95;format&#95;skip&#95;unknown&#95;fields](/ru/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) установлено значение 1.
В противном случае первая строка будет пропущена.
Если для настройки [`input_format_with_types_use_header`](/ru/operations/settings/settings-formats.md/#input_format_with_types_use_header) установлено значение `1`,
типы из входных данных будут сравниваться с типами соответствующих столбцов таблицы. В противном случае вторая строка будет пропущена.
:::