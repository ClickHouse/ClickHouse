---
alias: []
description: 'Документация по формату PrettyNoEscapes'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Отличается от [Pretty](/ru/interfaces/formats/Pretty) тем, что в нём не используются [ANSI-последовательности экранирования](http://en.wikipedia.org/wiki/ANSI_escape_code).
Это необходимо для отображения формата в браузере, а также для работы с утилитой командной строки &#39;watch&#39;.

<div id="example-usage">
  ## Пример использования
</div>

Пример:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
[HTTP-интерфейс](/ru/interfaces/http) можно использовать для просмотра этого формата в браузере.
:::

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />