---
alias: []
description: 'Документация по формату PrettyCompactNoEscapes'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Отличается от формата [`PrettyCompact`](./PrettyCompact.md) тем, что в нём не используются [escape-последовательности ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
Это необходимо для отображения формата в браузере, а также для использования утилиты командной строки `watch`.

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />