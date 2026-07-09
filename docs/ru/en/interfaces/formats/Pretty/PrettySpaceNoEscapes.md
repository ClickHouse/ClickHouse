---
alias: []
description: 'Документация по формату PrettySpaceNoEscapes'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Отличается от формата [`PrettySpace`](./PrettySpace.md) тем, что [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) не используются.
Это необходимо для отображения данного формата в браузере, а также для использования утилиты командной строки `watch`.

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />