---
alias: []
description: 'Документация для формата PrettyMonoBlock'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Вход | Выход | Алиас |
| ---- | ----- | ----- |
| ✗    | ✔     |       |

<div id="description">
  ## Описание
</div>

Отличается от формата [`Pretty`](/ru/interfaces/formats/Pretty) тем, что буферизуется до `10,000` строк,
которые затем выводятся как одна таблица, а не [блоками](/ru/development/architecture#block).

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />