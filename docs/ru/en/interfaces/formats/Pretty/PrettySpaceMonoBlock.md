---
alias: []
description: 'Документация по формату PrettySpaceMonoBlock'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Отличается от формата [`PrettySpace`](./PrettySpace.md) тем, что до `10,000` строк накапливаются в буфере,
а затем выводятся как одна таблица, а не [блоками](/ru/development/architecture#block).

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />