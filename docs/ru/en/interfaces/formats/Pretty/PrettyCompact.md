---
alias: []
description: 'Документация по формату PrettyCompact'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Ввод | Вывод | Алиас |
| ---- | ----- | ----- |
| ✗    | ✔     |       |

<div id="description">
  ## Описание
</div>

Отличается от формата [`Pretty`](./Pretty.md) тем, что таблица отображается с сеткой между строками.
За счёт этого результат получается более компактным.

:::note
Этот формат по умолчанию используется в клиенте командной строки в интерактивном режиме.
:::

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />