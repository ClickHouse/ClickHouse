---
alias: []
description: 'Документация по формату One'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✗     |           |

<div id="description">
  ## Описание
</div>

Формат `One` — это специальный input format, который не читает данные из файла и возвращает только одну строку со столбцом типа [`UInt8`](../../sql-reference/data-types/int-uint.md) с именем `dummy` и значением `0` (как в таблице `system.one`).
Может использоваться с виртуальными столбцами `_file/_path`, чтобы вывести список всех файлов без чтения самих данных.

<div id="example-usage">
  ## Пример использования
</div>

Пример:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## Настройки формата
</div>
