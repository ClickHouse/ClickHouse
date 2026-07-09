---
alias: []
description: 'Документация по формату LineAsStringWithNames'
input_format: false
keywords: ['LineAsStringWithNames']
output_format: true
slug: /interfaces/formats/LineAsStringWithNames
title: 'LineAsStringWithNames'
doc_type: 'Справочник'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `LineAsStringWithNames` похож на формат [`LineAsString`](./LineAsString.md), но также выводит строку заголовка с именами столбцов.

<div id="example-usage">
  ## Пример использования
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## Настройки формата
</div>
