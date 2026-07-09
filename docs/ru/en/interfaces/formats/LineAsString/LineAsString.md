---
alias: []
description: 'Документация по формату LineAsString'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: 'reference'
---

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `LineAsString` интерпретирует каждую строку входных данных как одно строковое значение.
Этот формат можно использовать только для таблицы с единственным полем типа [String](/ru/sql-reference/data-types/string.md).
Остальные столбцы должны быть заданы как [`DEFAULT`](/ru/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/ru/sql-reference/statements/create/view#materialized-view) или опущены.

<div id="example-usage">
  ## Пример использования
</div>

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

<div id="format-settings">
  ## Настройки формата
</div>
