---
alias: []
description: 'Документация для формата Vertical'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| Вход | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Выводит каждое значение на отдельной строке с указанием имени столбца. Этот формат удобен для вывода одной или нескольких строк, если каждая строка содержит большое количество столбцов.

Обратите внимание, что [`NULL`](/ru/sql-reference/syntax.md) выводится как `ᴺᵁᴸᴸ`, чтобы его было проще отличить от строкового значения `NULL` и отсутствия значения. JSON-столбцы будут выведены в удобочитаемом виде, а `NULL` — как `null`, поскольку это допустимое значение JSON, и его легко отличить от `"null"`.

<div id="example-usage">
  ## Пример использования
</div>

Пример:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

В формате Vertical строки не экранируются:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Этот формат подходит только для вывода результата запроса, но не для парсинга (получения данных для вставки в таблицу).

<div id="format-settings">
  ## Настройки формата
</div>
