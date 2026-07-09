---
alias: []
description: 'Документация по формату Pretty'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `Pretty` выводит данные в виде таблиц с Unicode-оформлением,
используя ANSI-escape-последовательности для отображения цветов в терминале.
Отрисовывается полная сетка таблицы, и каждая строка занимает в терминале две строки.
Каждый блок результатов выводится как отдельная таблица.
Это необходимо, чтобы блоки можно было выводить без буферизации результатов (буферизация потребовалась бы для предварительного вычисления видимой ширины всех значений).

[NULL](/ru/sql-reference/syntax.md) выводится как `ᴺᵁᴸᴸ`.

<div id="example-usage">
  ## Пример использования
</div>

Пример (для формата [`PrettyCompact`](./PrettyCompact.md)):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Строки не экранируются ни в одном из форматов `Pretty`. Ниже приведён пример для формата [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Чтобы не выводить в терминал слишком много данных, печатаются только первые `10,000` строк.
Если число строк больше или равно `10,000`, выводится сообщение &quot;Показаны первые 10 000&quot;.

:::note
Этот формат подходит только для вывода результата запроса, но не для парсинга данных.
:::

Формат Pretty поддерживает вывод итоговых значений (при использовании `WITH TOTALS`) и экстремумов (когда &#39;extremes&#39; установлено в 1).
В этих случаях итоговые и экстремальные значения выводятся после основных данных в отдельных таблицах.
Это показано в следующем примере, где используется формат [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## Настройки формата
</div>

<PrettyFormatSettings />