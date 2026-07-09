---
alias: []
description: 'Документация по формату Regexp'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✗     |           |

<div id="description">
  ## Описание
</div>

Формат `Regex` разбирает каждую строку импортируемых данных в соответствии с указанным регулярным выражением.

**Использование**

Регулярное выражение из настройки [format&#95;regexp](/ru/operations/settings/settings-formats.md/#format_regexp) применяется к каждой строке импортируемых данных. Количество подшаблонов в регулярном выражении должно совпадать с количеством столбцов в импортируемых данных.

Строки импортируемых данных должны разделяться символом новой строки `'\n'` или последовательностью новой строки в стиле DOS `"\r\n"`.

Содержимое каждого совпавшего подшаблона разбирается с помощью метода соответствующего типа данных в соответствии с настройкой [format&#95;regexp&#95;escaping&#95;rule](/ru/operations/settings/settings-formats.md/#format_regexp_escaping_rule).

Если регулярное выражение не совпадает со строкой и [format&#95;regexp&#95;skip&#95;unmatched](/ru/operations/settings/settings-formats.md/#format_regexp_escaping_rule) установлена в 1, строка молча пропускается. В противном случае генерируется исключение.

<div id="example-usage">
  ## Пример использования
</div>

Рассмотрим файл `data.tsv`:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

и таблицу `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Мы вставим данные из упомянутого выше файла в приведённую выше таблицу с помощью следующего запроса:

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Теперь мы можем выполнить `SELECT` из таблицы, чтобы увидеть, как формат `Regex` разобрал данные из файла:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## Настройки формата
</div>

При работе с форматом `Regexp` можно использовать следующие настройки:

* `format_regexp` — [String](/ru/sql-reference/data-types/string.md). Содержит регулярное выражение в формате [re2](https://github.com/google/re2/wiki/Syntax).

* `format_regexp_escaping_rule` — [String](/ru/sql-reference/data-types/string.md). Поддерживаются следующие правила экранирования:

  * CSV (аналогично [CSV](/ru/interfaces/formats/CSV)
  * JSON (аналогично [JSONEachRow](/ru/interfaces/formats/JSONEachRow)
  * Escaped (аналогично [TSV](/ru/interfaces/formats/TabSeparated)
  * Quoted (аналогично [Values](/ru/interfaces/formats/Values)
  * Raw (извлекает подшаблоны целиком, без правил экранирования, аналогично [TSVRaw](/ru/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/ru/sql-reference/data-types/int-uint.md). Определяет, нужно ли сгенерировать исключение, если выражение `format_regexp` не совпадает с импортируемыми данными. Можно установить значение `0` или `1`.