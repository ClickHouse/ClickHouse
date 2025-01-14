---
title : Regexp
slug : /en/interfaces/formats/Regexp
keywords : [Regexp]
---

## Description

Each line of imported data is parsed according to the regular expression.

When working with the `Regexp` format, you can use the following settings:

- `format_regexp` — [String](/docs/en/sql-reference/data-types/string.md). Contains regular expression in the [re2](https://github.com/google/re2/wiki/Syntax) format.

- `format_regexp_escaping_rule` — [String](/docs/en/sql-reference/data-types/string.md). The following escaping rules are supported:

    - CSV (similarly to [CSV](/docs/en/interfaces/formats/CSV)
    - JSON (similarly to [JSONEachRow](/docs/en/interfaces/formats/JSONEachRow)
    - Escaped (similarly to [TSV](/docs/en/interfaces/formats/TabSeparated)
    - Quoted (similarly to [Values](/docs/en/interfaces/formats/Values)
    - Raw (extracts subpatterns as a whole, no escaping rules, similarly to [TSVRaw](/docs/en/interfaces/formats/TabSeparated)

- `format_regexp_skip_unmatched` — [UInt8](/docs/en/sql-reference/data-types/int-uint.md). Defines the need to throw an exception in case the `format_regexp` expression does not match the imported data. Can be set to `0` or `1`.

**Usage**

The regular expression from [format_regexp](/docs/en/operations/settings/settings-formats.md/#format_regexp) setting is applied to every line of imported data. The number of subpatterns in the regular expression must be equal to the number of columns in imported dataset.

Lines of the imported data must be separated by newline character `'\n'` or DOS-style newline `"\r\n"`.

The content of every matched subpattern is parsed with the method of corresponding data type, according to [format_regexp_escaping_rule](/docs/en/operations/settings/settings-formats.md/#format_regexp_escaping_rule) setting.

If the regular expression does not match the line and [format_regexp_skip_unmatched](/docs/en/operations/settings/settings-formats.md/#format_regexp_escaping_rule) is set to 1, the line is silently skipped. Otherwise, exception is thrown.

## Example Usage

**Example**

Consider the file data.tsv:

```text
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```
and the table:

```sql
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Import command:

```bash
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Query:

```sql
SELECT * FROM imp_regex_table;
```

Result:

```text
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

## Format Settings