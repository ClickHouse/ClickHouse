---
description: 'Документация по файлам'
sidebar_label: 'Файлы'
slug: /sql-reference/functions/files
title: 'Файлы'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

Считывает файл как строку и загружает данные в указанный столбец. Содержимое файла не интерпретируется.

См. также табличную функцию [file](../table-functions/file.md).

**Синтаксис**

```sql
file(path[, default])
```

**Аргументы**

* `path` — Путь к файлу относительно [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path). Поддерживает подстановочные шаблоны `*`, `**`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, а `'abc', 'def'` — строки.
* `default` — Значение, которое возвращается, если файл не существует или недоступен. Поддерживаемые типы данных: [String](../data-types/string.md) и [NULL](/ru/operations/settings/formats#input_format_null_as_default).

**Пример**

Вставка данных из файлов a.txt и b.txt в таблицу как строк:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```