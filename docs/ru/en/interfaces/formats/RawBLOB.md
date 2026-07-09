---
description: 'Документация по формату RawBLOB'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## Описание
</div>

Форматы `RawBLOB` считывают все входные данные в одно значение. Поддерживается разбор только таблицы с одним полем типа [`String`](/ru/sql-reference/data-types/string.md) или схожего типа.
Результат выводится в бинарном формате без разделителей и экранирования. Если выводится более одного значения, формат становится неоднозначным, и снова прочитать эти данные будет невозможно.

<div id="raw-formats-comparison">
  ### Сравнение форматов Raw
</div>

Ниже приведено сравнение форматов `RawBLOB` и [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:

* данные выводятся в бинарном формате, без экранирования;
* между значениями нет разделителей;
* в конце каждого значения отсутствует символ перевода строки.

`TabSeparatedRaw`:

* данные выводятся без экранирования;
* строки содержат значения, разделённые символами табуляции;
* после последнего значения в каждой строке идёт символ перевода строки.

Ниже приведено сравнение форматов `RawBLOB` и [RowBinary](./RowBinary/RowBinary.md).

`RawBLOB`:

* строковые поля выводятся без префикса длины.

`RowBinary`:

* строковые поля представлены длиной в формате varint (беззнаковый [LEB128](https://en.wikipedia.org/wiki/LEB128)), за которой следуют байты строки.

Если на вход `RawBLOB` передаются пустые данные, ClickHouse генерирует исключение:

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## Пример использования
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## Настройки формата
</div>
