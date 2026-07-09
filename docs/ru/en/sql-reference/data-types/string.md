---
description: 'Документация по типу данных String в ClickHouse'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

Строки произвольной длины. Длина не ограничена. Значение может содержать произвольный набор байтов, включая null-байты.
Тип String заменяет типы VARCHAR, BLOB, CLOB и другие типы из других СУБД.

При создании таблиц для строковых полей можно задавать числовые параметры (например, `VARCHAR(255)`), но ClickHouse их игнорирует.

Псевдонимы:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## Кодировки
</div>

В ClickHouse нет понятия кодировок. Строки могут содержать произвольный набор байтов, которые хранятся и выводятся как есть.
Если вам нужно хранить тексты, мы рекомендуем использовать кодировку UTF-8. По крайней мере, если ваш терминал использует UTF-8 (что и рекомендуется), вы сможете читать и записывать свои значения без преобразований.
Аналогично, некоторые функции для работы со строками имеют отдельные варианты, которые работают в предположении, что строка содержит набор байтов, представляющих собой текст в кодировке UTF-8.
Например, функция [length](/ru/sql-reference/functions/array-functions#length) вычисляет длину строки в байтах, а функция [lengthUTF8](../functions/string-functions.md#lengthUTF8) — длину строки в кодовых точках Unicode, предполагая, что значение закодировано в UTF-8.