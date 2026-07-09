---
description: 'Документация по предложению INTO OUTFILE'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'Предложение INTO OUTFILE'
doc_type: 'reference'
---

Предложение `INTO OUTFILE` перенаправляет результат запроса `SELECT` в файл на стороне **клиента**.

Сжатые файлы поддерживаются. Тип сжатия определяется по расширению имени файла (по умолчанию используется режим `'auto'`). Его также можно явно указать в предложении `COMPRESSION`. Уровень сжатия для конкретного типа сжатия можно указать в предложении `LEVEL`.

**Синтаксис**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` и `type` — строковые литералы. Поддерживаются следующие типы сжатия: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level` — числовой литерал. Поддерживаются положительные целые числа в следующих диапазонах: `1-12` для типа `lz4`, `1-22` для типа `zstd` и `1-9` для остальных типов сжатия.

<div id="implementation-details">
  ## Подробности реализации
</div>

* Эта функциональность доступна в [клиенте командной строки](../../../interfaces/client.md) и [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Поэтому запрос, отправленный через [HTTP interface](/ru/interfaces/http), завершится ошибкой.
* Запрос завершится ошибкой, если файл с таким именем уже существует.
* Формат [вывода](../../../interfaces/formats.md) по умолчанию — `TabSeparated` (как в пакетном режиме клиента командной строки). Чтобы изменить его, используйте предложение [FORMAT](format.md).
* Если в запросе указано `AND STDOUT`, то вывод, записываемый в файл, также отображается в стандартном выводе. При использовании сжатия в стандартный вывод выводится несжатый текст.
* Если в запросе указано `APPEND`, то вывод дописывается в существующий файл. Если используется сжатие, `APPEND` использовать нельзя.
* При записи в уже существующий файл необходимо использовать `APPEND` или `TRUNCATE`.

**Пример**

Выполните следующий запрос с помощью [клиента командной строки](../../../interfaces/client.md):

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```