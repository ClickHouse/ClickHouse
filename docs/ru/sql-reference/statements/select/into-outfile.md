---
slug: /ru/sql-reference/statements/select/into-outfile
sidebar_label: INTO OUTFILE
---

# Секция INTO OUTFILE  {#into-outfile-clause}

Секция `INTO OUTFILE` перенаправляет результат запроса `SELECT` в файл на стороне **клиента**.

Поддерживаются сжатые файлы. Формат сжатия определяется по расширению файла (по умолчанию используется режим `'auto'`), либо он может быть задан явно в секции `COMPRESSION`. Уровень сжатия для конкретного алгоритма может быть задан в секции `LEVEL`.

**Синтаксис**

```sql
SELECT <expr_list> INTO OUTFILE file_name [COMPRESSION type [LEVEL level]]
```

`file_name` и `type` задаются в виде строковых литералов. Поддерживаются форматы сжатия: `'none`', `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level` задается в виде числового литерала. Поддерживаются положительные значения в следующих диапазонах: `1-12` для формата `lz4`, `1-22` для формата `zstd` и `1-9` для остальных форматов.

## Детали реализации {#implementation-details}

-   Эта функция доступна только в следующих интерфейсах: [клиент командной строки](../../../interfaces/cli.md) и [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Таким образом, запрос, отправленный через [HTTP интерфейс](../../../interfaces/http.md) вернет ошибку.
-   Запрос завершится ошибкой, если файл с тем же именем уже существует.
-   По умолчанию используется [выходной формат](../../../interfaces/formats.md) `TabSeparated` (как в пакетном режиме клиента командной строки). Его можно изменить в секции [FORMAT](format.md).

**Пример**

Выполните следующий запрос, используя [клиент командной строки](../../../interfaces/cli.md):

```bash
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

Результат:

```text
1,"ABC"
```
