---
toc_title: INTO OUTFILE
---

# Секция INTO OUTFILE  {#into-outfile-clause}

Секция `INTO OUTFILE` перенаправляет результат запроса `SELECT` в файл на стороне **клиента**.

Поддерживаются сжатые файлы. Формат сжатия определяется по расширению файла (по умолчанию используется режим `'auto'`), либо он может быть задан явно в секции `COMPRESSION`. 

**Синтаксис**

```sql
SELECT <expr_list> INTO OUTFILE file_name [COMPRESSION type]
```

`file_name` и `type` задаются в виде строковых литералов. Поддерживаются форматы сжатия: `'none`', `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

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
