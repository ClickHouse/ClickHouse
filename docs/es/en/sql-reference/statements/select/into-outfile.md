---
description: 'Documentación de la cláusula INTO OUTFILE'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'Cláusula INTO OUTFILE'
doc_type: 'reference'
---

La cláusula `INTO OUTFILE` redirige el resultado de una consulta `SELECT` a un archivo en el **Client**.

Se admiten archivos comprimidos. El tipo de compresión se detecta según la extensión del nombre del archivo (de forma predeterminada, se usa el modo `'auto'`). También puede especificarse explícitamente en una cláusula `COMPRESSION`. El nivel de compresión para un tipo de compresión determinado puede especificarse en una cláusula `LEVEL`.

**Sintaxis**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` y `type` son literales de cadena. Los tipos de compresión admitidos son: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level` es un literal numérico. Se admiten enteros positivos en los siguientes rangos: `1-12` para el tipo `lz4`, `1-22` para el tipo `zstd` y `1-9` para los demás tipos de compresión.

<div id="implementation-details">
  ## Detalles de implementación
</div>

* Esta funcionalidad está disponible en el [Client de línea de comandos](../../../interfaces/client.md) y en [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Por lo tanto, una consulta enviada a través de la [interfaz HTTP](/es/interfaces/http) fallará.
* La consulta fallará si ya existe un archivo con el mismo nombre.
* El [formato de salida](../../../interfaces/formats.md) predeterminado es `TabSeparated` (como en el modo por lotes del Client de línea de comandos). Use la cláusula [FORMAT](format.md) para cambiarlo.
* Si en la consulta se especifica `AND STDOUT`, la salida que se escribe en el archivo también se muestra en la salida estándar. Si se usa compresión, el texto sin cifrar se muestra en la salida estándar.
* Si en la consulta se especifica `APPEND`, la salida se agrega a un archivo existente. Si se usa compresión, no se puede usar `APPEND`.
* Al escribir en un archivo que ya existe, se debe usar `APPEND` o `TRUNCATE`.

**Ejemplo**

Ejecute la siguiente consulta con el [Client de línea de comandos](../../../interfaces/client.md):

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```