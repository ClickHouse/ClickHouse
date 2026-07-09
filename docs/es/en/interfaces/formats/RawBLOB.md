---
description: 'Documentación sobre el formato RawBLOB'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## Descripción
</div>

Los formatos `RawBLOB` leen todos los datos de entrada como un único valor. Solo se puede analizar una tabla con un único campo de tipo [`String`](/es/sql-reference/data-types/string.md) o similar.
El resultado se genera en formato binario, sin delimitadores ni secuencias de escape. Si se genera más de un valor, el formato es ambiguo y será imposible volver a leer los datos.

<div id="raw-formats-comparison">
  ### Comparación de formatos Raw
</div>

A continuación se muestra una comparación de los formatos `RawBLOB` y [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:

* los datos se generan en formato binario, sin escape;
* no hay delimitadores entre los valores;
* no hay salto de línea al final de cada valor.

`TabSeparatedRaw`:

* los datos se generan sin escape;
* las filas contienen valores separados por tabulaciones;
* hay un salto de línea después del último valor de cada fila.

A continuación se muestra una comparación de los formatos `RawBLOB` y [RowBinary](./RowBinary/RowBinary.md).

`RawBLOB`:

* los campos de tipo String se generan sin prefijo de longitud.

`RowBinary`:

* los campos de tipo String se representan como una longitud en formato varint ([LEB128] sin signo (https://en.wikipedia.org/wiki/LEB128)), seguida de los bytes de la cadena.

Cuando se pasan datos vacíos a la entrada `RawBLOB`, ClickHouse lanza una excepción:

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## Ejemplo de uso
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
  ## Configuración de formato
</div>
