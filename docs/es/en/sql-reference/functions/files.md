---
description: 'Documentación sobre archivos'
sidebar_label: 'Archivos'
slug: /sql-reference/functions/files
title: 'Archivos'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

Lee un archivo como cadena y carga los datos en la columna especificada. El contenido del archivo no se interpreta.

Véase también la función de tabla [file](../table-functions/file.md).

**Sintaxis**

```sql
file(path[, default])
```

**Argumentos**

* `path` — La ruta del archivo relativa a [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path). Admite comodines `*`, `**`, `?`, `{abc,def}` y `{N..M}`, donde `N` y `M` son números y `'abc'` y `'def'` son cadenas.
* `default` — El valor que se devuelve si el archivo no existe o no se puede acceder a él. Tipos de datos compatibles: [String](../data-types/string.md) y [NULL](/es/operations/settings/formats#input_format_null_as_default).

**Ejemplo**

Inserción de datos de los archivos a.txt y b.txt en una tabla como cadenas:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```