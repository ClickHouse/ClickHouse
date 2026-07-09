---
description: 'Proporciona acceso al sistema de archivos para listar archivos y devolver sus metadatos y contenido.'
sidebar_label: 'filesystem'
sidebar_position: 62
slug: /sql-reference/table-functions/filesystem
title: 'filesystem'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="filesystem-table-function">
  # Función de tabla filesystem
</div>

<CloudNotSupportedBadge />

Recorre recursivamente un directorio y devuelve una tabla con los metadatos de los archivos (rutas, tamaños, tipos, permisos y fechas de modificación) y, opcionalmente, su contenido.

En el modo `clickhouse-server`, la ruta debe estar dentro del directorio [user&#95;files&#95;path](/es/operations/server-configuration-parameters/settings.md#user_files_path). Se siguen los enlaces simbólicos dentro de `user_files_path` que apuntan fuera de este, pero solo se devuelven las entradas cuya ruta (a través del enlace simbólico) comienza con `user_files_path`.

En el modo `clickhouse-local`, no hay restricciones de ruta.

<div id="syntax">
  ## Sintaxis
</div>

```sql
filesystem([path])
```

<div id="arguments">
  ## Argumentos
</div>

| Parámetro | Descripción                                                                                                                                                                                                                                  |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`    | El directorio que se va a listar. Puede ser una ruta absoluta (debe estar dentro de `user_files_path` en modo servidor) o una ruta relativa a `user_files_path`. Si está vacío o se omite, se usa `user_files_path` de forma predeterminada. |

<div id="returned_columns">
  ## Columnas devueltas
</div>

| Columna             | Tipo                       | Descripción                                                                                                                                                                                                                                                                      |
| ------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`              | `String`                   | Directorio que contiene la entrada (no incluye el nombre del archivo o directorio en sí).                                                                                                                                                                                        |
| `name`              | `String`                   | Nombre del archivo o directorio (el último componente de la ruta).                                                                                                                                                                                                               |
| `file`              | `String` (ALIAS de `name`) | Alias de la columna `name`.                                                                                                                                                                                                                                                      |
| `type`              | `Enum8`                    | Tipo de archivo: `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`.                                                                                                                                  |
| `size`              | `Nullable(UInt64)`         | Tamaño del archivo en bytes (para archivos regulares). `NULL` para archivos no regulares (directorios, enlaces simbólicos, etc.) y en caso de error.                                                                                                                             |
| `depth`             | `UInt16`                   | Profundidad de recursión. `0` para el propio directorio consultado y sus hijos inmediatos, `1` para las entradas un nivel más abajo, y así sucesivamente.                                                                                                                        |
| `modification_time` | `Nullable(DateTime64(6))`  | Hora de la última modificación con precisión de microsegundos. `NULL` en caso de error.                                                                                                                                                                                          |
| `is_symlink`        | `Bool`                     | Indica si la entrada es un enlace simbólico.                                                                                                                                                                                                                                     |
| `content`           | `Nullable(String)`         | Contenido del archivo (para archivos regulares). `NULL` para archivos no regulares (directorios, enlaces simbólicos, etc.). Los errores de lectura generan una excepción. Leer esta columna desencadena operaciones reales de IO del archivo, así que omítala si no la necesita. |
| `owner_read`        | `Bool`                     | El propietario tiene permiso de lectura.                                                                                                                                                                                                                                         |
| `owner_write`       | `Bool`                     | El propietario tiene permiso de escritura.                                                                                                                                                                                                                                       |
| `owner_exec`        | `Bool`                     | El propietario tiene permiso de ejecución.                                                                                                                                                                                                                                       |
| `group_read`        | `Bool`                     | El grupo tiene permiso de lectura.                                                                                                                                                                                                                                               |
| `group_write`       | `Bool`                     | El grupo tiene permiso de escritura.                                                                                                                                                                                                                                             |
| `group_exec`        | `Bool`                     | El grupo tiene permiso de ejecución.                                                                                                                                                                                                                                             |
| `others_read`       | `Bool`                     | Otros tienen permiso de lectura.                                                                                                                                                                                                                                                 |
| `others_write`      | `Bool`                     | Otros tienen permiso de escritura.                                                                                                                                                                                                                                               |
| `others_exec`       | `Bool`                     | Otros tienen permiso de ejecución.                                                                                                                                                                                                                                               |
| `set_gid`           | `Bool`                     | Bit Set-GID.                                                                                                                                                                                                                                                                     |
| `set_uid`           | `Bool`                     | Bit Set-UID.                                                                                                                                                                                                                                                                     |
| `sticky_bit`        | `Bool`                     | Bit sticky.                                                                                                                                                                                                                                                                      |

Solo se calculan las columnas que realmente se usan en la consulta, por lo que seleccionar un subconjunto de columnas (especialmente si se omite `content`) es eficiente.

<div id="examples">
  ## Ejemplos
</div>

<div id="list-files">
  ### Listar archivos en user_files
</div>

```sql
SELECT name, type, size, depth
FROM filesystem()
ORDER BY name;
```

<div id="find-large-files">
  ### Buscar archivos grandes
</div>

```sql
SELECT path, name, size
FROM filesystem()
WHERE type = 'regular' AND size > 1000000
ORDER BY size DESC;
```

<div id="read-contents">
  ### Leer el contenido de un archivo
</div>

```sql
SELECT name, content
FROM filesystem('my_directory')
WHERE name LIKE '%.csv';
```

<div id="list-immediate">
  ### Listar solo los hijos directos
</div>

```sql
SELECT name, type
FROM filesystem('my_directory')
WHERE depth = 0;
```