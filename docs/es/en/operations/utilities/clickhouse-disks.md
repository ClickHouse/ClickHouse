---
description: 'Documentación de Clickhouse-disks'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
doc_type: 'reference'
---

Una utilidad que ofrece operaciones similares a las de un sistema de archivos para los discos de ClickHouse. Puede funcionar tanto en modo interactivo como en modo no interactivo.

<div id="program-wide-options">
  ## Opciones globales del programa
</div>

* `--config-file, -C` -- ruta al archivo de configuración de ClickHouse; el valor predeterminado es `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- registra el progreso de los comandos invocados en `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- qué [tipo](../server-configuration-parameters/settings#logger) de eventos registrar; el valor predeterminado es `none`.
* `--disk` -- qué disco usar para los comandos `mkdir, move, read, write, remove`. El valor predeterminado es `default`.
* `--query, -q` -- consulta única que puede ejecutarse sin iniciar el modo interactivo
* `--help, -h` -- muestra todas las opciones y comandos con su descripción

<div id="lazy-initialization">
  ## Inicialización diferida
</div>

Todos los discos disponibles en la configuración se inicializan de forma diferida. Esto significa que el objeto correspondiente a un disco se inicializa solo cuando ese disco se utiliza en algún comando. Esto se hace para que la utilidad sea más robusta y para evitar interactuar con discos definidos en la configuración pero no utilizados por el usuario, y que pueden fallar durante la inicialización. Sin embargo, debe haber un disco que se inicialice al arrancar `clickhouse-disks`. Este disco se especifica con el parámetro `--disk` en la línea de comandos (el valor predeterminado es `default`).

<div id="default-disks">
  ## Discos predeterminados
</div>

Tras el inicio, hay dos discos que no están especificados en la configuración, pero están disponibles para su inicialización.

1. **Disco `local`**: Este disco está diseñado para emular el sistema de archivos local desde el que se ejecutó la utilidad `clickhouse-disks`. Su ruta inicial es el directorio desde el que se inició `clickhouse-disks`, y está montado en el directorio raíz del sistema de archivos.

2. **Disco `default`**: Este disco está montado en el sistema de archivos local en el directorio especificado por el parámetro `clickhouse/path` en la configuración (el valor predeterminado es `/var/lib/clickhouse`). Su ruta inicial está establecida en `/`.

<div id="clickhouse-disks-state">
  ## Estado de Clickhouse-disks
</div>

Para cada disco que se añade, la utilidad almacena el directorio actual (como en cualquier sistema de archivos habitual). El usuario puede cambiar el directorio actual y alternar entre discos.

El estado se refleja en el prompt &quot;`disk_name`:`path_name`&quot;

<div id="commands">
  ## Comandos
</div>

En este archivo de documentación, todos los argumentos posicionales obligatorios se indican como `<parameter>` y los argumentos con nombre como `[--parameter value]`. Todos los parámetros posicionales también pueden indicarse como parámetros con nombre con el nombre correspondiente.

* `cd (change-dir, change_dir) [--disk disk] <path>`
  Cambia el directorio a la ruta `path` en el disco `disk` (el valor predeterminado es el disco actual). No se cambia de disco.
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  Copia datos recursivamente desde `path-from` en el disco `disk_1` (el valor predeterminado es el disco actual (parámetro `disk` en modo no interactivo))
  a `path-to` en el disco `disk_2` (el valor predeterminado es el disco actual (parámetro `disk` en modo no interactivo)).
* `current_disk_with_path (current, current_disk, current_path)`
  Imprime el estado actual con el formato:
  `Disk: "current_disk" Path: "current path on current disk"`
* `du [--human-readable] [<path>]`
  Imprime el tamaño total en bytes del archivo o directorio en `path` en el disco actual. En el caso de un directorio, el tamaño de todos los archivos que contiene se suma recursivamente. Si no se especifica `path`, se usa el directorio actual. Con `--human-readable` (`-h`), el tamaño se imprime en un formato legible para humanos (p. ej., `1.23 GiB`).
* `help [<command>]`
  Imprime el mensaje de ayuda del comando `command`. Si no se especifica `command`, imprime información sobre todos los comandos.
* `move (mv) <path-from> <path-to>`.
  Mueve un archivo o directorio de `path-from` a `path-to` dentro del disco actual.
* `remove (rm, delete) <path>`.
  Elimina `path` recursivamente en el disco actual.
* `link (ln) <path-from> <path-to>`.
  Crea un enlace físico de `path-from` a `path-to` en el disco actual.
* `list (ls) [--recursive] <path>`
  Lista los archivos en `path` en el disco actual. No es recursivo de forma predeterminada.
* `list-disks (list_disks, ls-disks, ls_disks)`.
  Lista los nombres de los discos.
* `mkdir [--recursive] <path>` en el disco actual.
  Crea un directorio. No es recursivo de forma predeterminada.
* `read (r) <path-from> [--path-to path]`
  Lee un archivo desde `path-from` hacia `path` (`stdout` si no se proporciona).
* `read-bitmap <path-from> [--values]`
  Inspecciona un archivo auxiliar de bitmap de eliminación (`.rbm`) en `path-from`. Imprime el magic y la versión, la validez del CRC, la cardinalidad (número de filas eliminadas) y el rango de filas. Con `--values`, también vuelca todos los bits activados (los desplazamientos de las filas eliminadas) en orden ascendente.
* `switch-disk [--path path] <disk>`
  Cambia al disco `disk` en la ruta `path` (si no se especifica `path`, el valor predeterminado es la ruta anterior en el disco `disk`).
* `write (w) [--path-from path] <path-to>`.
  Escribe un archivo desde `path` (`stdin` si no se proporciona `path`; la entrada debe finalizar con Ctrl+D) a `path-to`.
* `wc <path> [--bytes] [--lines] [--words]`
  Cuenta bytes, líneas y palabras en el archivo en `path` en el disco actual (como Unix `wc`). Sin ningún indicador, se imprimen los tres recuentos en este orden: líneas, palabras y bytes. Usa `--bytes` (`-c`), `--lines` (`-l`) y `--words` (`-w`) para seleccionar recuentos específicos.
* `sed <expression> <path>`
  Aplica la `expression` de `sed` al archivo en `path` en el disco actual, modificándolo en el lugar. Requiere que `sed` esté instalado en el host. Solo se admite una única expresión `sed` sin opciones (p. ej., `'s/foo/bar/g'`, `'/foo/d'`), no varias expresiones (`-e ... -e ...`) ni opciones combinadas con una dirección (p. ej., `-n` con `4,10p`).
* `read-checksums <path>`
  Lee un archivo `checksums.txt` de un data part de `MergeTree` en el disco actual y lo imprime en `stdout` como una tabla tab-separated, legible para humanos, con las columnas `name`, `file_size`, `file_hash`, `uncompressed_size` y `uncompressed_hash`. Las dos últimas columnas solo están presentes en los archivos comprimidos.