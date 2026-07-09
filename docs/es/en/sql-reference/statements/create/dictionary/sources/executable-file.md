---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: 'Origen de diccionario de archivo ejecutable'
sidebar_position: 3
sidebar_label: 'Archivo ejecutable'
description: 'Configurar un archivo ejecutable como origen de diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

El uso de archivos ejecutables depende de [cómo se almacena el diccionario en memoria](../layouts/). Si el diccionario se almacena con `cache` y `complex_key_cache`, ClickHouse solicita las claves necesarias enviando una solicitud al stdin del archivo ejecutable. En caso contrario, ClickHouse inicia el archivo ejecutable y considera su salida como datos del diccionario.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

Campos de configuración:

| Setting                       | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | La ruta absoluta al archivo ejecutable o el nombre del archivo (si el directorio del comando está en `PATH`).                                                                                                                                                                                                                                                                                                                                      |
| `format`                      | El formato del archivo. Se admiten todos los formatos descritos en [Formatos](/es/sql-reference/formats).                                                                                                                                                                                                                                                                                                                                             |
| `command_termination_timeout` | El script ejecutable debe contener un bucle principal de lectura y escritura. Después de que se destruya el diccionario, se cierra el pipe y el archivo ejecutable tendrá `command_termination_timeout` segundos para finalizar antes de que ClickHouse envíe una señal SIGTERM al proceso hijo. Se especifica en segundos. El valor predeterminado es `10`. Opcional.                                                                             |
| `command_read_timeout`        | Tiempo de espera para leer datos del stdout del comando, en milisegundos. El valor predeterminado es `10000`. Opcional.                                                                                                                                                                                                                                                                                                                            |
| `command_write_timeout`       | Tiempo de espera para escribir datos en el stdin del comando, en milisegundos. El valor predeterminado es `10000`. Opcional.                                                                                                                                                                                                                                                                                                                       |
| `implicit_key`                | La fuente ejecutable puede devolver solo valores, y la correspondencia con las claves solicitadas se determina implícitamente por el orden de las filas en el resultado. El valor predeterminado es `false`.                                                                                                                                                                                                                                       |
| `execute_direct`              | Si `execute_direct` = `1`, `command` se buscará dentro de la carpeta user&#95;scripts especificada por [user&#95;scripts&#95;path](/es/operations/server-configuration-parameters/settings#user_scripts_path). Se pueden especificar argumentos adicionales del script separados por espacios. Ejemplo: `script_name arg1 arg2`. Si `execute_direct` = `0`, `command` se pasa como argumento a `bin/sh -c`. El valor predeterminado es `0`. Opcional. |
| `send_chunk_header`           | Controla si se debe enviar el número de filas antes de enviar un fragmento de datos al proceso. El valor predeterminado es `false`. Opcional.                                                                                                                                                                                                                                                                                                      |

Esa fuente de diccionario solo puede configurarse mediante una configuración XML. La creación de diccionarios con una fuente ejecutable mediante DDL está deshabilitada; de lo contrario, el usuario de la base de datos podría ejecutar binarios arbitrarios en el nodo de ClickHouse.