---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'Fuente de diccionario de archivo local'
sidebar_position: 2
sidebar_label: 'Local File'
description: 'Configure un archivo local como fuente de diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

La fuente de archivo local carga los datos del diccionario desde un archivo del sistema de archivos local. Esto resulta útil para tablas de consulta pequeñas y estáticas que pueden almacenarse como archivos planos en formatos como TSV, CSV o cualquier otro [formato compatible](/es/sql-reference/formats).

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Configuración | Descripción                                                                                           |
| ------------- | ----------------------------------------------------------------------------------------------------- |
| `path`        | La ruta absoluta al archivo.                                                                          |
| `format`      | El formato del archivo. Se admiten todos los formatos descritos en [Formats](/es/sql-reference/formats). |

Cuando se crea un diccionario con la fuente `FILE` mediante un comando DDL (`CREATE DICTIONARY ...`), el archivo de origen debe estar ubicado en el directorio `user_files` para evitar que los usuarios de la BD accedan a archivos arbitrarios del nodo de ClickHouse.

**Véase también**

* [Función dictionary](/es/sql-reference/table-functions/dictionary)