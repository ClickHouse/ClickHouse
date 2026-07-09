---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'Fuente de diccionario YAMLRegExpTree'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'Configure un archivo YAML como fuente para diccionarios de árbol de expresiones regulares.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

La fuente `YAMLRegExpTree` carga un árbol de expresiones regulares desde un archivo YAML en el sistema de archivos local.
Está diseñada exclusivamente para usarse con el layout de diccionario [`regexp_tree`](../layouts/regexp-tree.md)
y proporciona asignaciones jerárquicas de regex a atributos para búsquedas basadas en patrones, como el análisis de user agents.

:::note
La fuente `YAMLRegExpTree` solo está disponible en ClickHouse Open Source.
Para ClickHouse Cloud, exporte el diccionario a CSV y cárguelo mediante una [fuente de tabla de ClickHouse](./clickhouse.md).
Consulte [Uso de diccionarios regexp&#95;tree en ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) para obtener más detalles.
:::

<div id="configuration">
  ## Configuración
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

Campos de configuración:

| Configuración | Descripción                                                                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PATH`        | La ruta absoluta del archivo YAML que contiene el árbol de expresiones regulares. Cuando se crea mediante DDL, el archivo debe estar en el directorio `user_files`. |

<div id="yaml-file-structure">
  ## Estructura del archivo YAML
</div>

El archivo YAML contiene una lista de nodos de un árbol de expresiones regulares. Cada nodo puede tener atributos y nodos hijo, formando una jerarquía:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Cada nodo tiene la siguiente estructura:

* **`regexp`**: La expresión regular de este nodo.
* **atributos**: Atributos de diccionario definidos por el usuario (p. ej., `name`, `version`). Los valores de los atributos pueden contener **retroreferencias** a grupos de captura en la expresión regular, escritas como `\1` o `$1` (números del 1 al 9). Estas se sustituyen por el grupo de captura coincidente en el momento de la consulta.
* **nodos hijo**: Una lista de hijos, cada uno con sus propios atributos y, opcionalmente, más hijos. El nombre de la lista de hijos es arbitrario (p. ej., `versions` arriba). La búsqueda de coincidencias de cadenas se realiza en profundidad: si una cadena coincide con un nodo, también se comprueban sus hijos. Los atributos del nodo coincidente más profundo tienen prioridad y sobrescriben los atributos del padre con el mismo nombre.

<div id="related-pages">
  ## Páginas relacionadas
</div>

* [layout del diccionario regexp&#95;tree](../layouts/regexp-tree.md) — configuración del layout, ejemplos de consulta y modos de coincidencia
* [dictGet](/es/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/es/sql-reference/functions/ext-dict-functions#dictGetAll) — funciones para consultar diccionarios regexp tree