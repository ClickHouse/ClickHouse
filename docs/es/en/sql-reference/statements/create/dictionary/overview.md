---
description: 'Documentación para crear y configurar diccionarios'
sidebar_label: 'Descripción general'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

Un diccionario es una asociación (`key -> attributes`) útil para varios tipos de listas de referencia.
ClickHouse admite funciones especiales para trabajar con diccionarios que pueden usarse en consultas. Usar diccionarios con funciones es más fácil y eficiente que hacer un `JOIN` con tablas de referencia.

Los diccionarios se pueden crear de dos maneras:

* [Con una consulta DDL](#creating-a-dictionary-with-a-ddl-query) (recomendado)
* [Con un archivo de configuración](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## Crear un diccionario con una consulta DDL
</div>

<CloudSupportedBadge />

Los diccionarios se pueden crear con consultas DDL.
Este es el método recomendado porque, con los diccionarios creados mediante DDL:

* No se añaden registros adicionales a los archivos de configuración del servidor.
* Los diccionarios se pueden usar como entidades de primera clase, como las tablas o las vistas.
* Los datos se pueden leer directamente, usando la sintaxis habitual de `SELECT` en lugar de funciones de tabla para diccionarios. Ten en cuenta que, al acceder directamente a un diccionario mediante una instrucción `SELECT`, un diccionario en caché devolverá solo los datos almacenados en caché, mientras que un diccionario sin caché devolverá todos los datos que almacena.
* Los diccionarios se pueden renombrar fácilmente.

<div id="syntax">
  ### Sintaxis
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Cláusula                                    | Descripción                                                                                                                                                                                 |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Atributos](./attributes.md)                | Los atributos del diccionario se especifican de forma similar a las columnas de una tabla. La única propiedad obligatoria es el tipo; todas las demás pueden tener valores predeterminados. |
| PRIMARY KEY                                 | Define la(s) columna(s) clave para las búsquedas del diccionario. Según el diseño, se pueden especificar uno o varios atributos como claves.                                                |
| [`SOURCE`](./sources/overview.md)           | Define la fuente de datos del diccionario (p. ej., tabla de ClickHouse, HTTP, PostgreSQL).                                                                                                  |
| [`LAYOUT`](./layouts/overview.md)           | Controla cómo se almacena el diccionario en memoria (p. ej., `FLAT`, `HASHED`, `CACHE`).                                                                                                    |
| [`LIFETIME`](./lifetime.md)                 | Establece el intervalo de actualización del diccionario.                                                                                                                                    |
| [`ON CLUSTER`](../../../distributed-ddl.md) | Crea el diccionario en un clúster. Opcional.                                                                                                                                                |
| `SETTINGS`                                  | Ajustes adicionales del diccionario. Opcional.                                                                                                                                              |
| `COMMENT`                                   | Añade un comentario de texto al diccionario. Opcional.                                                                                                                                      |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## Crear un diccionario con un archivo de configuración
</div>

<CloudNotSupportedBadge />

:::note
La creación de un diccionario con un archivo de configuración no es aplicable a ClickHouse Cloud. Utilice DDL (consulte más arriba) y cree el diccionario como el usuario `default`.
:::

El archivo de configuración del diccionario tiene el siguiente formato:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

Puede configurar tantos diccionarios como quiera en el mismo archivo.

<div id="related-content">
  ## Contenido relacionado
</div>

* [Diseños de almacenamiento](/es/sql-reference/statements/create/dictionary/layouts) — Cómo se almacenan los diccionarios en memoria
* [Fuentes](/es/sql-reference/statements/create/dictionary/sources) — Conectar con fuentes de datos
* [Tiempo de vida](./lifetime.md) — Configuración de actualización automática
* [Atributos](./attributes.md) — Configuración de claves y atributos
* [Diccionarios integrados](./embedded.md) — Diccionarios geobase integrados
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — Tabla del sistema con información sobre diccionarios