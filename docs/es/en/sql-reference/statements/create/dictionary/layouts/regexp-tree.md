---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: 'Layout de diccionario de árbol de expresiones regulares'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'Configure un diccionario de árbol de expresiones regulares para realizar búsquedas por patrones.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## Descripción general
</div>

El diccionario `regexp_tree` permite asignar claves a valores en función de patrones jerárquicos de expresiones regulares.
Está optimizado para búsquedas por coincidencia de patrones (por ejemplo, para clasificar cadenas como las de user agent mediante patrones regex), en lugar de para la coincidencia exacta de claves.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="Introducción a los diccionarios de árbol regexp de ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## Usar el diccionario de árbol de expresiones regulares con la fuente YAMLRegExpTree
</div>

<CloudNotSupportedBadge />

Los diccionarios de árbol de expresiones regulares se definen en ClickHouse open-source mediante la fuente [`YAMLRegExpTree`](../sources/yamlregexptree.md), a la que se le pasa la ruta de un archivo YAML que contiene el árbol de expresiones regulares.

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

La fuente del diccionario [`YAMLRegExpTree`](../sources/yamlregexptree.md) representa la estructura de un árbol de expresiones regulares. Por ejemplo:

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

Esta configuración consiste en una lista de nodos de un árbol de expresiones regulares. Cada nodo tiene la siguiente estructura:

* **regexp**: la expresión regular del nodo.
* **attributes**: una lista de atributos de diccionario definidos por el usuario. En este ejemplo, hay dos atributos: `name` y `version`. El primer nodo define ambos atributos. El segundo nodo solo define el atributo `name`. El atributo `version` lo proporcionan los nodos hijo del segundo nodo.
  * El valor de un atributo puede contener **retroreferencias**, que remiten a grupos de captura de la expresión regular coincidente. En el ejemplo, el valor del atributo `version` en el primer nodo consiste en una retroreferencia `\1` al grupo de captura `(\d+[\.\d]*)` de la expresión regular. Los números de retroreferencia van del 1 al 9 y se escriben como `$1` o `\1` (para el número 1). La retroreferencia se sustituye por el grupo de captura coincidente durante la ejecución de la consulta.
* **child nodes**: una lista de nodos hijo de un nodo de árbol regexp, cada uno con sus propios atributos y (potencialmente) nodos hijo. La coincidencia de cadenas se realiza en profundidad. Si una cadena coincide con un nodo regexp, el diccionario comprueba si también coincide con los nodos hijo de ese nodo. Si es así, se asignan los atributos del nodo coincidente más profundo. Los atributos de un nodo hijo sobrescriben los atributos con el mismo nombre de los nodos padre. El nombre de los nodos hijo en los archivos YAML puede ser arbitrario; por ejemplo, `versions` en el ejemplo anterior.

Los diccionarios de árbol regexp solo permiten el acceso mediante las funciones `dictGet`, `dictGetOrDefault` y `dictGetAll`. Por ejemplo:

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

En este caso, primero se hace coincidir la expresión regular `\d+/tclwebkit(?:\d+[\.\d]*)` en el segundo nodo de la capa superior.
Luego, el diccionario sigue buscando en los nodos hijo y encuentra que la cadena también coincide con `3[12]/tclwebkit`.
Como resultado, el valor del atributo `name` es `Android` (definido en la primera capa) y el valor del atributo `version` es `12` (definido en el nodo hijo).

Con un sofisticado archivo de configuración YAML, puede usar un diccionario de árbol de expresiones regulares como parser de cadenas user agent.
ClickHouse admite [uap-core](https://github.com/ua-parser/uap-core), y puede ver cómo usarlo en la prueba funcional [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

<div id="collecting-attribute-values">
  ### Recopilación de valores de atributo
</div>

A veces es útil devolver valores de varias expresiones regulares que hayan coincidido, en lugar de solo el valor de un nodo hoja. En estos casos, se puede usar la función especializada [`dictGetAll`](/es/sql-reference/functions/ext-dict-functions.md#dictGetAll). Si un nodo tiene un valor de atributo de tipo `T`, `dictGetAll` devolverá un `Array(T)` que contendrá cero o más valores.

De forma predeterminada, el número de coincidencias devueltas por clave no tiene límite. Se puede pasar un límite como cuarto argumento opcional a `dictGetAll`. El array se completa en *orden topológico*, lo que significa que los nodos hijo van antes que los nodos padre y que los nodos hermanos siguen el orden del origen.

Ejemplo:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### Modos de coincidencia
</div>

El comportamiento de la coincidencia de patrones puede modificarse con determinadas configuraciones del diccionario:

* `regexp_dict_flag_case_insensitive`: Activa la coincidencia sin distinción entre mayúsculas y minúsculas (el valor predeterminado es `false`). Puede sobrescribirse en expresiones individuales con `(?i)` y `(?-i)`.
* `regexp_dict_flag_dotall`: Permite que &#39;.&#39; coincida con caracteres de salto de línea (el valor predeterminado es `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## Usar un diccionario de árbol de expresiones regulares en ClickHouse Cloud
</div>

La fuente [`YAMLRegExpTree`](../sources/yamlregexptree.md) funciona en ClickHouse Open Source, pero no en ClickHouse Cloud.
Para usar diccionarios de árbol de expresiones regulares en ClickHouse Cloud, primero cree de forma local en ClickHouse Open Source un diccionario de árbol de expresiones regulares a partir de un archivo YAML y, después, vuelque ese diccionario en un archivo CSV mediante la función de tabla `dictionary` y la cláusula [INTO OUTFILE](/es/sql-reference/statements/select/into-outfile.md).

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

El contenido del archivo CSV es:

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

El esquema del archivo exportado es:

* `id UInt64`: el id del nodo de RegexpTree.
* `parent_id UInt64`: el id del parent de un nodo.
* `regexp String`: la cadena de la expresión regular.
* `keys Array(String)`: los nombres de los atributos definidos por el usuario.
* `values Array(String)`: los valores de los atributos definidos por el usuario.

Para crear el diccionario en ClickHouse Cloud, primero cree una tabla `regexp_dictionary_source_table` con la siguiente estructura de tabla:

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

Luego, actualice el CSV local con

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

Para más detalles, consulta cómo [insertar archivos locales](/es/integrations/data-ingestion/insert-local-files). Después de inicializar la tabla de origen, podemos crear un RegexpTree a partir de la tabla de origen:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```