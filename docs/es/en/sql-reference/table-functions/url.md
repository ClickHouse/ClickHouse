---
description: 'Crea una tabla a partir de la `URL` con `format` y `structure` especificados'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # Función de tabla url
</div>

La función `url` crea una tabla a partir de la `URL` especificada, con el `format` y la estructura indicados.

La función `url` puede usarse en consultas `SELECT` e `INSERT` sobre datos de tablas [URL](../../engines/table-engines/special/url.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## Parámetros
</div>

| Parámetro   | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URL`       | Una URL entre comillas simples cuyo esquema selecciona el backend. Una URL `http`/`https` (o no reconocida) es una dirección del servidor que acepta solicitudes `GET` o `POST` (para consultas `SELECT` o `INSERT`, respectivamente); un esquema no HTTP reconocido (`file://`, `s3://`, `az://`, `hdfs://`, …) se delega a la función de tabla correspondiente; consulte [enrutamiento según el esquema de URL](#scheme-dispatch). Tipo: [String](../../sql-reference/data-types/string.md). |
| `format`    | [Formato](/es/sql-reference/formats) de los datos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                      |
| `structure` | Estructura de la tabla en formato `'UserID UInt64, Name String'`. Determina los nombres de las columnas y los tipos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                 |
| `headers`   | Encabezados en formato `'headers('key1'='value1', 'key2'='value2')'`. Puede establecer encabezados para la llamada HTTP.                                                                                                                                                                                                                                                                                                                                                                       |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con el formato y la estructura especificados, y con los datos de la `URL` definida.

<div id="examples">
  ## Ejemplos
</div>

Obtener las 3 primeras líneas de una tabla que contiene columnas de tipo `String` y [UInt32](../../sql-reference/data-types/int-uint.md) de un servidor HTTP que responde en formato [CSV](/es/interfaces/formats/CSV).

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Insertar datos desde una `URL` en una tabla:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## Enrutamiento según el esquema de URL
</div>

La función `url` actúa como un envoltorio unificado sobre las demás funciones de tabla para archivos y almacenamiento de objetos: enruta al backend adecuado según el esquema de la URL. Esto permite leer desde cualquier ubicación compatible con una única sintaxis uniforme.

| Esquema                                             | Se enruta a                                          |
| --------------------------------------------------- | ---------------------------------------------------- |
| `http`, `https` (y cualquier esquema no reconocido) | el propio motor `URL` (HTTP `GET`/`POST`)            |
| `file`                                              | la función [`file`](file.md)                         |
| `s3`, `gs`, `gcs`, `oss`                            | la función [`s3`](s3.md)                             |
| `az`, `azure`, `abfss`, `abfs`                      | la función [`azureBlobStorage`](azureBlobStorage.md) |
| `hdfs`                                              | la función [`hdfs`](hdfs.md)                         |

Solo se enrutan los esquemas de S3 que el mapeador de URI de S3 resuelve a un endpoint concreto sin configuración adicional (`s3`, además de `gs`/`gcs`/`oss`). Otros esquemas de proveedores compatibles con S3 (`cos`, `obs`, `eos`, …) son específicos de cada región y no tienen una asignación de endpoint predeterminada, por lo que una URL `cos://…` se trata como un esquema no reconocido y se notifica como un error; use la función [`s3`](s3.md) directamente (con `url_scheme_mappers` configurado) para esos backends.

Para `file://`, una ruta relativa (`file://data.csv`) se resuelve dentro del directorio [user&#95;files](/es/operations/server-configuration-parameters/settings#user_files_path), y una ruta absoluta (`file:///home/user/data.csv`) debe apuntar dentro de él, como es habitual.

Los argumentos `format`, `structure` y `compression_method`, así como la configuración [url&#95;base](#resolving-relative-urls), funcionan igual independientemente del destino del enrutamiento.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

El enrutamiento según el esquema aún no está implementado en [`urlCluster`](urlCluster.md): si se pasa a `urlCluster` un esquema distinto de `http(s)`, se rechaza con un error. En su lugar, use la función de clúster correspondiente (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …) para esos backends.

<div id="globs-in-url">
  ## Globs en la URL
</div>

Los patrones entre `{ }` se usan para generar un conjunto de segmentos o para especificar direcciones de conmutación por error. Para ver los tipos de patrones admitidos y ejemplos, consulta la descripción de la función [remote](remote.md#globs-in-addresses).
El carácter `|` dentro de los patrones se usa para especificar direcciones de conmutación por error. Se recorren en el mismo orden en que se enumeran en el patrón. El número de direcciones generadas está limitado por la opción [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).
Para la sintaxis de globs de ruta en la ruta de la URL (como `*`, `{a,b}`, `{N..M}` y `**`), consulta [Globs en la ruta](file.md#globs-in-path). Ten en cuenta que `?` inicia la cadena de consulta en una URL y no puede usarse como comodín en el componente de ruta.

<div id="wildcards-with-http-index-pages">
  ## Comodines con páginas de índice HTTP
</div>

Para `url` y el motor de tabla `URL`, ClickHouse puede expandir comodines recuperando páginas de índice HTTP (HTML o texto sin formato) y extrayendo las URL del cuerpo de la respuesta. Esto permite patrones como `/**/` cuando el servidor expone listados de directorios.

Notas:

* Las URL relativas se resuelven con respecto a la URL de la página de índice.
* Las plantillas de `URL` se expanden antes de recuperar las páginas de índice, incluida la expansión por comas y rangos numéricos de segmentos, así como las opciones de conmutación por error `|` fuera del componente de ruta.
* Los patrones de conmutación por error `|` dentro del componente de ruta no se admiten para la expansión de páginas de índice HTTP.
* La coincidencia de comodines se aplica al componente de ruta de la URL.
* Si una URL listada ya contiene una cadena de consulta o un fragmento, tiene prioridad sobre los de la URL de origen. En caso contrario, se usan la cadena de consulta y el fragmento de la URL de origen.
* Se permite un listado vacío; los errores HTTP (por ejemplo, 404) en las páginas de índice generan excepciones.
* El tamaño máximo de la página de índice está limitado por [max&#95;http&#95;index&#95;page&#95;size](/es/operations/server-configuration-parameters/settings.md#max_http_index_page_size).
* El número máximo de directorios leídos durante la expansión recursiva está limitado por [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/es/operations/settings/settings.md#url_wildcard_max_directories_to_read).

Ejemplo:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta de la `URL`. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del recurso de la `URL`. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del recurso en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_headers` - Encabezados de respuesta HTTP. Tipo: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="hive-style-partitioning">
  ## ajuste `use_hive_partitioning`
</div>

Si el ajuste `use_hive_partitioning` se establece en 1, ClickHouse detectará el particionado de estilo Hive en la ruta (`/name=value/`) y permitirá usar las columnas de partición como columnas virtuales en la consulta. Estas columnas virtuales tendrán los mismos nombres que en la ruta particionada.

**Ejemplo**

Usar una columna virtual creada con particionado de estilo Hive

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## Resolución de URLs relativas
</div>

La configuración [url&#95;base](/es/operations/settings/settings.md#url_base) permite pasar una URL relativa a la función `url`. Cuando `url_base` está configurada y el argumento de la función es una referencia relativa, esta se resuelve con respecto a la URL base según [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

Las reglas de resolución son:

* **Relativa a la ruta** (p. ej., `data.csv`): se combina con la ruta de la URL base; todo lo que aparece después de la última `/` de la ruta base se reemplaza. La barra diagonal final importa: `https://example.com/dir/` + `data.csv` da `https://example.com/dir/data.csv`, pero `https://example.com/dir` + `data.csv` da `https://example.com/data.csv`. Los segmentos de punto (`./` y `../`) se normalizan.
* **Relativa al host** (p. ej., `/test/data.csv`): se resuelve usando el esquema y el host de la URL base.
* **Relativa al esquema** (p. ej., `//other.com/test/data.csv`): se resuelve usando el esquema de la URL base.
* **Solo consulta** (p. ej., `?x=1`): se añade a la ruta base completa y reemplaza cualquier consulta o fragmento existente.
* **Solo fragmento** (p. ej., `#frag`): se añade a la URL base, conserva la consulta y reemplaza cualquier fragmento existente.
* **Vacía**: devuelve la URL base sin fragmento.
* **URL absoluta**: se pasa sin cambios; `url_base` se ignora.

**Ejemplo**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## Configuración de almacenamiento
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/es/operations/settings/settings.md#engine_url_skip_empty_files) - permite omitir archivos vacíos durante la lectura. Está deshabilitado de forma predeterminada.
* [enable&#95;url&#95;encoding](/es/operations/settings/settings.md#enable_url_encoding) - permite habilitar o deshabilitar la decodificación/codificación de la ruta en el URI. Está habilitado de forma predeterminada.
* [url&#95;base](/es/operations/settings/settings.md#url_base) - URL base para resolver las URL relativas que se pasan a la función `url`.

<div id="permissions">
  ## Permisos
</div>

La función `url` requiere el permiso `CREATE TEMPORARY TABLE`. Por lo tanto, no funcionará para los usuarios con la configuración [readonly](/es/operations/settings/permissions-for-queries#readonly) = 1. Se requiere como mínimo readonly = 2.

<div id="related">
  ## Relacionado
</div>

* [Columnas virtuales](/es/engines/table-engines/index.md#table_engines-virtual_columns)