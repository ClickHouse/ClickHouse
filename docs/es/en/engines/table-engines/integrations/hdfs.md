---
description: 'Este motor proporciona integración con el ecosistema de Apache Hadoop
  al permitir gestionar datos en HDFS mediante ClickHouse. Este motor es similar a los motores File
  y URL, pero ofrece funcionalidades específicas de Hadoop.'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'Motor de tabla HDFS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # Motor de tabla HDFS
</div>

<CloudNotSupportedBadge />

Este motor proporciona integración con el ecosistema de [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) al permitir gestionar datos en [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) a través de ClickHouse. Este motor es similar a los motores [File](/es/engines/table-engines/special/file) y [URL](/es/engines/table-engines/special/url), pero ofrece funcionalidades específicas de Hadoop.

Esta funcionalidad no cuenta con soporte por parte de los ingenieros de ClickHouse, y se sabe que su calidad es cuestionable. Si tienes algún problema, arréglalo tú mismo y envía un pull request.

<div id="usage">
  ## Uso
</div>

```sql
ENGINE = HDFS(URI, format)
```

**Parámetros del motor**

* `URI` - URI completa del archivo en HDFS. La parte de la ruta de `URI` puede contener patrones glob. En este caso, la tabla sería de solo lectura.
* `format` - especifica uno de los formatos de archivo disponibles. Para realizar
  consultas `SELECT`, el formato debe ser compatible con la entrada, y para realizar
  consultas `INSERT`, con la salida. Los formatos disponibles se enumeran en la
  sección [Formatos](/es/sql-reference/formats#formats-overview).
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — Opcional. En la mayoría de los casos no necesitas una clave de partición y, si hace falta, por lo general no debe ser más granular que por mes. La partición no acelera las consultas (a diferencia de la expresión ORDER BY). Nunca debes usar una partición demasiado granular. No particiones tus datos por identificadores o nombres de client (en su lugar, haz que el identificador o el nombre del client sea la primera columna de la expresión ORDER BY).

Para particionar por mes, usa la expresión `toYYYYMM(date_column)`, donde `date_column` es una columna con una fecha del tipo [Date](/es/sql-reference/data-types/date.md). Los nombres de las particiones aquí tienen el formato `"YYYYMM"`.

**Ejemplo:**

**1.** Configura la tabla `hdfs_engine_table`:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Archivo de relleno:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Consulta los datos:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## Detalles de implementación
</div>

* Las lecturas y escrituras pueden realizarse en paralelo.
* No se admite:

  * Las operaciones `ALTER` y `SELECT...SAMPLE`.
  * Los índices.
  * La [replicación zero-copy](../../../operations/storing-data.md#zero-copy) es posible, pero no se recomienda.

  :::note La replicación zero-copy no está lista para producción
  La replicación zero-copy está deshabilitada de forma predeterminada en ClickHouse versión 22.8 y posteriores. Esta funcionalidad no se recomienda para su uso en producción.
  :::

**Globs en la ruta**

Varios componentes de la ruta pueden contener globs. Para que un archivo se procese, debe existir y coincidir con el patrón de la ruta completa. La lista de archivos se determina durante `SELECT` (no en el momento de `CREATE`).

* `*` — Sustituye cualquier número de caracteres, excepto `/`, incluida la cadena vacía.
* `?` — Sustituye cualquier carácter individual.
* `{some_string,another_string,yet_another_one}` — Sustituye cualquiera de las cadenas `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Sustituye cualquier número del rango de N a M, incluidos ambos límites.

Las construcciones con `{}` son similares a la table function [remote](../../../sql-reference/table-functions/remote.md).

**Ejemplo**

1. Supongamos que tenemos varios archivos en formato TSV con los siguientes URI en HDFS:

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Hay varias formas de crear una tabla compuesta por los seis archivos:

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Otra opción:

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

La tabla incluye todos los archivos de ambos directorios (todos los archivos deben cumplir el formato y el esquema descritos en la consulta):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
Si la lista de archivos contiene rangos numéricos con ceros a la izquierda, use la construcción con llaves para cada dígito por separado o `?`.
:::

**Ejemplo**

Cree una tabla con archivos llamados `file000`, `file001`, ... , `file999`:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## Configuración
</div>

Al igual que GraphiteMergeTree, el motor HDFS admite una configuración ampliada mediante el archivo de configuración de ClickHouse. Puede usar dos claves de configuración: una global (`hdfs`) y otra a nivel de usuario (`hdfs_*`). La configuración global se aplica primero y, después, la configuración a nivel de usuario (si existe).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### Opciones de configuración
</div>

<div id="supported-by-libhdfs3">
  #### Compatible con libhdfs3
</div>

| **parámetro**                                                           | **valor predeterminado**          |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

La [referencia de configuración de HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) puede ayudar a aclarar algunos parámetros.

<div id="clickhouse-extras">
  #### Opciones adicionales de ClickHouse
</div>

| **parámetro**                     | **valor predeterminado** |
| --------------------------------- | ------------------------ |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot;             |
| hadoop&#95;kerberos&#95;principal | &quot;&quot;             |
| libhdfs3&#95;conf                 | &quot;&quot;             |

<div id="limitations">
  ### Limitaciones
</div>

* `hadoop_security_kerberos_ticket_cache_path` y `libhdfs3_conf` solo pueden ser globales; no específicos de cada usuario

<div id="kerberos-support">
  ## Compatibilidad con Kerberos
</div>

Si el parámetro `hadoop_security_authentication` tiene el valor `kerberos`, ClickHouse se autentica mediante Kerberos.
Los parámetros están [aquí](#clickhouse-extras), y `hadoop_security_kerberos_ticket_cache_path` puede ser útil.
Tenga en cuenta que, debido a las limitaciones de libhdfs3, solo se admite el enfoque tradicional;
las comunicaciones del datanode no están protegidas con SASL (`HADOOP_SECURE_DN_USER` es un indicador fiable de este
enfoque de seguridad). Use `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` como referencia.

Si se especifican `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` o `hadoop_security_kerberos_ticket_cache_path`, se utilizará la autenticación de Kerberos. En este caso, `hadoop_kerberos_keytab` y `hadoop_kerberos_principal` son obligatorios.

<div id="namenode-ha">
  ## Compatibilidad con la alta disponibilidad del namenode de HDFS
</div>

libhdfs3 admite la alta disponibilidad del namenode de HDFS.

* Copie `hdfs-site.xml` desde un nodo de HDFS a `/etc/clickhouse-server/`.
* Añada la siguiente sección al archivo de configuración de ClickHouse:

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* Luego, usa el valor de la etiqueta `dfs.nameservices` de `hdfs-site.xml` como dirección del namenode en el URI de HDFS. Por ejemplo, reemplaza `hdfs://appadmin@192.168.101.11:8020/abc/` por `hdfs://appadmin@my_nameservice/abc/`.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.

<div id="storage-settings">
  ## Configuración de almacenamiento
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/es/operations/settings/settings.md#hdfs_truncate_on_insert) - permite truncar el archivo antes de insertar datos en él. Deshabilitado de forma predeterminada.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/es/operations/settings/settings.md#hdfs_create_new_file_on_insert) - permite crear un archivo nuevo en cada inserción si el formato tiene un sufijo. Deshabilitado de forma predeterminada.
* [hdfs&#95;skip&#95;empty&#95;files](/es/operations/settings/settings.md#hdfs_skip_empty_files) - permite omitir archivos vacíos durante la lectura. Deshabilitado de forma predeterminada.

**Véase también**

* [Columnas virtuales](../../../engines/table-engines/index.md#table_engines-virtual_columns)