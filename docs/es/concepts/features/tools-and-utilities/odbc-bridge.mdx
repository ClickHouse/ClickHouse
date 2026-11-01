---
description: 'Documentación de ODBC Bridge'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

Servidor HTTP sencillo que actúa como proxy para el controlador ODBC. La principal motivación
eran los posibles fallos de segmentación u otros errores en las implementaciones de ODBC, que pueden
hacer que se caiga todo el proceso de clickhouse-server.

Esta herramienta funciona mediante HTTP, no mediante pipes, memoria compartida ni TCP, porque:

* Es más sencilla de implementar
* Es más sencilla de depurar
* jdbc-bridge puede implementarse de la misma forma

<div id="usage">
  ## Uso
</div>

`clickhouse-server` usa esta herramienta dentro de la función de tabla odbc y de StorageODBC.
Sin embargo, puede usarse como herramienta independiente desde la línea de comandos con los siguientes
parámetros en la URL de la solicitud POST:

* `connection_string` -- cadena de conexión ODBC.
* `sample_block` -- descripción de columnas en el formato ClickHouse NamesAndTypesList, con el nombre entre acentos graves
  y el tipo como cadena. El nombre y el tipo están separados por un espacio; las filas, por
  saltos de línea.
* `max_block_size` -- parámetro opcional que establece el tamaño máximo de un solo bloque.
  La consulta se envía en el cuerpo de la solicitud POST. La respuesta se devuelve en formato RowBinary.

<div id="example">
  ## Ejemplo:
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```