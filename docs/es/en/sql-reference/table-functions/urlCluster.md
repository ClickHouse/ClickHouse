---
description: 'Permite procesar archivos desde una URL en paralelo desde varios nodos de un
  clúster especificado.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

Permite procesar archivos desde una URL en paralelo desde varios nodos de un clúster especificado. En el nodo iniciador, crea una conexión con todos los nodos del clúster, expande el asterisco en la ruta de archivo de la URL y asigna dinámicamente cada archivo. En el nodo worker, consulta al iniciador cuál es la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que todas las tareas hayan finalizado.

<div id="syntax">
  ## Sintaxis
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento      | Descripción                                                                                                                                                                   |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nombre de un clúster que se utiliza para construir un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                     |
| `URL`          | Dirección del servidor HTTP o HTTPS que puede aceptar peticiones `GET`. Tipo: [String](../../sql-reference/data-types/string.md).                                             |
| `format`       | [Formato](/es/sql-reference/formats) de los datos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                     |
| `structure`    | Estructura de la tabla en el formato `'UserID UInt64, Name String'`. Determina los nombres y tipos de las columnas. Tipo: [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con el formato y la estructura especificados, y con datos de la `URL` especificada.

<div id="examples">
  ## Ejemplos
</div>

Obtención de las primeras 3 líneas de una tabla que contiene columnas de tipo `String` y [UInt32](../../sql-reference/data-types/int-uint.md) desde un servidor HTTP que responde en formato [CSV](/es/interfaces/formats/CSV).

1. Cree un servidor HTTP básico con las herramientas estándar de Python 3 e inícielo:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## Globs en la URL
</div>

Los patrones entre `{ }` se utilizan para generar un conjunto de segmentos o para especificar direcciones de failover. Para consultar los tipos de patrones admitidos y ver ejemplos, consulte la descripción de la función [remote](remote.md#globs-in-addresses).
El carácter `|` dentro de los patrones se utiliza para especificar direcciones de failover. Se recorren en el mismo orden en que se enumeran en el patrón. La cantidad de direcciones generadas está limitada por la configuración [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).

<div id="related">
  ## Relacionados
</div>

* [motor HDFS](/es/engines/table-engines/integrations/hdfs)
* [función de tabla URL](/es/engines/table-engines/special/url)