---
description: 'Permite ejecutar consultas `SELECT` sobre datos almacenados en un
  servidor remoto de MongoDB.'
sidebar_label: 'mongodb'
sidebar_position: 135
slug: /sql-reference/table-functions/mongodb
title: 'mongodb'
doc_type: 'reference'
---

Permite ejecutar consultas `SELECT` sobre datos almacenados en un servidor remoto de MongoDB.

<div id="syntax">
  ## Sintaxis
</div>

```sql
mongodb(host:port, database, collection, user, password, structure[, options[, oid_columns]]);
mongodb(uri, collection, structure[, oid_columns]);
mongodb(named_collection_name[, <arg>=<value>...]);
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento     | Descripción                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `host:port`   | Dirección del servidor de MongoDB.                                                                                       |
| `database`    | Nombre de la base de datos remota.                                                                                       |
| `collection`  | Nombre de la colección remota.                                                                                           |
| `user`        | Usuario de MongoDB.                                                                                                      |
| `password`    | Contraseña del usuario.                                                                                                  |
| `structure`   | El esquema de la tabla de ClickHouse que devuelve esta función.                                                          |
| `options`     | Opciones de la cadena de conexión de MongoDB (parámetro opcional).                                                       |
| `oid_columns` | Lista de columnas separadas por comas que deben tratarse como `oid` en la cláusula WHERE. `_id` de forma predeterminada. |

:::tip
Si utiliza el servicio en la nube de MongoDB Atlas, agregue estas opciones:

```ini
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

También puede conectarse mediante URI:

```sql
mongodb(uri, collection, structure[, oid_columns])
```

| Argumento     | Descripción                                                                                                  |
| ------------- | ------------------------------------------------------------------------------------------------------------ |
| `uri`         | Cadena de conexión.                                                                                          |
| `collection`  | Nombre de la colección remota.                                                                               |
| `structure`   | El esquema de la tabla de ClickHouse que devuelve esta función.                                              |
| `oid_columns` | Lista de columnas separadas por comas que deben tratarse como `oid` en la cláusula WHERE. `_id` por defecto. |
| :::           |                                                                                                              |

Puede pasar los argumentos mediante una colección nombrada:

```sql
mongodb(_named_collection_[, host][, port][, database][, collection][, user][, password][, structure][, options][, oid_columns])
-- or
mongodb(_named_collection_[, uri][, structure][, oid_columns])
```

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tipo tabla con las mismas columnas que la tabla original de MongoDB.

<div id="examples">
  ## Ejemplos
</div>

Supongamos que tenemos una colección llamada `my_collection` definida en una base de datos de MongoDB llamada `test`, e insertamos un par de documentos:

```sql
db.createUser({user:"test_user",pwd:"password",roles:[{role:"readWrite",db:"test"}]})

db.createCollection("my_collection")

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.9", command: "check-cpu-usage -w 75 -c 90" }
)

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.4", command: "system-check"}
)
```

Vamos a consultar la colección con la función de tabla `mongodb`:

```sql
SELECT * FROM mongodb(
    '127.0.0.1:27017',
    'test',
    'my_collection',
    'test_user',
    'password',
    'log_type String, host String, command String',
    'connectTimeoutMS=10000'
)
```

o:

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

o:

```sql
CREATE NAMED COLLECTION mongo_creds AS
       uri='mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
       collection='default_collection';

SELECT * FROM mongodb(
        mongo_creds,
        collection = 'my_collection',
        structure = 'log_type String, host String, command String'
)
```

<div id="related">
  ## Relacionado
</div>

* [El motor de tabla `MongoDB`](/es/engines/table-engines/integrations/mongodb.md)
* [Uso de MongoDB como fuente de un diccionario](../statements/create/dictionary/sources/mongodb.md)