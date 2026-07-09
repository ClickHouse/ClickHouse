---
alias: []
description: 'Documentación del formato Protobuf'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `Protobuf` es el formato de [Protocol Buffers](https://protobuf.dev/).

Este formato requiere un esquema de formato externo, que se guarda en caché entre consultas.

ClickHouse admite:

* las sintaxis `proto2` y `proto3`.
* campos `Repeated`/`optional`/`required`.

Para determinar la correspondencia entre las columnas de la tabla y los campos del tipo de mensaje de Protocol Buffers, ClickHouse compara sus nombres.
Esta comparación no distingue entre mayúsculas y minúsculas, y los caracteres `_` (guion bajo) y `.` (punto) se consideran equivalentes.
Si los tipos de una columna y de un campo del mensaje de Protocol Buffers son diferentes, se aplica la conversión necesaria.

Se admiten mensajes anidados. Por ejemplo, para el campo `z` en el siguiente tipo de mensaje:

```capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse intenta encontrar una columna llamada `x.y.z` (o `x_y_z` o `X.y_Z`, etc.).

Los mensajes anidados son adecuados como entrada o salida de [estructuras de datos anidadas](/es/sql-reference/data-types/nested-data-structures/index.md).

Los valores predeterminados definidos en un esquema Protobuf como el siguiente no se aplican; en su lugar, se usan los [valores predeterminados de la tabla](/es/sql-reference/statements/create/table#default_values):

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

Si un mensaje contiene [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) y está configurado `input_format_protobuf_oneof_presence`, ClickHouse rellena la columna que indica qué campo de oneof se ha encontrado.

```capnp
syntax = "proto3";

message StringOrString {
  oneof string_oneof {
    string string1 = 1;
    string string2 = 42;
  }
}
```

```sql
CREATE TABLE string_or_string ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string
```

```text
   ┌─────────┬─────────┬──────────────┐
   │ string1 │ string2 │ string_oneof │
   ├─────────┼─────────┼──────────────┤
1. │         │ string2 │ world        │
   ├─────────┼─────────┼──────────────┤
2. │ string1 │         │ hello        │
   └─────────┴─────────┴──────────────┘
```

El nombre de la columna que indica la presencia debe ser el mismo que el de `oneof`.
Se admiten mensajes anidados (consulte [basic-examples](#basic-examples)). También se admiten mensajes vacíos.
Los tipos permitidos son Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 o Enum16.
Enum (así como Enum8 o Enum16) debe contener todas las posibles etiquetas de `oneof` más 0 para indicar ausencia; las representaciones de cadena no importan.

La configuración [`input_format_protobuf_oneof_presence`](/es/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) está deshabilitada de forma predeterminada

ClickHouse recibe y genera mensajes protobuf en el formato `length-delimited`.
Esto significa que, antes de cada mensaje, su longitud debe escribirse como un [entero de longitud variable (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="basic-examples">
  ### Lectura y escritura de datos
</div>

:::note Archivos de ejemplo
Los archivos utilizados en este ejemplo están disponibles en el [repositorio de ejemplos](https://github.com/ClickHouse/formats/ProtoBuf)
:::

En este ejemplo, leeremos datos de un archivo `protobuf_message.bin` en una tabla de ClickHouse. Luego los volveremos a escribir
en un archivo llamado `protobuf_message_from_clickhouse.bin` con el formato `Protobuf`.

Dado el archivo `schemafile.proto`:

```capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

<details>
  <summary>Generación del archivo binario</summary>

  Si ya sabes cómo serializar y deserializar datos en formato `Protobuf`, puedes omitir este paso.

  Usaremos Python para serializar algunos datos en `protobuf_message.bin` y leerlos en ClickHouse.
  Si quieres usar otro lenguaje, consulta también: [&quot;Cómo leer y escribir mensajes Protobuf con delimitación por longitud en lenguajes populares&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

  Ejecuta el siguiente comando para generar un archivo de Python llamado `schemafile_pb2.py` en
  el mismo directorio que `schemafile.proto`. Este archivo contiene las clases de Python
  que representan tu mensaje `UserData` de Protobuf:

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  Ahora, crea un nuevo archivo de Python llamado `generate_protobuf_data.py` en el mismo
  directorio que `schemafile_pb2.py`. Pega en él el siguiente código:

  ```python
  import schemafile_pb2  # Módulo generado por 'protoc'
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # Importa el codificador varint interno

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      Crea y puebla un mensaje UserData de Protobuf.
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # Datos de nuestros usuarios de ejemplo
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # Abre el archivo binario en modo de escritura binaria ('wb')
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # Crea una instancia de mensaje Protobuf para el usuario actual
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # Serializa el mensaje
          serialized_data = message.SerializeToString()

          # Obtiene la longitud de los datos serializados
          message_length = len(serialized_data)

          # Usa _VarintBytes interno de la biblioteca Protobuf para codificar la longitud
          length_prefix = _VarintBytes(message_length)

          # Escribe el prefijo de longitud
          f.write(length_prefix)
          # Escribe los datos del mensaje serializado
          f.write(serialized_data)

  print(f"Protobuf messages (length-delimited) written to {output_filename}")

  # --- Opcional: Verificación (leer de nuevo e imprimir) ---
  # Para volver a leer, también usaremos el decoder interno de Protobuf para varints.
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- Verificando al volver a leer ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # Lee el archivo completo en un búfer para facilitar la decodificación de varints
      n = 0
      while n < len(buf):
          # Decodifica el prefijo de longitud varint
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # Extrae los datos del mensaje
          message_data = buf[n:n+msg_len]
          n += msg_len

          # Interpreta el mensaje
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  Ahora ejecuta el script desde la línea de comandos. Se recomienda ejecutarlo en un
  entorno virtual de Python, por ejemplo con `uv`:

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  Tendrás que instalar las siguientes bibliotecas de Python:

  ```bash
  uv pip install --upgrade protobuf
  ```

  Ejecuta el script para generar el archivo binario:

  ```bash
  python generate_protobuf_data.py
  ```
</details>

Crea una tabla de ClickHouse que coincida con el esquema:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

Inserte los datos en la tabla desde la línea de comandos:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

También puede volver a escribir los datos en un archivo binario con el formato `Protobuf`:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

Con tu esquema Protobuf, ahora puedes deserializar los datos que ClickHouse escribió en el archivo `protobuf_message_from_clickhouse.bin`.

<div id="basic-examples-cloud">
  ### Lectura y escritura de datos con ClickHouse Cloud
</div>

Con ClickHouse Cloud no es posible cargar un archivo de esquema Protobuf. Sin embargo, puede usar el ajuste `format_protobuf_schema`
para especificar el esquema en la consulta. En este ejemplo, le mostramos cómo leer datos serializados desde su máquina local
e insertarlos en una tabla en ClickHouse Cloud.

Como en el ejemplo anterior, cree la tabla según el esquema de Protobuf en ClickHouse Cloud:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

La configuración `format_schema_source` define el origen de la configuración `format_schema`

Valores posibles:

* &#39;file&#39; (predeterminado): no compatible con Cloud
* &#39;string&#39;: `format_schema` es el contenido literal del esquema.
* &#39;query&#39;: `format_schema` es una consulta para obtener el esquema.

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

Para insertar los datos en ClickHouse Cloud y especificar el esquema como una cadena, ejecute:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Seleccione los datos insertados en la tabla:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="format-schema-source-query">
  ### `format_schema_source='query'`
</div>

También puedes almacenar el esquema Protobuf en una tabla.

Crea una tabla en ClickHouse Cloud para insertar datos en ella:

```sql
CREATE TABLE testing.protobuf_schema (
  schema String
)
ENGINE = MergeTree()
ORDER BY tuple();
```

```sql
INSERT INTO testing.protobuf_schema VALUES ('syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};');
```

Inserta los datos en ClickHouse Cloud y especifica el esquema en la consulta que se va a ejecutar:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Seleccione los datos insertados en la tabla:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### Uso de un esquema autogenerado
</div>

Si no tiene un esquema Protobuf externo para sus datos, aún puede importar/exportar datos en formato Protobuf
usando un esquema autogenerado. Para ello, use la opción `format_protobuf_use_autogenerated_schema`.

Por ejemplo:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

En este caso, ClickHouse autogenerará el esquema Protobuf según la estructura de la tabla mediante la función
[`structureToProtobufSchema`](/es/sql-reference/functions/other-functions#structureToProtobufSchema). Después usará este esquema para serializar los datos en formato Protobuf.

También puede leer un archivo Protobuf con el esquema autogenerado. En este caso, es necesario que el archivo se cree con el mismo esquema:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

La configuración [`format_protobuf_use_autogenerated_schema`](/es/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) está habilitada de forma predeterminada y se aplica si no se ha establecido [`format_schema`](/es/operations/settings/formats#format_schema).

También puede guardar el esquema autogenerado en el archivo durante las operaciones de entrada/salida mediante la configuración [`output_format_schema`](/es/operations/settings/formats#output_format_schema). Por ejemplo:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

En este caso, el esquema Protobuf autogenerado se guardará en el archivo `path/to/schema/schema.capnp`.

<div id="drop-protobuf-cache">
  ### Eliminar la caché de Protobuf
</div>

Para volver a cargar el esquema Protobuf cargado desde [`format_schema_path`](/es/operations/server-configuration-parameters/settings.md/#format_schema_path), use la sentencia [`SYSTEM DROP ... FORMAT CACHE`](/es/sql-reference/statements/system.md/#system-drop-schema-format).

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```