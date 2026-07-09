---
alias: []
description: 'Documentação do formato Protobuf'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `Protobuf` é o formato [Protocol Buffers](https://protobuf.dev/).

Este formato requer um esquema de formato externo, que fica em cache entre consultas.

O ClickHouse oferece suporte a:

* às sintaxes `proto2` e `proto3`.
* a campos `Repeated`/`optional`/`required`.

Para encontrar a correspondência entre as colunas da tabela e os campos do tipo de mensagem do Protocol Buffers&#39;, o ClickHouse compara seus nomes.
Essa comparação não diferencia maiúsculas de minúsculas, e os caracteres `_` (underscore) e `.` (ponto) são considerados equivalentes.
Se os tipos de uma coluna e de um campo da mensagem do Protocol Buffers&#39; forem diferentes, a conversão necessária será aplicada.

Há suporte para mensagens aninhadas. Por exemplo, para o campo `z` no seguinte tipo de mensagem:

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

O ClickHouse tenta encontrar uma coluna chamada `x.y.z` (ou `x_y_z`, `X.y_Z` e assim por diante).

Mensagens aninhadas são adequadas para a entrada ou saída de [estruturas de dados aninhadas](/pt-BR/sql-reference/data-types/nested-data-structures/index.md).

Os valores padrão definidos em um esquema Protobuf como o mostrado a seguir não são aplicados; em vez disso, são usados os [valores padrão da tabela](/pt-BR/sql-reference/statements/create/table#default_values):

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

Se uma mensagem contiver [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) e `input_format_protobuf_oneof_presence` estiver definido, o ClickHouse preenche a coluna que indica qual campo do oneof foi encontrado.

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

O nome da coluna que indica a presença deve ser o mesmo que o nome do oneof.
Mensagens aninhadas são suportadas (consulte [basic-examples](#basic-examples)). Mensagens vazias também são suportadas.
Os tipos permitidos são Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 ou Enum16.
Enum (assim como Enum8 ou Enum16) deve conter todas as tags possíveis do oneof&#39;, além de 0 para indicar ausência; as representações em string não importam.

A configuração [`input_format_protobuf_oneof_presence`](/pt-BR/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) fica desativada por padrão

O ClickHouse lê e grava mensagens Protobuf no formato `length-delimited`.
Isso significa que, antes de cada mensagem, seu comprimento deve ser escrito como um [inteiro de largura variável (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="basic-examples">
  ### Lendo e escrevendo dados
</div>

:::note Arquivos de exemplo
Os arquivos usados neste exemplo estão disponíveis no [repositório de exemplos](https://github.com/ClickHouse/formats/ProtoBuf)
:::

Neste exemplo, vamos ler alguns dados de um arquivo `protobuf_message.bin` para uma tabela no ClickHouse. Em seguida, vamos escrevê-los
de volta em um arquivo chamado `protobuf_message_from_clickhouse.bin` usando o formato `Protobuf`.

Dado o arquivo `schemafile.proto`:

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
  <summary>Gerando o arquivo binário</summary>

  Se você já sabe como serializar e desserializar dados no formato `Protobuf`, pode pular esta etapa.

  Vamos usar Python para serializar alguns dados em `protobuf_message.bin` e carregá-los no ClickHouse.
  Se quiser usar outra linguagem, veja também: [&quot;Como ler/gravar mensagens Protobuf delimitadas por comprimento em linguagens populares&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

  Execute o comando abaixo para gerar um arquivo Python chamado `schemafile_pb2.py` no
  mesmo diretório de `schemafile.proto`. Esse arquivo contém as classes Python
  que representam sua mensagem Protobuf `UserData`:

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  Agora, crie um novo arquivo Python chamado `generate_protobuf_data.py`, no mesmo
  diretório de `schemafile_pb2.py`. Cole o código abaixo nele:

  ```python
  import schemafile_pb2  # Módulo gerado por 'protoc'
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # Importa o codificador varint interno

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      Cria e preenche uma mensagem Protobuf UserData.
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # Dados dos nossos usuários de exemplo
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # Abre o arquivo binário em modo de escrita binária ('wb')
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # Cria uma instância de mensagem Protobuf para o usuário atual
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # Serializa a mensagem
          serialized_data = message.SerializeToString()

          # Obtém o comprimento dos dados serializados
          message_length = len(serialized_data)

          # Usa o _VarintBytes interno da biblioteca Protobuf para codificar o comprimento
          length_prefix = _VarintBytes(message_length)

          # Grava o prefixo de comprimento
          f.write(length_prefix)
          # Grava os dados da mensagem serializada
          f.write(serialized_data)

  print(f"Protobuf messages (length-delimited) written to {output_filename}")

  # --- Opcional: verificação (lendo de volta e imprimindo) ---
  # Para ler de volta, também usaremos o decodificador interno do Protobuf para varints.
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- Verificando por meio da leitura ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # Lê o arquivo inteiro em um buffer para facilitar a decodificação de varint
      n = 0
      while n < len(buf):
          # Decodifica o prefixo de comprimento varint
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # Extrai os dados da mensagem
          message_data = buf[n:n+msg_len]
          n += msg_len

          # Faz o parse da mensagem
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  Agora execute o script pela linha de comando. É recomendável executá-lo em um
  ambiente virtual Python, por exemplo usando `uv`:

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  Você precisará instalar as seguintes bibliotecas Python:

  ```bash
  uv pip install --upgrade protobuf
  ```

  Execute o script para gerar o arquivo binário:

  ```bash
  python generate_protobuf_data.py
  ```
</details>

Crie uma tabela ClickHouse correspondente ao esquema:

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

Insira os dados na tabela pela linha de comando:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Você também pode gravar os dados novamente em um arquivo binário usando o formato `Protobuf`:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

Com seu esquema Protobuf, agora você pode desserializar os dados que foram gravados pelo ClickHouse no arquivo `protobuf_message_from_clickhouse.bin`.

<div id="basic-examples-cloud">
  ### Leitura e gravação de dados usando ClickHouse Cloud
</div>

No ClickHouse Cloud, não é possível enviar um arquivo de esquema Protobuf. No entanto, você pode usar a configuração `format_protobuf_schema`
para especificar o esquema na consulta. Neste exemplo, mostramos como ler dados serializados da sua máquina local
e inseri-los em uma tabela no ClickHouse Cloud.

Como no exemplo anterior, crie a tabela de acordo com o seu esquema Protobuf no ClickHouse Cloud:

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

A configuração `format_schema_source` define a origem da configuração `format_schema`

Valores possíveis:

* &#39;file&#39; (padrão): não compatível com o Cloud
* &#39;string&#39;: O `format_schema` é o conteúdo literal do esquema.
* &#39;query&#39;: O `format_schema` é uma consulta para obter o esquema.

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

Insira os dados no ClickHouse Cloud, especificando o esquema como string, execute:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Consulte os dados inseridos na tabela:

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

Você também pode armazenar seu esquema Protobuf em uma tabela.

Crie uma tabela no ClickHouse Cloud para inserir dados nela:

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

Insira os dados no ClickHouse Cloud, especificando o esquema como uma consulta a ser executada:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Selecione os dados inseridos na tabela:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### Usando esquema autogerado
</div>

Se você não tiver um esquema Protobuf externo para os seus dados, ainda poderá exportar/importar dados no formato Protobuf
usando um esquema autogerado. Para isso, use a configuração `format_protobuf_use_autogenerated_schema`.

Por exemplo:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

Nesse caso, o ClickHouse gerará automaticamente o esquema Protobuf com base na estrutura da tabela usando a função
[`structureToProtobufSchema`](/pt-BR/sql-reference/functions/other-functions#structureToProtobufSchema). Em seguida, ele usará esse esquema para serializar dados no formato Protobuf.

Você também pode ler um arquivo Protobuf com o esquema gerado automaticamente. Nesse caso, é necessário que o arquivo seja criado usando o mesmo esquema:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

A configuração [`format_protobuf_use_autogenerated_schema`](/pt-BR/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) fica habilitada por padrão e se aplica caso [`format_schema`](/pt-BR/operations/settings/formats#format_schema) não esteja definida.

Você também pode salvar o esquema gerado automaticamente no arquivo durante a entrada e a saída usando a configuração [`output_format_schema`](/pt-BR/operations/settings/formats#output_format_schema). Por exemplo:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

Neste caso, o esquema Protobuf gerado automaticamente será salvo no arquivo `path/to/schema/schema.capnp`.

<div id="drop-protobuf-cache">
  ### Remover o cache do Protobuf
</div>

Para recarregar o esquema Protobuf carregado de [`format_schema_path`](/pt-BR/operations/server-configuration-parameters/settings.md/#format_schema_path), use a instrução [`SYSTEM DROP ... FORMAT CACHE`](/pt-BR/sql-reference/statements/system.md/#system-drop-schema-format).

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```