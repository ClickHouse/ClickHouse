---
alias: []
description: 'Documentação do CapnProto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `CapnProto` é um formato de mensagem binária semelhante ao formato [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) e ao [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), mas diferente de [JSON](./JSON/JSON.md) ou [MessagePack](https://msgpack.org/).
As mensagens CapnProto são estritamente tipadas e não são autodescritivas, o que significa que precisam de uma descrição de esquema externa. O esquema é aplicado dinamicamente e armazenado em cache para cada consulta.

Veja também [Format Schema](/pt-BR/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## Correspondência entre tipos de dados
</div>

A tabela abaixo mostra os tipos de dados compatíveis e como eles correspondem aos [tipos de dados](/pt-BR/sql-reference/data-types/index.md) do ClickHouse em consultas `INSERT` e `SELECT`.

| Tipo de dados CapnProto (`INSERT`)                   | Tipo de dados ClickHouse                                                                                                                               | Tipo de dados CapnProto (`SELECT`)                   |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/pt-BR/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/pt-BR/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/pt-BR/sql-reference/data-types/int-uint.md), [Date](/pt-BR/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/pt-BR/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/pt-BR/sql-reference/data-types/int-uint.md), [DateTime](/pt-BR/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/pt-BR/sql-reference/data-types/int-uint.md), [Decimal32](/pt-BR/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/pt-BR/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/pt-BR/sql-reference/data-types/int-uint.md), [DateTime64](/pt-BR/sql-reference/data-types/datetime.md), [Decimal64](/pt-BR/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/pt-BR/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/pt-BR/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/pt-BR/sql-reference/data-types/string.md), [FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/pt-BR/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/pt-BR/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/pt-BR/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/pt-BR/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/pt-BR/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/pt-BR/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/pt-BR/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/pt-BR/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* Tipos inteiros podem ser convertidos entre si durante a entrada/saída.
* Para trabalhar com `Enum` no formato CapnProto, use a configuração [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/pt-BR/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode).
* Arrays podem ser aninhados e podem ter um valor do tipo `Nullable` como argumento. Os tipos `Tuple` e `Map` também podem ser aninhados.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### Inserindo e selecionando dados
</div>

Você pode inserir dados CapnProto de um arquivo em uma tabela do ClickHouse usando o seguinte comando:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Em que o `schema.capnp` fica assim:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Você pode selecionar dados de uma tabela do ClickHouse e salvá-los em um arquivo no formato `CapnProto` usando o seguinte comando:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### Usando esquema gerado automaticamente
</div>

Se você não tiver um esquema externo do `CapnProto` para seus dados, ainda será possível gerar/importar dados no formato `CapnProto` usando um esquema gerado automaticamente.

Por exemplo:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

Neste caso, o ClickHouse gerará automaticamente o esquema CapnProto com base na estrutura da tabela usando a função [structureToCapnProtoSchema](/pt-BR/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) e usará esse esquema para serializar os dados no formato CapnProto.

Você também pode ler um arquivo CapnProto com esquema gerado automaticamente (nesse caso, o arquivo deve ser criado usando o mesmo esquema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## Configurações de formato
</div>

A configuração [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) vem habilitada por padrão e se aplica quando [`format_schema`](/pt-BR/interfaces/formats#formatschema) não está definida.

Você também pode salvar o esquema gerado automaticamente em um arquivo durante a entrada/saída usando a configuração [`output_format_schema`](/pt-BR/operations/settings/formats#output_format_schema).

Por exemplo:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

Nesse caso, o esquema `CapnProto` gerado automaticamente será salvo no arquivo `path/to/schema/schema.capnp`.