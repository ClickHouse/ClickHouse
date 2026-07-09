---
alias: []
description: 'Documentation sur CapnProto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `CapnProto` est un format de message binaire similaire au format [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) et à [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), mais pas à [JSON](./JSON/JSON.md) ni à [MessagePack](https://msgpack.org/).
Les messages CapnProto sont strictement typés et ne sont pas autodescriptifs, ce qui signifie qu’ils nécessitent un schéma externe. Le schéma est appliqué à la volée et mis en cache pour chaque requête.

Voir aussi [Format Schema](/fr/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## Correspondance des types de données
</div>

Le tableau ci-dessous présente les types de données pris en charge et leur correspondance avec les [types de données](/fr/sql-reference/data-types/index.md) de ClickHouse dans les requêtes `INSERT` et `SELECT`.

| Type de données CapnProto (`INSERT`)                 | Type de données ClickHouse                                                                                                                             | Type de données CapnProto (`SELECT`)                 |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/fr/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/fr/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/fr/sql-reference/data-types/int-uint.md), [Date](/fr/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/fr/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/fr/sql-reference/data-types/int-uint.md), [DateTime](/fr/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/fr/sql-reference/data-types/int-uint.md), [Decimal32](/fr/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/fr/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/fr/sql-reference/data-types/int-uint.md), [DateTime64](/fr/sql-reference/data-types/datetime.md), [Decimal64](/fr/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/fr/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/fr/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/fr/sql-reference/data-types/string.md), [FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/fr/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/fr/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/fr/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/fr/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/fr/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/fr/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/fr/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/fr/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/fr/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* Les types entiers peuvent être convertis entre eux en entrée et en sortie.
* Pour utiliser `Enum` avec le format CapnProto, utilisez le paramètre [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/fr/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode).
* Les `Array` peuvent être imbriqués et accepter comme argument une valeur de type `Nullable`. Les types `Tuple` et `Map` peuvent également être imbriqués.

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### Insertion et sélection de données
</div>

Vous pouvez insérer dans une table ClickHouse des données CapnProto à partir d’un fichier à l’aide de la commande suivante :

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

où `schema.capnp` se présente ainsi :

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Vous pouvez sélectionner des données dans une table ClickHouse et les enregistrer dans un fichier au format `CapnProto` à l’aide de la commande suivante :

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### Utilisation d’un schéma autogénéré
</div>

Si vous ne disposez pas d’un schéma `CapnProto` externe pour vos données, vous pouvez tout de même écrire/lire des données au format `CapnProto` à l’aide d’un schéma autogénéré.

Par exemple :

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

Dans ce cas, ClickHouse générera automatiquement le schéma CapnProto en fonction de la structure de la table à l’aide de la fonction [structureToCapnProtoSchema](/fr/sql-reference/functions/other-functions.md#structureToCapnProtoSchema), puis utilisera ce schéma pour sérialiser les données au format CapnProto.

Vous pouvez également lire un fichier CapnProto avec un schéma autogénéré (dans ce cas, le fichier doit être créé à l’aide du même schéma) :

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## Paramètres de format
</div>

Le paramètre [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) est activé par défaut et s&#39;applique si [`format_schema`](/fr/interfaces/formats#formatschema) n&#39;est pas défini.

Vous pouvez également enregistrer le schéma autogénéré dans un fichier lors des opérations d&#39;entrée/sortie à l&#39;aide du paramètre [`output_format_schema`](/fr/operations/settings/formats#output_format_schema).

Par exemple :

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

Dans ce cas, le schéma `CapnProto` auto-généré sera enregistré dans le fichier `path/to/schema/schema.capnp`.