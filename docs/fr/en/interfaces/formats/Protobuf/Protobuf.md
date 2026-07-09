---
alias: []
description: 'Documentation sur le format Protobuf'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `Protobuf` correspond au format [Protocol Buffers](https://protobuf.dev/).

Ce format nécessite un schéma de format externe, mis en cache entre les requêtes.

ClickHouse prend en charge :

* les syntaxes `proto2` et `proto3`.
* les champs `Repeated`/`optional`/`required`.

Pour établir la correspondance entre les colonnes de la table et les champs du type de message Protocol Buffers, ClickHouse compare leurs noms.
Cette comparaison est insensible à la casse, et les caractères `_` (tiret bas) et `.` (point) sont considérés comme équivalents.
Si les types d’une colonne et d’un champ du message Protocol Buffers diffèrent, la conversion nécessaire est appliquée.

Les messages imbriqués sont pris en charge. Par exemple, pour le champ `z` dans le type de message suivant :

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

ClickHouse essaie de trouver une colonne nommée `x.y.z` (ou `x_y_z`, ou `X.y_Z`, etc.).

Les messages imbriqués conviennent en entrée comme en sortie pour les [structures de données imbriquées](/fr/sql-reference/data-types/nested-data-structures/index.md).

Les valeurs par défaut définies dans un schéma Protobuf comme celui ci-dessous ne sont pas appliquées ; ce sont plutôt les [valeurs par défaut de la table](/fr/sql-reference/statements/create/table#default_values) qui sont utilisées à la place :

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

Si un message contient [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) et que `input_format_protobuf_oneof_presence` est défini, ClickHouse renseigne la colonne indiquant quel champ de oneof a été trouvé.

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

Le nom de la colonne qui indique la présence doit être le même que celui du oneof.
Les messages imbriqués sont pris en charge (voir  [basic-examples](#basic-examples)). Les messages vides sont également pris en charge.
Les types autorisés sont Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 ou Enum16.
Enum (ainsi que Enum8 ou Enum16) doit contenir tous les tags possibles du oneof, plus 0 pour indiquer l&#39;absence ; les représentations sous forme de chaînes n&#39;ont pas d&#39;importance.

Le paramètre [`input_format_protobuf_oneof_presence`](/fr/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) est désactivé par défaut

ClickHouse lit et écrit les messages Protobuf au format `length-delimited`.
Cela signifie qu&#39;avant chaque message, sa longueur doit être écrite sous la forme d&#39;un [entier à largeur variable (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="basic-examples">
  ### Lecture et écriture de données
</div>

:::note Fichiers d’exemple
Les fichiers utilisés dans cet exemple sont disponibles dans le [dépôt d’exemples](https://github.com/ClickHouse/formats/ProtoBuf)
:::

Dans cet exemple, nous allons lire des données depuis le fichier `protobuf_message.bin` dans une table ClickHouse. Nous les réécrirons ensuite
dans un fichier nommé `protobuf_message_from_clickhouse.bin` à l’aide du format `Protobuf`.

Étant donné le fichier `schemafile.proto` :

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
  <summary>Génération du fichier binaire</summary>

  Si vous savez déjà comment sérialiser et désérialiser des données au format `Protobuf`, vous pouvez ignorer cette étape.

  Nous allons utiliser Python pour sérialiser des données dans `protobuf_message.bin` et les lire dans ClickHouse.
  Si vous souhaitez utiliser un autre langage, voir aussi : [&quot;How to read/write length-delimited Protobuf messages in popular languages&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

  Exécutez la commande suivante pour générer un fichier Python nommé `schemafile_pb2.py` dans
  le même répertoire que `schemafile.proto`. Ce fichier contient les classes Python
  qui représentent votre message Protobuf `UserData` :

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  Créez maintenant un nouveau fichier Python nommé `generate_protobuf_data.py`, dans le même
  répertoire que `schemafile_pb2.py`. Collez-y le code suivant :

  ```python
  import schemafile_pb2  # Module généré par 'protoc'
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # Importe l'encodeur varint interne

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      Crée et remplit un message Protobuf UserData.
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # Les données de nos utilisateurs d'exemple
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # Ouvre le fichier binaire en mode écriture binaire ('wb')
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # Crée une instance de message Protobuf pour l'utilisateur actuel
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # Sérialise le message
          serialized_data = message.SerializeToString()

          # Récupère la longueur des données sérialisées
          message_length = len(serialized_data)

          # Utilise le _VarintBytes interne de la bibliothèque Protobuf pour encoder la longueur
          length_prefix = _VarintBytes(message_length)

          # Écrit le préfixe de longueur
          f.write(length_prefix)
          # Écrit les données du message sérialisé
          f.write(serialized_data)

  print(f"Messages Protobuf (length-delimited) écrits dans {output_filename}")

  # --- Facultatif : vérification (relecture et affichage) ---
  # Pour relire les données, nous utiliserons également le décodeur Protobuf interne pour les varints.
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- Vérification par relecture ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # Lit le fichier entier dans un buffer pour faciliter le décodage des varints
      n = 0
      while n < len(buf):
          # Décode le préfixe de longueur varint
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # Extrait les données du message
          message_data = buf[n:n+msg_len]
          n += msg_len

          # Analyse le message
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  Exécutez maintenant le script depuis la ligne de commande. Il est recommandé de l’exécuter depuis un
  environnement virtuel Python, par exemple avec `uv` :

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  Vous devrez installer les bibliothèques Python suivantes :

  ```bash
  uv pip install --upgrade protobuf
  ```

  Exécutez le script pour générer le fichier binaire :

  ```bash
  python generate_protobuf_data.py
  ```
</details>

Créez une table ClickHouse correspondant au schéma :

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

Insérez les données dans la table à partir de la ligne de commande :

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Vous pouvez également réécrire les données dans un fichier binaire au format `Protobuf` :

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

Grâce à votre schéma Protobuf, vous pouvez maintenant désérialiser les données écrites par ClickHouse dans le fichier `protobuf_message_from_clickhouse.bin`.

<div id="basic-examples-cloud">
  ### Lecture et écriture de données avec ClickHouse Cloud
</div>

Avec ClickHouse Cloud, vous ne pouvez pas téléverser de fichier de schéma Protobuf. Cependant, vous pouvez utiliser le paramètre `format_protobuf_schema`
pour préciser le schéma dans la requête. Dans cet exemple, nous vous montrons comment lire des données sérialisées depuis votre machine locale
et les insérer dans une table de ClickHouse Cloud.

Comme dans l’exemple précédent, créez la table dans ClickHouse Cloud d’après votre schéma Protobuf :

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

Le paramètre `format_schema_source` définit la source du paramètre `format_schema`

Valeurs possibles :

* &#39;file&#39; (par défaut) : non pris en charge dans Cloud
* &#39;string&#39; : `format_schema` correspond au contenu littéral du schéma.
* &#39;query&#39; : `format_schema` est une requête permettant de récupérer le schéma.

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

Pour insérer les données dans ClickHouse Cloud en spécifiant le schéma sous forme de chaîne de caractères, exécutez :

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Sélectionnez les données insérées dans la table :

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

Vous pouvez également stocker votre schéma Protobuf dans une table.

Créez une table sur ClickHouse Cloud pour y insérer des données :

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

Insérez les données dans ClickHouse Cloud en indiquant le schéma dans la requête à exécuter :

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Sélectionnez les données insérées dans la table :

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### Utilisation d’un schéma autogénéré
</div>

Si vous ne disposez pas d’un schéma Protobuf externe pour vos données, vous pouvez tout de même exporter/importer des données au format Protobuf
à l’aide d’un schéma autogénéré. Pour cela, utilisez le paramètre `format_protobuf_use_autogenerated_schema`.

Par exemple :

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

Dans ce cas, ClickHouse générera automatiquement le schéma Protobuf en fonction de la structure de la table à l’aide de la fonction
[`structureToProtobufSchema`](/fr/sql-reference/functions/other-functions#structureToProtobufSchema). Il utilisera ensuite ce schéma pour sérialiser les données au format Protobuf.

Vous pouvez également lire un fichier Protobuf à l’aide du schéma autogénéré. Dans ce cas, le fichier doit avoir été créé à l’aide du même schéma :

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

Le paramètre [`format_protobuf_use_autogenerated_schema`](/fr/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) est activé par défaut et s’applique si [`format_schema`](/fr/operations/settings/formats#format_schema) n’est pas défini.

Vous pouvez également enregistrer le schéma autogénéré dans le fichier lors des opérations d’entrée/sortie à l’aide du paramètre [`output_format_schema`](/fr/operations/settings/formats#output_format_schema). Par exemple :

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

Dans ce cas, le schéma Protobuf autogénéré sera enregistré dans le fichier `path/to/schema/schema.capnp`.

<div id="drop-protobuf-cache">
  ### Vider le cache Protobuf
</div>

Pour recharger le schéma Protobuf chargé à partir de [`format_schema_path`](/fr/operations/server-configuration-parameters/settings.md/#format_schema_path), utilisez l’instruction [`SYSTEM DROP ... FORMAT CACHE`](/fr/sql-reference/statements/system.md/#system-drop-schema-format).

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```