---
alias: []
description: 'Documentation sur le format Native'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

La spécification officielle complète du format `Native` est disponible [ici](/fr/interfaces/specs/NativeFormat), et la spécification complémentaire du protocole `Native` — le protocole TCP sous-jacent qui le transporte — est disponible [ici](/fr/interfaces/specs/NativeProtocol).

:::note
Les deux spécifications ont été générées par des LLM à partir du code source de ClickHouse. Le code reste la source faisant autorité : en cas de divergence entre la spécification et le code, c&#39;est le code qui prévaut.
:::

Le format `Native` est le format le plus efficace de ClickHouse, car il est véritablement « colonnaire »
dans la mesure où il ne convertit pas les colonnes en lignes.

Dans ce format, les données sont écrites et lues par [blocs](/fr/development/architecture#block) au format binaire.
Pour chaque bloc, le nombre de lignes, le nombre de colonnes, les noms et types des colonnes, ainsi que les portions de colonnes contenues dans le bloc sont enregistrés les uns à la suite des autres.

Il s&#39;agit du format utilisé dans l&#39;interface native pour les échanges entre serveurs, pour le client en ligne de commande et pour les clients C++.

:::tip
Vous pouvez utiliser ce format pour générer rapidement des dumps qui ne peuvent être lus que par le SGBD ClickHouse.
Il peut être peu pratique de manipuler vous-même ce format.
:::

<div id="data-types-wire-format">
  ## Format wire des types de données
</div>

Les données sont transmises dans un format colonnaire, ce qui signifie que chaque colonne est envoyée séparément
et que toutes les valeurs d&#39;une même colonne sont envoyées ensemble sous la forme d&#39;un seul tableau.

Chaque colonne d&#39;un bloc contient un en-tête similaire à [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
Lors de l&#39;utilisation du protocole binaire TCP natif (ou lorsque l&#39;endpoint HTTP reçoit `?client_protocol_version=<n>`),
une structure `BlockInfo` est écrite avant le nombre de colonnes et de lignes. Les exemples de cette section utilisent
l&#39;interface HTTP simple, sans version de protocole, ce qui omet `BlockInfo`.
:::

<div id="block-structure">
  ### Structure de bloc
</div>

La requête suivante renvoie deux colonnes, `number` et `str`, sur trois lignes :

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

Les données de sortie tiennent dans un seul bloc ClickHouse et ressembleront à ceci :

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

<div id="multiple-blocks">
  ### Plusieurs blocs
</div>

Cependant, dans de nombreux cas, les données ne tiendront pas dans un seul bloc et ClickHouse les enverra sous forme de plusieurs blocs.
Considérez la requête suivante, qui récupère deux lignes avec une taille de bloc réduite afin de forcer le découpage des données en une ligne par bloc :

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

La sortie :

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

<div id="simple-data-types">
  ### Types de données simples
</div>

Le format wire d&#39;une valeur individuelle pour l&#39;un des types de données les plus simples est similaire à `RowBinary`/`RowBinaryWithNamesAndTypes`.
La liste complète des types correspondant à cette description comprend :

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* DateTime
* DateTime64
* IPv4
* IPv6
* UUID

Consultez les descriptions des types ci-dessus dans [« Format wire des types de données RowBinary »](/fr/interfaces/formats/RowBinary#data-types-wire-format) pour plus de détails.

<div id="complex-data-types">
  ### Types de données complexes
</div>

L’encodage des types suivants diffère de celui de `RowBinary` et `RowBinaryWithNamesAndTypes`.

* Nullable
* LowCardinality
* Array
* Map
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### Nullable
</div>

Au format `Native`, une colonne Nullable comporte, avant les données proprement dites, un nombre d’octets égal au nombre de lignes du bloc. Chacun de ces octets indique si la valeur est `NULL` ou non. Par exemple, avec cette requête, chaque nombre impair sera `NULL` :

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

Le résultat ressemblera à ceci :

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

Cela fonctionne de façon similaire avec `Nullable(String)`. L&#39;indicateur de valeur nulle provient toujours de l&#39;octet de masque du type nullable —
une valeur de masque de `0x01` signifie que la ligne est `NULL`, quel que soit le contenu de la chaîne. Pour les lignes `NULL`,
la chaîne sous-jacente est stockée sous la forme d&#39;une chaîne vide (longueur LEB128 `0`). Notez qu&#39;une chaîne vide non `NULL`
a également une longueur LEB128 de `0` ; seul l&#39;octet de masque permet donc de distinguer les deux cas. Par exemple, la requête suivante :

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

La sortie ressemblera à ceci :

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

<div id="lowcardinality">
  #### LowCardinality
</div>

Contrairement à [RowBinary](RowBinary/RowBinary.md#lowcardinality), où `LowCardinality` est transparent, le format Native utilise un encodage colonnaire basé sur un dictionnaire. Une colonne est encodée avec un préfixe de version, puis un dictionnaire de valeurs uniques, ainsi qu’un tableau d’index entiers dans ce dictionnaire.

:::note
Une colonne peut être définie comme `LowCardinality(Nullable(T))`, mais il n’est pas possible de la définir comme `Nullable(LowCardinality(T))` — cela entraîne toujours une erreur du serveur.
:::

Le préfixe de version est un `UInt64(LE)` de valeur `1`, écrit une fois par colonne. Ensuite, pour chaque bloc, les éléments suivants sont écrits :

* `UInt64(LE)` — champ de bits `IndexesSerializationType`. Les bits 0–7 encodent la largeur de l’index (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). Le bit 8 (`NeedGlobalDictionaryBit`) n’est jamais défini dans le format Native (le serveur lève une exception s’il le rencontre). Le bit 9 indique que des clés de dictionnaire supplémentaires sont présentes. Le bit 10 indique que le dictionnaire doit être réinitialisé.
* `UInt64(LE)` — nombre de clés du dictionnaire, suivi des clés sérialisées en bloc à l’aide de l’encodage du type interne.
* `UInt64(LE)` — nombre de lignes, suivi des valeurs d’index sérialisées en bloc à l’aide de la largeur UInt appropriée.

Le dictionnaire contient toujours une valeur par défaut à l’index 0 (par exemple, une chaîne vide pour `String`, 0 pour les types numériques). Pour `LowCardinality(Nullable(T))`, l’index 0 représente `NULL`, et les clés sont sérialisées sans l’enveloppe `Nullable`.

Par exemple, `LowCardinality(String)` avec 5 lignes `['foo', 'bar', 'baz', 'foo', 'bar']` :

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

Avec `LowCardinality(Nullable(String))`, l’index 0 est `NULL` :

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

<div id="array">
  #### Array
</div>

Contrairement à [RowBinary](RowBinary/RowBinary.md#array), où chaque tableau est précédé du nombre d’éléments encodé en LEB128, le Native format encode les tableaux sous forme de deux sous-flux colonnaires :

* N offsets `UInt64` cumulatifs (little-endian, 8 octets chacun). La ligne `i` contient `offset[i] - offset[i-1]` éléments, avec `offset[-1]` implicitement égal à 0.
* Tous les éléments imbriqués de toutes les lignes, sérialisés en bloc de façon contiguë.

Par exemple, `Array(UInt32)` avec 3 lignes `[[0, 10], [1, 11], [2, 12]]` :

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

Un tableau vide a le même offset que la ligne précédente. Par exemple, `Array(String)` avec 4 lignes `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

<div id="map">
  #### Map
</div>

Un `Map(K, V)` est encodé sous la forme `Array(Tuple(K, V))` — les offsets du tableau, suivis de toutes les clés, puis de toutes les valeurs. Cela diffère de [RowBinary](RowBinary/RowBinary.md#map), où les clés et les valeurs sont entrelacées entrée par entrée.

Par exemple, `Map(String, UInt64)` avec 3 lignes `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]` :

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

<div id="variant">
  #### Variant
</div>

Contrairement à [RowBinary](RowBinary/RowBinary.md#variant), où chaque ligne contient son propre octet discriminant suivi de la valeur intégrée, le format Native sépare les discriminants des données.

:::warning
Comme avec RowBinary, les types dans la définition sont toujours triés par ordre alphabétique, et le discriminant correspond à l’indice dans cette liste triée. `0xFF` (255) représente `NULL`.
:::

Une colonne `Variant` est encodée comme suit :

* préfixe du mode des discriminants `UInt64(LE)` (`0` = BASIC, `1` = COMPACT). La sortie du format Native utilise généralement BASIC (`0`) ; le mode COMPACT peut apparaître lors de la lecture de données stockées avec `use_compact_variant_discriminators_serialization` activé.
* N discriminants `UInt8`, un par ligne.
* Les données de chaque type de variant sous la forme d’une colonne groupée distincte contenant uniquement les lignes correspondantes, dans l’ordre des discriminants.

Par exemple, `Variant(String, UInt32)` avec 5 lignes `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (triées : `String` = 0, `UInt32` = 1) :

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

<div id="dynamic">
  #### Dynamic
</div>

Contrairement à [RowBinary](RowBinary/RowBinary.md#dynamic), où chaque valeur est auto-descriptive (préfixe de type + valeur), le format Native sérialise `Dynamic` sous la forme d’un préfixe de structure suivi d’une colonne [Variant](#variant).

Le préfixe de structure contient une version de sérialisation `UInt64(LE)`, puis le nombre de types dynamiques (sous forme de VarUInt), puis les noms de type sous forme de chaînes. Dans la version V1, le nombre de types est écrit deux fois pour assurer la compatibilité. Les données qui suivent forment une colonne `Variant` dont la liste de types correspond aux types dynamiques, plus un type interne `SharedVariant`, triés par ordre alphabétique.

Par exemple, `Dynamic` avec 5 lignes `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` :

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

<div id="json">
  #### JSON
</div>

Contrairement à [RowBinary](RowBinary/RowBinary.md#json), où chaque ligne s&#39;auto-décrit avec des noms de chemin et des valeurs, le format Native sérialise `JSON` dans une structure colonnaire. L&#39;encodage est complexe et dépend de la version : il se compose d&#39;un préfixe de structure contenant la version de sérialisation, les noms de chemins dynamiques et l&#39;organisation des données partagées, suivis des chemins typés (chacun sous forme de bulk column), des chemins dynamiques (chacun sous forme de colonne [Dynamic](#dynamic)) et des données partagées pour les chemins de débordement.

Pour une interopérabilité plus simple, envisagez d&#39;utiliser le paramètre `output_format_native_write_json_as_string=1`, qui sérialise les colonnes JSON sous forme de simples chaînes de texte JSON (une `String` par ligne).