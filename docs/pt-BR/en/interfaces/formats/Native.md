---
alias: []
description: 'Documentação do formato Native'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'referência'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

A especificação oficial completa do formato `Native` está disponível [aqui](/pt-BR/interfaces/specs/NativeFormat), e a especificação complementar do protocolo `Native` — o protocolo wire TCP que o transporta — está disponível [aqui](/pt-BR/interfaces/specs/NativeProtocol).

:::note
Ambas as especificações foram geradas por LLMs a partir do código-fonte do ClickHouse. O código continua sendo a principal fonte de verdade: quando houver divergência entre a especificação e o código, o código está correto.
:::

O formato `Native` é o formato mais eficiente do ClickHouse porque é realmente &quot;colunar&quot;
no sentido de que não converte colunas em linhas.

Nesse formato, os dados são gravados e lidos em [blocos](/pt-BR/development/architecture#block) em formato binário.
Para cada bloco, o número de linhas, o número de colunas, os nomes e tipos das colunas e as partes das colunas no bloco são registrados um após o outro.

Esse é o formato usado na interface nativa para interação entre servidores, no cliente de linha de comando e em clientes C++.

:::tip
Você pode usar esse formato para gerar rapidamente dumps que só podem ser lidos pelo SGBD ClickHouse.
Talvez não seja prático trabalhar diretamente com esse formato.
:::

<div id="data-types-wire-format">
  ## Formato wire dos tipos de dados
</div>

Os dados são enviados pelo wire em formato colunar, o que significa que cada coluna é enviada separadamente,
e todos os valores de uma coluna são enviados juntos como um único array.

Cada coluna em um bloco contém um cabeçalho semelhante ao de [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
Ao usar o protocolo binário TCP nativo (ou quando o endpoint HTTP recebe `?client_protocol_version=<n>`),
uma estrutura `BlockInfo` é escrita antes das contagens de colunas e linhas. Os exemplos nesta seção usam
a interface HTTP simples, sem versão de protocolo, o que omite `BlockInfo`.
:::

<div id="block-structure">
  ### Estrutura do bloco
</div>

A consulta abaixo retorna duas colunas, `number` e `str`, com três linhas:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

Os dados de saída cabem em um único bloco do ClickHouse e terão esta aparência:

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
  ### Múltiplos blocos
</div>

No entanto, em muitos casos, os dados não cabem em um único bloco, e o ClickHouse enviará os dados em vários blocos.
Considere a consulta a seguir, que retorna duas linhas com o tamanho do bloco reduzido para forçar a divisão dos dados em uma linha por bloco:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

A saída:

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
  ### Tipos de dados simples
</div>

O formato wire de um valor individual de um desses tipos de dados mais simples é semelhante ao de `RowBinary`/`RowBinaryWithNamesAndTypes`.
A lista completa dos tipos que se encaixam nessa descrição inclui:

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

Consulte as descrições dos tipos acima em [&quot;formato wire dos tipos de dados do RowBinary&quot;](/pt-BR/interfaces/formats/RowBinary#data-types-wire-format) para mais detalhes.

<div id="complex-data-types">
  ### Tipos de dados complexos
</div>

A codificação dos tipos a seguir difere da de `RowBinary` e `RowBinaryWithNamesAndTypes`.

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

No formato `Native`, uma coluna Nullable terá uma quantidade de bytes igual ao número de linhas do bloco antes dos dados propriamente ditos. Cada um desses bytes indica se o valor é `NULL` ou não. Por exemplo, nesta consulta, cada número ímpar será `NULL`:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

A saída será assim:

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

Funciona de forma semelhante com `Nullable(String)`. O indicador de nulo sempre vem do byte da máscara de nullable —
um valor de máscara `0x01` significa que a linha é `NULL`, independentemente do conteúdo da string. Para linhas `NULL`,
a string subjacente é armazenada como uma string vazia (comprimento LEB128 `0`). Observe que uma string vazia não `NULL`
também tem comprimento LEB128 `0`, portanto apenas o byte da máscara diferencia os dois casos. Por exemplo, a consulta a seguir:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

A saída será assim:

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

Ao contrário de [RowBinary](RowBinary/RowBinary.md#lowcardinality), em que `LowCardinality` é transparente, o formato Native usa uma codificação colunar baseada em dicionário. Uma coluna é codificada com um prefixo de versão, depois um dicionário de valores únicos e um array de índices inteiros para esse dicionário.

:::note
Uma coluna pode ser definida como `LowCardinality(Nullable(T))`, mas não é possível defini-la como `Nullable(LowCardinality(T))` — isso sempre resultará em um erro do servidor.
:::

O prefixo de versão é um `UInt64(LE)` com valor `1`, gravado uma vez por coluna. Depois, em cada bloco, é gravado o seguinte:

* `UInt64(LE)` — campo de bits `IndexesSerializationType`. Os bits 0–7 codificam a largura do índice (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). O bit 8 (`NeedGlobalDictionaryBit`) nunca é definido no formato Native (o servidor lança uma exceção se ele for encontrado). O bit 9 indica que há chaves adicionais do dicionário. O bit 10 indica que o dicionário deve ser reiniciado.
* `UInt64(LE)` — número de chaves do dicionário, seguido das chaves serializadas em lote usando a codificação do tipo interno.
* `UInt64(LE)` — número de linhas, seguido dos valores de índice serializados em lote usando a largura UInt apropriada.

O dicionário sempre contém um valor padrão no índice 0 (por exemplo, string vazia para `String`, 0 para tipos numéricos). Para `LowCardinality(Nullable(T))`, o índice 0 representa `NULL`, e as chaves são serializadas sem o modificador `Nullable`.

Por exemplo, `LowCardinality(String)` com 5 linhas `['foo', 'bar', 'baz', 'foo', 'bar']`:

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

Com `LowCardinality(Nullable(String))`, o índice 0 é `NULL`:

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

Ao contrário de [RowBinary](RowBinary/RowBinary.md#array), em que cada array é precedido por uma contagem de elementos em LEB128, o formato Native codifica arrays como dois subfluxos colunares:

* N offsets cumulativos `UInt64` (little-endian, 8 bytes cada). A linha `i` tem `offset[i] - offset[i-1]` elementos, com `offset[-1]` implicitamente igual a 0.
* Todos os elementos aninhados de todas as linhas, serializados em lote de forma contígua.

Por exemplo, `Array(UInt32)` com 3 linhas `[[0, 10], [1, 11], [2, 12]]`:

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

Um array vazio tem o mesmo deslocamento da linha anterior. Por exemplo, `Array(String)` com 4 linhas `[[], ['0'], ['0','1'], ['0','1','2']]`:

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

Um `Map(K, V)` é codificado como `Array(Tuple(K, V))` — offsets do array seguidos de todas as chaves e, depois, de todos os valores. Isso difere de [RowBinary](RowBinary/RowBinary.md#map), em que chaves e valores são intercalados em cada entrada.

Por exemplo, `Map(String, UInt64)` com 3 linhas `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

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

Diferentemente de [RowBinary](RowBinary/RowBinary.md#variant), em que cada linha carrega seu próprio byte discriminante seguido pelo valor inline, o formato Native separa os discriminantes dos dados.

:::warning
Assim como no RowBinary, os tipos na definição são sempre ordenados alfabeticamente, e o discriminante é o índice nessa lista ordenada. `0xFF` (255) representa `NULL`.
:::

Uma coluna `Variant` é codificada da seguinte forma:

* Prefixo `UInt64(LE)` do modo de discriminante (`0` = BASIC, `1` = COMPACT). A saída do formato Native normalmente usa BASIC (`0`); o modo COMPACT pode aparecer ao ler dados armazenados com `use_compact_variant_discriminators_serialization` habilitado.
* N discriminantes `UInt8`, um por linha.
* Os dados de cada tipo variante como uma coluna em bloco separada, contendo apenas as linhas correspondentes, em ordem de discriminante.

Por exemplo, `Variant(String, UInt32)` com 5 linhas `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (ordenado: `String` = 0, `UInt32` = 1):

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

Ao contrário de [RowBinary](RowBinary/RowBinary.md#dynamic), em que cada valor é autodescritivo (prefixo de tipo + valor), o formato Native serializa `Dynamic` como um prefixo de estrutura seguido de uma coluna [Variant](#variant).

O prefixo de estrutura contém uma versão de serialização `UInt64(LE)`, depois o número de tipos dinâmicos (como VarUInt) e, em seguida, os nomes dos tipos como strings. Na versão V1, a contagem de tipos é gravada duas vezes por compatibilidade. Os dados que vêm em seguida constituem uma coluna `Variant` cuja lista de tipos consiste nos tipos dinâmicos mais um tipo interno `SharedVariant`, ordenados em ordem alfabética.

Por exemplo, `Dynamic` com 5 linhas `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

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

Ao contrário de [RowBinary](RowBinary/RowBinary.md#json), em que cada linha é autodescritiva, com nomes de paths e valores, o formato Native serializa `JSON` em uma estrutura colunar. A codificação é complexa e depende da versão: ela consiste em um prefixo de estrutura com a versão de serialização, nomes de paths dinâmicos e o layout de dados compartilhados, seguido por paths tipados (cada um como uma coluna em bloco), paths dinâmicos (cada um como uma coluna [Dynamic](#dynamic)) e dados compartilhados para paths de overflow.

Para uma interoperabilidade mais simples, considere usar a configuração `output_format_native_write_json_as_string=1`, que serializa colunas JSON como strings simples de texto JSON (uma `String` por linha).