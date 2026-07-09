---
alias: []
description: 'Documentação sobre o formato RowBinary'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `RowBinary` analisa os dados linha por linha em formato binário.
As linhas e os valores são listados consecutivamente, sem separadores.
Como os dados estão em formato binário, o delimitador após `FORMAT RowBinary` é especificado estritamente da seguinte forma:

* Qualquer quantidade de espaços em branco:
  * `' '` (espaço - código `0x20`)
  * `'\t'` (tabulação - código `0x09`)
  * `'\f'` (avanço de página - código `0x0C`)
* Seguido de exatamente uma sequência de nova linha:
  * estilo Windows `"\r\n"`
  * ou estilo Unix `'\n'`
* Seguido imediatamente por dados binários.

:::note
Este formato é menos eficiente que o formato [Native](../Native.md), pois é baseado em linhas.
:::

<div id="data-types-wire-format">
  ## Formato wire dos tipos de dados
</div>

:::tip
A maioria das consultas fornecidas nos exemplos pode ser executada com curl com saída para arquivo.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

Em seguida, os dados podem ser examinados com um editor hexadecimal.

<div id="unsigned-leb128">
  ### LEB128 sem sinal (Little Endian Base 128)
</div>

Uma codificação de inteiro sem sinal, de largura variável, em **little-endian**, usada para codificar o comprimento de tipos de dados de tamanho variável, como `String`, `Array` e `Map`. Uma implementação de exemplo pode ser encontrada na [página da wiki do LEB128](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

Todos os tipos inteiros são codificados com um número apropriado de bytes em **little-endian**. Os tipos com sinal (`Int8` a `Int256`) usam a representação em **complemento de dois**. A maioria das linguagens oferece suporte à extração desses inteiros de arrays de bytes, usando ferramentas nativas ou bibliotecas amplamente conhecidas. Para `Int128`/`Int256` e `UInt128`/`UInt256`, que excedem os tamanhos nativos de inteiros da maioria das linguagens, pode ser necessária uma desserialização personalizada.

<div id="bool">
  ### Bool
</div>

Valores booleanos são codificados como um único byte e podem ser desserializados de forma semelhante a `UInt8`.

* `0` é `false`
* `1` é `true`

<div id="float32-float64">
  ### Float32, Float64
</div>

**Little-endian** para números de ponto flutuante, codificados em 4 bytes para `Float32` e em 8 bytes para `Float64`. Assim como no caso dos inteiros, a maioria das linguagens oferece ferramentas adequadas para desserializar esses valores.

<div id="bfloat16">
  ### BFloat16
</div>

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) é um formato de ponto flutuante de 16 bits com a faixa de Float32 e precisão reduzida, o que o torna útil para cargas de trabalho de machine learning. O formato wire é essencialmente os 16 bits mais altos de um valor Float32. Se sua linguagem não oferecer suporte nativo a isso, a maneira mais fácil de lidar com ele é ler e gravar como UInt16, convertendo de e para Float32:

Para converter BFloat16 em Float32 (pseudocódigo):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

Para converter Float32 em BFloat16 (pseudocódigo):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

Exemplos de valores subjacentes para `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

Os tipos Decimal são representados como inteiros **little-endian** com a respectiva largura de bits.

* `Decimal32` - 4 bytes, ou `Int32`.
* `Decimal64` - 8 bytes, ou `Int64`.
* `Decimal128` - 16 bytes, ou `Int128`.
* `Decimal256` - 32 bytes, ou `Int256`.

Ao desserializar um valor Decimal, as partes inteira e fracionária podem ser obtidas usando o seguinte pseudocódigo:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

Onde `trunc` realiza o truncamento em direção a zero (e não a divisão por piso, que difere para valores negativos), e `scale` é o número de dígitos após o separador decimal. Por exemplo, para `Decimal(10, 2)` (equivalente a `Decimal32(2)`), a escala é `2`, e o valor `12345` será representado como `(123, 45)`.

A serialização requer a operação inversa:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

Veja mais detalhes na [Documentação do ClickHouse sobre os tipos Decimal](https://clickhouse.com/docs/sql-reference/data-types/decimal).

<div id="string">
  ### String
</div>

As strings do ClickHouse são **sequências arbitrárias de bytes**. Não precisam ser UTF-8 válidas. O prefixo de comprimento é o **tamanho em bytes**, não a quantidade de caracteres.

Codificada em duas partes:

1. Um inteiro de comprimento variável (LEB128) que indica o tamanho da string em bytes.
2. Os bytes brutos da string.

Por exemplo, uma string `foobar` será codificada usando *sete* bytes da seguinte forma:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

<div id="fixedstring">
  ### FixedString
</div>

Ao contrário de `String`, `FixedString` tem comprimento fixo, definido no esquema. Ele é codificado como uma sequência de bytes, preenchida com bytes zero no final caso o valor seja menor que `N`.

:::note
Ao ler um `FixedString`, os bytes zero no final podem ser tanto padding quanto caracteres `\0` reais nos dados; eles são indistinguíveis on the wire. O próprio ClickHouse preserva todos os `N` bytes exatamente como estão.
:::

Um `FixedString(3)` vazio contém apenas zeros de padding:

```text
0x00, 0x00, 0x00
```

`FixedString(3)` não vazia contendo a cadeia de caracteres `hi`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

`FixedString(3)` não vazia contendo a string `bar`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

Nenhum preenchimento é necessário no último exemplo, já que os *três* bytes são todos usados.

<div id="date">
  ### Date
</div>

Armazenado como `UInt16` (dois bytes), representando o número de dias transcorridos desde `1970-01-01`.

Faixa de valores suportada: `[1970-01-01, 2149-06-06]`.

Exemplos de valores subjacentes para `Date`:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

Armazenado como `Int32` (quatro bytes), representando o número de dias ***antes ou depois*** de `1970-01-01`.

Intervalo de valores suportado: `[1900-01-01, 2299-12-31]`.

Exemplos de valores subjacentes para `Date32`:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

Uma data anterior à epoch:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### DateTime
</div>

Armazenado como `UInt32` (quatro bytes), representando o número de segundos decorridos ***desde*** `1970-01-01 00:00:00 UTC`.

Sintaxe:

```text
DateTime([timezone])
```

Por exemplo, `DateTime` ou `DateTime('UTC')`.

:::note
O valor binário é sempre um deslocamento do epoch UTC. O fuso horário não altera a codificação. No entanto, o fuso horário **de fato** afeta como valores de string são interpretados na inserção: inserir `'2024-01-15 10:30:00'` em uma coluna `DateTime('America/New_York')` armazena um valor de epoch diferente de inserir a mesma string em uma coluna `DateTime('UTC')`, porque a string é interpretada como hora local no fuso horário da coluna. No wire, ambos são apenas segundos de epoch `UInt32`.
:::

Intervalo de valores compatível: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

Exemplo de valores subjacentes para `DateTime`:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

<div id="datetime64">
  ### DateTime64
</div>

Armazenado como `Int64` (oito bytes), representando o número de **ticks** ***antes ou após*** `1970-01-01 00:00:00 UTC`. A resolução do tick é definida pelo parâmetro `precision`; veja a sintaxe abaixo:

```text
DateTime64(precision, [timezone])
```

Em que `precision` é um número inteiro de `0` a `9`. Normalmente, apenas os seguintes valores são usados: `3` (milissegundos), `6` (microssegundos),
`9` (nanossegundos).

Exemplos de definições válidas de `DateTime64`: `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')` ou `DateTime64(9, 'Europe/Amsterdam')`.

:::note
Assim como em `DateTime`, o valor binário é sempre um deslocamento em relação ao epoch UTC. O fuso horário afeta como os valores de string são interpretados na inserção (consulte a observação sobre [DateTime](#datetime)), mas a codificação em si sempre corresponde a ticks `Int64` desde o epoch UTC.
:::

O valor `Int64` subjacente do tipo `DateTime64` pode ser interpretado como a quantidade das seguintes unidades antes ou depois do Unix epoch:

* `DateTime64(0)` - segundos.
* `DateTime64(3)` - milissegundos.
* `DateTime64(6)` - microssegundos.
* `DateTime64(9)` - nanossegundos.

Intervalo de valores com suporte: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

Exemplos de valores subjacentes para `DateTime64`:

* `DateTime64(3)`: o valor `1546300800000` representa `2019-01-01 00:00:00 UTC`.
* `DateTime64(6)`: o valor `1705314600123456` representa `2024-01-15 10:30:00.123456 UTC`.
* `DateTime64(9)`: o valor `1705314600123456789` representa `2024-01-15 10:30:00.123456789 UTC`.

:::note
A precisão do valor máximo é 8. Se a precisão máxima de 9 dígitos (nanossegundos) for usada, o valor máximo com suporte será 2262-04-11 23:47:16 em UTC.
:::

<div id="time">
  ### Time
</div>

Armazenado como `Int32`, representando um valor de tempo em segundos. Valores negativos são válidos.

Faixa de valores suportada: `[-999:59:59, 999:59:59]` (ou seja, `[-3599999, 3599999]` segundos).

:::note
No momento, a configuração `enable_time_time64_type` deve ser definida como `1` para usar `Time` ou `Time64`.
:::

Valores subjacentes de exemplo para `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

<div id="time64">
  ### Time64
</div>

Armazenado internamente como um `Decimal64` (que, por sua vez, é armazenado como `Int64`), representando um valor de tempo com frações de segundo e precisão configurável. Valores negativos são válidos.

Sintaxe:

```text
Time64(precision)
```

Em que `precision` é um inteiro de `0` a `9`. Valores comuns: `3` (milissegundos), `6` (microssegundos), `9` (nanossegundos).

Intervalo de valores aceito: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
No momento, a configuração `enable_time_time64_type` deve ser definida como `1` para usar `Time` ou `Time64`.
:::

O valor `Int64` subjacente representa frações de segundo escaladas por `10^precision`.

Exemplos de valores subjacentes para `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

<div id="interval-types">
  ### Tipos de intervalo
</div>

Todos os tipos de intervalo são armazenados como `Int64` (oito bytes, little-endian). O valor representa a quantidade da respectiva unidade de tempo. Valores negativos são válidos.

Os tipos de intervalo são: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
O nome do tipo de intervalo (por exemplo, `IntervalSecond` vs `IntervalDay`) determina a unidade do valor armazenado. A codificação wire é sempre a mesma.
:::

Exemplos de valores subjacentes:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

<div id="enum8-enum16">
  ### Enum8, Enum16
</div>

Armazenado como um único byte (`Enum8` == `Int8`) ou dois bytes (`Enum16` == `Int16`), representando o índice do valor `enum` na definição do `enum`. Observe que o tipo de armazenamento é **com sinal** — os valores de `enum` podem ser negativos (por exemplo, `Enum8('a' = -128, 'b' = 0)`).

Um Enum pode ser definido de forma simples, assim:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

O Enum8 definido acima terá o seguinte mapeamento de valores no cliente:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

Ou, de forma mais complexa, assim:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

O Enum16 definido acima terá o seguinte mapeamento de valores no cliente:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

Para o parser de tipo de dados, o principal desafio é acompanhar os símbolos escapados na definição do enum, como `\'`, e símbolos especiais como `=` que podem aparecer dentro de strings entre aspas.

<div id="uuid">
  ### UUID
</div>

Representado como uma sequência de 16 bytes. O UUID é armazenado como **dois valores `UInt64` little-endian**: os primeiros 8 bytes da representação padrão do UUID têm os bytes invertidos, e os 8 bytes seguintes têm os bytes invertidos independentemente.

Por exemplo, dado o UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0`:

* Representação padrão em bytes: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* Primeira metade invertida (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* Segunda metade invertida (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

Exemplos de valores subjacentes para `UUID`:

* `61f0c404-5cb3-11e7-907b-a6006ad3dba0` é representado como:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* O UUID padrão `00000000-0000-0000-0000-000000000000` é representado como 16 bytes com valor zero:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

Pode ser usado quando um novo registro foi inserido, mas o UUID não foi especificado.

<div id="ipv4">
  ### IPv4
</div>

Armazenado em quatro bytes como `UInt32`, na ordem de bytes **little-endian**. Observe que isso difere da ordem de bytes de rede tradicional (big-endian), comumente usada para endereços IP. Valores subjacentes de exemplo para `IPv4`:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

<div id="ipv6">
  ### IPv6
</div>

Armazenado em 16 bytes, em **big-endian / ordem de bytes de rede** (MSB primeiro). Exemplos de valores internos para `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

<div id="nullable">
  ### Nullable
</div>

Um tipo de dado Nullable é codificado da seguinte forma:

1. Um único byte que indica se o valor é `NULL` ou não:
   * `0x00` significa que o valor não é `NULL`.
   * `0x01` significa que o valor é `NULL`.
2. Se o valor não for `NULL`, o tipo de dado subjacente é codificado normalmente. Se o valor for `NULL`, **nenhum byte adicional** é escrito para o tipo subjacente.

Por exemplo, um valor `Nullable(UInt32)`:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

<div id="lowcardinality">
  ### LowCardinality
</div>

No formato RowBinary, o marcador de baixa cardinalidade não afeta o formato wire. Por exemplo, `LowCardinality(String)` é codificado da mesma forma que um `String` обычário.

:::warning
Isso se aplica apenas ao RowBinary. No Native format, `LowCardinality` usa uma codificação diferente, baseada em dicionário.
:::

:::note
Uma coluna pode ser definida como `LowCardinality(Nullable(T))`, mas não é possível defini-la como `Nullable(LowCardinality(T))` — isso sempre resultará em um erro do servidor.
:::

Durante os testes, [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) pode ser definido como `1` para permitir a maioria dos tipos de dados em `LowCardinality`, proporcionando uma cobertura melhor.

<div id="array">
  ### Array
</div>

Um array é codificado da seguinte maneira:

1. Um [inteiro de comprimento variável (LEB128)](#unsigned-leb128) que indica o número de elementos no array.
2. Os elementos do array, codificados da mesma forma que o tipo de dado subjacente.

Por exemplo, um array com valores `UInt32`:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

Um exemplo um pouco mais complexo:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
Um array pode conter valores Nullable, mas o próprio array não pode ser Nullable.
:::

O seguinte é válido:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

E será codificado da seguinte forma:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

Um exemplo de como lidar com arrays multidimensionais pode ser encontrado na [seção Geo](#geo-types).

<div id="tuple">
  ### Tuple
</div>

Uma tupla é codificada como todos os elementos da tupla em sequência, cada um no respectivo formato wire, sem nenhuma metainformação adicional nem delimitadores.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

A representação em string do tipo de dado Tuple apresenta desafios semelhantes aos do [tipo Enum](#enum8-enum16), como acompanhar os símbolos escapados e os caracteres especiais; agora, com Tuple, também é necessário acompanhar os parênteses de abertura e fechamento. Além disso, observe que os Tuples mais complexos podem conter outros Tuples aninhados, Arrays, Maps e até enums.

Por exemplo, na tabela a seguir, o Tuple contém um enum com uma crase e parênteses no nome, o que pode causar problemas de parsing se não for tratado corretamente:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

<div id="map">
  ### Map
</div>

Um map pode ser visto como um `Array(Tuple(K, V))`, em que `K` é o tipo da chave e `V` é o tipo do valor. O map é codificado da seguinte forma:

1. Um [inteiro de tamanho variável (LEB128)](#unsigned-leb128) que indica o número de elementos no map.
2. Os elementos do map como pares chave-valor, codificados de acordo com seus tipos correspondentes.

Por exemplo, um map com chaves `String` e valores `UInt32`:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
É possível ter maps com estruturas aninhadas em vários níveis, como `Map(String, Map(Int32, Array(Nullable(String))))`, que serão codificados de maneira semelhante à descrita acima.
:::

<div id="variant">
  ### Variant
</div>

Esse tipo representa uma união de outros tipos de dados. O tipo `Variant(T1, T2, ..., TN)` significa que cada linha desse tipo tem um valor do tipo `T1`, `T2`, …, `TN` ou nenhum deles (valor `NULL`).

:::warning
Embora, para o usuário final, `Variant(T1, T2)` signifique exatamente a mesma coisa que `Variant(T2, T1)`, a ordem dos tipos na definição importa para o formato wire: os tipos na definição são sempre ordenados alfabeticamente, e isso é importante, pois a variante exata é codificada por um &quot;discriminante&quot; — o índice do tipo de dado na definição.
:::

Considere o exemplo a seguir:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

Um valor `NULL` é codificado com um byte discriminante de `0xFF`:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

A configuração [allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) pode ser usada para permitir testes mais abrangentes do tipo `Variant`.

<div id="dynamic">
  ### Dynamic
</div>

O tipo `Dynamic` pode armazenar valores de qualquer tipo, determinados em tempo de execução. No formato RowBinary, cada valor é autodescritivo: a primeira parte é a especificação do tipo, em [este formato](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding). Em seguida vem o conteúdo, com a codificação do valor conforme descrita neste documento. Portanto, para fazer o parsing de um valor, basta usar o índice de tipo para determinar o parser correto e então reutilizar o parsing de RowBinary que você já usa em outro lugar.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

Em que `BinaryTypeIndex` é um único byte que identifica o tipo. Consulte a [referência](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding) aqui para ver os índices e parâmetros de tipo.

Um valor Dynamic `NULL` é codificado com `BinaryTypeIndex` `0x00` (o tipo `Nothing`), sem bytes adicionais:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**Exemplos:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

<div id="json">
  ### JSON
</div>

O tipo JSON codifica dados em duas categorias distintas:

1. **Caminhos tipados** - Caminhos declarados com tipos explícitos no esquema (por exemplo, `JSON(user_id UInt32, name String)`)
2. **Caminhos dinâmicos/caminhos de overflow quando o limite de caminhos dinâmicos é excedido** - Caminhos descobertos em tempo de execução e armazenados como tipo `Dynamic`. A codificação do valor é precedida pela definição do tipo.

O wire format e as regras são diferentes para essas duas categorias.

| Categoria do caminho              | Incluído na serialização   | Codificação do valor               | Variant/Nullable permitidos |
| --------------------------------- | -------------------------- | ---------------------------------- | --------------------------- |
| **Caminhos com tipagem definida** | Sempre (mesmo se for NULL) | Formato binário específico do tipo | Sim                         |
| **Caminhos do tipo Dynamic**      | Apenas se não for NULL     | Dynamic                            | Não                         |

Os paths são serializados em três grupos, escritos sequencialmente: paths tipados, paths dinâmicos e, por fim, paths de shared data (overflow). Os paths tipados e dinâmicos são escritos em uma ordem implementation-defined (determinada pela iteração interna do hash-map), enquanto os paths de shared data são escritos em ordem alfabética. Os leitores não devem depender de nenhuma ordenação específica de paths. O desserializador despacha cada path pelo nome, não pela posição.

Cada linha JSON no formato RowBinary é serializada como:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**Exemplos:**

**1. JSON simples somente com paths tipados:**

Schema: `JSON(user_id UInt32, active Bool)`

Linha: `{"user_id": 42, "active": true}`

Codificação binária (hex com anotações):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. JSON simples com caminhos tipados e dinâmicos:**

Schema: `JSON(user_id UInt32, active Bool)`

Linha: `{"user_id": 42, "active": true, "name": "Alice"}`

Codificação binária (hex com anotações):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. Tratamento de NULL:**

Com uma coluna Nullable tipada, você obtém null:

Schema: `JSON(score Nullable(Int32))`

Linha: `{"score": null }`

Codificação binária (hex com anotações):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

Com uma coluna tipada não nulável, você obtém o valor padrão:

Schema: `JSON(name String)`

Linha: `{"name": null}`

Codificação binária:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

Com um caminho dinâmico, ele é ignorado:

Schema: `JSON(id UInt64)`

Linha: `{"id": 100, "metadata": null}`

Codificação binária:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

Nota: O caminho `metadata` com valor NULL **não está incluído** porque caminhos dinâmicos são serializados apenas quando não são nulos. Essa é uma diferença fundamental em relação aos caminhos tipados.

**4. Objetos JSON aninhados:**

Esquema: `JSON()`

Linha: `{"user": {"name": "Bob", "age": 30}}`

Codificação binária (hex com anotações):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

Nota: objetos aninhados são achatados em caminhos separados por ponto (por exemplo, `user.name` em vez de uma estrutura aninhada).

**Alternativa: JSON no modo String**

Com a configuração `output_format_binary_write_json_as_string=1`, colunas JSON são serializadas como uma única string de texto JSON em vez do formato binário estruturado. Há uma configuração correspondente para gravar em colunas JSON, `input_format_binary_read_json_as_string`. A escolha da configuração aqui depende de você querer fazer o parse do JSON no cliente ou no servidor.

<div id="geo-types">
  ### Tipos Geo
</div>

Geo é uma categoria de tipos de dados que representam dados geográficos. Ela inclui:

* `Point` - como `Tuple(Float64, Float64)`.
* `Ring` - como `Array(Point)`, ou `Array(Tuple(Float64, Float64))`.
* `Polygon` - como `Array(Ring)`, ou `Array(Array(Tuple(Float64, Float64)))`.
* `MultiPolygon` - como `Array(Polygon)`, ou `Array(Array(Array(Tuple(Float64, Float64))))`.
* `LineString` - como `Array(Point)`, ou `Array(Tuple(Float64, Float64))`.
* `MultiLineString` - como `Array(LineString)`, ou `Array(Array(Tuple(Float64, Float64)))`.

O formato wire dos valores Geo é exatamente o mesmo de Tuple e Array. Os cabeçalhos do formato `RowBinaryWithNamesAndTypes` conterão os aliases desses tipos, por exemplo, `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString` e `MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

<div id="geometry">
  ### Geometry
</div>

`Geometry` é um tipo `Variant` que pode armazenar qualquer um dos tipos Geo listados acima. Na wire, ele é codificado exatamente como um `Variant`, com um byte discriminante indicando qual tipo Geo vem em seguida.

Os índices do discriminante para `Geometry` são:

| Índice | Tipo            |
| ------ | --------------- |
| 0      | LineString      |
| 1      | MultiLineString |
| 2      | MultiPolygon    |
| 3      | Point           |
| 4      | Polygon         |
| 5      | Ring            |

Estrutura do formato wire:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

Exemplo de codificação de um `Point` como `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

Exemplo de codificação de um `Ring` como `Geometry`:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

<div id="nested">
  ### Nested
</div>

O formato wire de `Nested` depende da configuração `flatten_nested`.

:::warning
Todos os arrays de componentes em uma única linha **devem ter o mesmo tamanho**. Esta é uma restrição imposta pelo servidor. Tamanhos diferentes causarão erros de inserção.
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (padrão)
</div>

Com a configuração padrão, `Nested` é desmembrado em arrays independentes. Cada subcoluna se torna uma coluna `Array` separada, com um nome separado por ponto:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` mostra as colunas desaninhadas:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

Cada array é serializado independentemente, conforme descrito na seção [Array](#array):

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

<div id="nested-unflattened">
  #### `flatten_nested = 0`
</div>

Com `flatten_nested = 0`, `Nested` é mantido como uma única coluna do tipo `Array(Tuple(...))`. O nome da coluna não é separado por pontos:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo` mostra uma única coluna:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

A codificação é `Array(Tuple(String, Int32))`: um prefixo com o comprimento do array, seguido pelos campos da tupla de cada elemento, em ordem:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

Observe como os campos são intercalados em cada elemento (a₁, b₁, a₂, b₂), em vez de serem agrupados por coluna (a₁, a₂, b₁, b₂), como na representação achatada.

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

`SimpleAggregateFunction(func, T)` é codificada de forma idêntica ao seu tipo de dado subjacente `T`. O nome da função de agregação não afeta o formato wire.

Por exemplo, `SimpleAggregateFunction(max, UInt32)` é codificada da mesma forma que um `UInt32` simples:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

O cabeçalho RowBinaryWithNamesAndTypes informa o tipo como `SimpleAggregateFunction(max, UInt32)`, mas o valor transmitido é apenas um `UInt32`:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

`AggregateFunction(func, T)` armazena o estado intermediário completo de uma função de agregação. Ao contrário de `SimpleAggregateFunction`, que também armazena um estado intermediário, mas o codifica de forma idêntica ao tipo de dado subjacente, `AggregateFunction` armazena um blob binário opaco cujo formato é específico de cada função de agregação.

:::warning
Estados de agregação **não têm prefixo de comprimento** em RowBinary. Um analisador precisa entender o formato interno de serialização de cada função de agregação específica para saber quantos bytes consumir. Na prática, a maioria dos clientes trata estados de agregação como opacos e usa os combinadores `*State` / `*Merge` para deixar o servidor cuidar da serialização.
:::

O formato interno varia de acordo com a função. Alguns exemplos simples:

**`countState`** — armazena a contagem como um VarUInt (LEB128):

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — armazena a soma acumulada em um inteiro de tamanho fixo. A largura depende do tipo de argumento (`UInt64` para argumentos inteiros):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — armazena um byte de sinalizador seguido do valor no tipo subjacente. O sinalizador é `0x00` para um estado vazio (nenhum valor foi observado) ou `0x01` quando há um valor presente:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

Um estado vazio (sem linhas agregadas):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
Funções mais complexas, como `uniq`, `quantile` ou `groupArray`, usam formatos específicos de implementação. Se você precisar ler ou gravar esses estados, consulte o código-fonte do ClickHouse da função específica.
:::

<div id="qbit">
  ### QBit
</div>

`QBit` é um tipo de vetor para busca eficiente com diferentes níveis de precisão. Internamente, ele é armazenado em um formato transposto. No wire, o QBit é simplesmente um `Array` do tipo de elemento subjacente (`Int8`, `Float32`, `Float64` ou `BFloat16`). A otimização de transposição de bits para armazenamento acontece no servidor, não no protocolo RowBinary.

Sintaxe:

```text
QBit(element_type, dimension[, stride])
```

Em que `element_type` é `Int8`, `Float32`, `Float64` ou `BFloat16`, e `dimension` é a dimensão fixa do vetor. O `stride` opcional controla apenas como os planos de bits são agrupados em fluxos de armazenamento no servidor; ele não afeta o formato wire `RowBinary`, que é sempre o array completo com `dimension` elementos.

Formato wire: idêntico a `Array(element_type)`:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

Exemplo de codificação de `QBit(Float32, 4)` contendo `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

<div id="format-settings">
  ## Configurações de formato
</div>

<RowBinaryFormatSettings />