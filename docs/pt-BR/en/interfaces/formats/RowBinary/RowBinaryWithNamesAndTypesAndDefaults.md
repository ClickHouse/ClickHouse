---
alias: []
description: 'Documentação do formato RowBinaryWithNamesAndTypesAndDefaults'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✗     |       |

<div id="description">
  ## Descrição
</div>

Semelhante ao formato [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md), mas com um byte extra antes de cada célula indicando se o valor `DEFAULT` da coluna deve ser usado — exatamente como no formato [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md). Essa combinação oferece suporte a `INSERT`s com evolução de esquema: o emissor pode omitir colunas do cabeçalho (elas recebem o `DEFAULT` da coluna de destino) e, para qualquer coluna que enviar, pode marcar células individuais como &quot;usar o `DEFAULT` da coluna&quot; sem confundir isso com `NULL`.

Este formato é apenas de entrada.

<div id="wire-format">
  ## Formato wire
</div>

O cabeçalho é idêntico ao de [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md):

1. Um `VarUInt` com o número de colunas `N`.
2. `N` `String`s com prefixo de comprimento contendo os nomes das colunas.
3. `N` tipos de coluna — nomes textuais ou codificação binária compacta, controlados pelas configurações `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format`.

Após o cabeçalho, cada linha é composta por `N` células. Para cada célula:

* Um único byte marcador `UInt8`.
  * `0x01` — usa a expressão `DEFAULT` da coluna de destino. Nenhum byte de valor vem em seguida.
  * `0x00` — um valor vem em seguida, serializado pelo serializador `RowBinary` do tipo da coluna. Para `Nullable(T)`, os bytes do valor começam com o byte nulo de `Nullable` (`0` para não nulo, `1` para NULL) e, em seguida, o valor interno, se não for nulo.

<div id="defaults-vs-null">
  ## Valores padrão vs NULL
</div>

O marcador de valor padrão por célula e o byte nulo embutido de `Nullable` são independentes. Uma coluna `Nullable(UInt32) DEFAULT 42` pode ser enviada de três formas diferentes por linha:

| Bytes     | Significado                                            |
| --------- | ------------------------------------------------------ |
| `01`      | Usar `DEFAULT 42`.                                     |
| `00 01`   | Caminho do valor, depois `NULL` via o tipo `Nullable`. |
| `00 00 …` | Caminho do valor, depois um valor interno não nulo.    |

<div id="schema-evolution">
  ## Evolução de esquema
</div>

| Caso                                                         | Comportamento                                                                                                                                                                            |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Coluna totalmente ausente do cabeçalho do arquivo            | Preenchida na tabela de destino por meio de `insertDefaultsForNotSeenColumns`; condicionada a `defaults_for_omitted_fields`.                                                             |
| Coluna presente no cabeçalho, marcador de célula `0x01`      | `insertDefault` por linha.                                                                                                                                                               |
| Coluna presente no cabeçalho, marcador de célula `0x00`      | O valor é interpretado normalmente.                                                                                                                                                      |
| Coluna extra no cabeçalho, não presente na tabela de destino | Descartada silenciosamente quando `input_format_skip_unknown_fields = 1` (o marcador é consumido primeiro; se `0x01`, nada mais; se `0x00`, o valor tipado é interpretado e descartado). |

<div id="example-usage">
  ## Exemplo de uso
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* O cabeçalho traz uma coluna chamada `x` do tipo `Nullable(UInt32)`.
* A única célula usa o marcador `0x01`, que significa &quot;usar `DEFAULT 42`&quot;.

<div id="format-settings">
  ## Configurações do formato
</div>

<RowBinaryFormatSettings />