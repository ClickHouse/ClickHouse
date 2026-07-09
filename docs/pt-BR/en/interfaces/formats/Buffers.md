---
alias: []
description: 'Documentação do formato Buffers'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

`Buffers` é um formato binário muito simples para troca de dados **temporários**, em que tanto o consumidor quanto o produtor já conhecem o esquema e a ordem das colunas.

Diferentemente de [Native](./Native.md), ele **não** armazena nomes de colunas, tipos de coluna nem metadados extras.

Nesse formato, os dados são gravados e lidos em [blocos](/pt-BR/development/architecture#block) em formato binário. Buffers usa a mesma representação binária por coluna do formato [Native](./Native.md) e respeita as mesmas configurações do formato Native.

Para cada bloco, a seguinte sequência é gravada:

1. Número de colunas (UInt64, little-endian).
2. Número de linhas (UInt64, little-endian).
3. Para cada coluna:

* Tamanho total, em bytes, dos dados de coluna serializados (UInt64, little-endian).
* Bytes dos dados de coluna serializados, exatamente como no formato [Native](./Native.md).

<div id="example-usage">
  ## Exemplo de uso
</div>

Grave em um arquivo:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

Leia novamente com tipos de coluna explícitos:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

Se você tiver uma tabela com os mesmos tipos de coluna, poderá populá-la diretamente:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

Observe a tabela:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>
