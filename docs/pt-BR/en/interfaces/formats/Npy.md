---
alias: []
description: 'Documentação do formato Npy'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `Npy` foi projetado para carregar um array do NumPy de um arquivo `.npy` para o ClickHouse.
O formato de arquivo do NumPy é um formato binário usado para armazenar arrays de dados numéricos com eficiência.
Durante a importação, o ClickHouse trata a dimensão mais externa como um array de linhas com uma única coluna.

A tabela abaixo mostra os tipos de dados Npy compatíveis e seus tipos correspondentes no ClickHouse:

<div id="data_types-matching">
  ## Correspondência entre tipos de dados
</div>

| Tipo de dado Npy (`INSERT`) | Tipo de dado ClickHouse                                 | Tipo de dado Npy (`SELECT`) |
| --------------------------- | ------------------------------------------------------- | --------------------------- |
| `i1`                        | [Int8](/pt-BR/sql-reference/data-types/int-uint.md)           | `i1`                        |
| `i2`                        | [Int16](/pt-BR/sql-reference/data-types/int-uint.md)          | `i2`                        |
| `i4`                        | [Int32](/pt-BR/sql-reference/data-types/int-uint.md)          | `i4`                        |
| `i8`                        | [Int64](/pt-BR/sql-reference/data-types/int-uint.md)          | `i8`                        |
| `u1`, `b1`                  | [UInt8](/pt-BR/sql-reference/data-types/int-uint.md)          | `u1`                        |
| `u2`                        | [UInt16](/pt-BR/sql-reference/data-types/int-uint.md)         | `u2`                        |
| `u4`                        | [UInt32](/pt-BR/sql-reference/data-types/int-uint.md)         | `u4`                        |
| `u8`                        | [UInt64](/pt-BR/sql-reference/data-types/int-uint.md)         | `u8`                        |
| `f2`, `f4`                  | [Float32](/pt-BR/sql-reference/data-types/float.md)           | `f4`                        |
| `f8`                        | [Float64](/pt-BR/sql-reference/data-types/float.md)           | `f8`                        |
| `S`, `U`                    | [String](/pt-BR/sql-reference/data-types/string.md)           | `S`                         |
|                             | [FixedString](/pt-BR/sql-reference/data-types/fixedstring.md) | `S`                         |

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### Salvando um array no formato .npy usando Python
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### Lendo um arquivo NumPy no ClickHouse
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### Selecionando dados
</div>

Você pode selecionar dados de uma tabela do ClickHouse e salvá-los em um arquivo no formato Npy com o seguinte comando usando o clickhouse-client:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## Configurações de formato
</div>
