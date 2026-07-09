---
alias: []
description: 'Documentação do formato One'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✗     |       |

<div id="description">
  ## Descrição
</div>

O formato `One` é um formato de entrada especial que não lê nenhum dado do arquivo e retorna apenas uma linha com uma coluna do tipo [`UInt8`](../../sql-reference/data-types/int-uint.md), chamada `dummy` e com valor `0` (como a tabela `system.one`).
Pode ser usado com colunas virtuais `_file/_path` para listar todos os arquivos sem ler os dados de fato.

<div id="example-usage">
  ## Exemplo de uso
</div>

Exemplo:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>
