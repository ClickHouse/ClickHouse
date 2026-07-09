---
alias: []
description: 'Documentação do formato JSONColumnsWithMetadata'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere do formato [`JSONColumns`](./JSONColumns.md) por também incluir alguns metadados e estatísticas (semelhante ao formato [`JSON`](./JSON.md)).

:::note
O formato `JSONColumnsWithMetadata` mantém todos os dados em memória e depois os gera como um único bloco, portanto pode levar a um alto consumo de memória.
:::

<div id="example-usage">
  ## Exemplo de uso
</div>

Exemplo:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

Para o formato de entrada `JSONColumnsWithMetadata`, se a configuração [`input_format_json_validate_types_from_metadata`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) estiver definida como `1`,
os tipos dos metadados nos dados de entrada serão comparados aos tipos das colunas correspondentes da tabela.

<div id="format-settings">
  ## Configurações de formato
</div>
