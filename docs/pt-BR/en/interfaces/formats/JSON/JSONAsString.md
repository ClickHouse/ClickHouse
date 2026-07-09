---
alias: []
description: 'Documentação do formato JSONAsString'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: 'referência'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✗     |       |

<div id="description">
  ## Descrição
</div>

Neste formato, um único objeto JSON é interpretado como um único valor.
Se a entrada tiver vários objetos JSON (separados por vírgulas), eles serão interpretados como linhas distintas.
Se os dados de entrada estiverem entre `[]`, serão interpretados como um array de objetos JSON.

:::note
Este formato só pode ser analisado em uma tabela com um único campo do tipo [String](/pt-BR/sql-reference/data-types/string.md).
As colunas restantes devem ser definidas como [`DEFAULT`](/pt-BR/sql-reference/statements/create/table.md/#default) ou [`MATERIALIZED`](/pt-BR/sql-reference/statements/create/view#materialized-view),
ou devem ser omitidas.
:::

Depois de serializar todo o objeto JSON em uma String, você pode usar as [funções JSON](/pt-BR/sql-reference/functions/json-functions.md) para processá-lo.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="basic-example">
  ### Exemplo básico
</div>

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

<div id="an-array-of-json-objects">
  ### Um array de objetos JSON
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>
