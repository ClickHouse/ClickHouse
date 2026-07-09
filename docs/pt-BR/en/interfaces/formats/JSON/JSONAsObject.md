---
alias: []
description: 'Documentação sobre o formato JSONAsObject'
input_format: true
keywords: ['JSONAsObject']
output_format: false
slug: /interfaces/formats/JSONAsObject
title: 'JSONAsObject'
doc_type: 'reference'
---

<div id="description">
  ## Descrição
</div>

Neste formato, um único objeto JSON é interpretado como um único valor [JSON](/pt-BR/sql-reference/data-types/newjson.md). Se a entrada tiver vários objetos JSON (separados por vírgula), eles serão interpretados como linhas separadas. Se os dados de entrada estiverem entre `[]`, serão interpretados como um array de JSONs.

Este formato só pode ser processado em uma tabela com um único campo do tipo [JSON](/pt-BR/sql-reference/data-types/newjson.md). As colunas restantes devem estar definidas como [`DEFAULT`](/pt-BR/sql-reference/statements/create/table.md/#default) ou [`MATERIALIZED`](/pt-BR/sql-reference/statements/create/view#materialized-view).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="basic-example">
  ### Exemplo básico
</div>

```sql title="Query"
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

```response title="Response"
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

<div id="an-array-of-json-objects">
  ### Um array de objetos JSON
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

```response title="Response"
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

<div id="columns-with-default-values">
  ### Colunas com valores padrão
</div>

```sql title="Query"
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response title="Response"
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

<div id="format-settings">
  ## Configurações de formato
</div>
