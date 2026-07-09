---
alias: []
description: 'Documentação sobre o formato LineAsStringWithNamesAndTypes'
input_format: false
keywords: ['LineAsStringWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/LineAsStringWithNamesAndTypes
title: 'LineAsStringWithNamesAndTypes'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `LineAsStringWithNames` é semelhante ao formato [`LineAsString`](./LineAsString.md),
mas gera duas linhas de cabeçalho: uma com os nomes das colunas e outra com os tipos.

<div id="example-usage">
  ## Exemplo de uso
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## Configurações do formato
</div>
