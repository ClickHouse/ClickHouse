---
description: 'Altera a string de consulta fornecida com variações aleatórias.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

Altera a string de consulta fornecida com variações aleatórias.

<div id="syntax">
  ## Sintaxe
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento          | Descrição                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------ |
| `query`            | (String) - A consulta de origem sobre a qual o fuzzing será executado.                     |
| `max_query_length` | (UInt64) - O comprimento máximo que a consulta pode atingir durante o processo de fuzzing. |
| `random_seed`      | (UInt64) - Uma semente aleatória para produzir resultados consistentes.                    |

<div id="returned_value">
  ## Valor retornado
</div>

Um objeto de tabela com uma única coluna contendo strings de consulta modificadas.

<div id="usage-example">
  ## Exemplo de uso
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```