---
description: 'Tabela do sistema que lista índices hipotéticos (what-if) definidos na sessão atual'
keywords: ['tabela do sistema', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

Lista todos os índices de salto hipotéticos (what-if) definidos na sessão atual. Veja [`CREATE HYPOTHETICAL INDEX`](/pt-BR/sql-reference/statements/hypothetical-index#create-hypothetical-index) e [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif).

O conteúdo está no escopo da sessão: cada conexão vê apenas seus próprios índices hipotéticos, e a tabela fica vazia quando nenhum índice foi criado na sessão atual.

Os `(database, table)` atuais são resolvidos por UUID no momento da consulta, portanto refletem `RENAME TABLE`, e as entradas de tabelas removidas são ocultadas automaticamente.

<div id="columns">
  ## Colunas
</div>

| Coluna        | Tipo     | Descrição                                                                             |
| ------------- | -------- | ------------------------------------------------------------------------------------- |
| `database`    | `String` | Banco de dados de destino.                                                            |
| `table`       | `String` | Tabela de destino.                                                                    |
| `name`        | `String` | Nome do índice.                                                                       |
| `type`        | `String` | Tipo de índice (`minmax`, `set`, `bloom_filter` etc.).                                |
| `type_full`   | `String` | Expressão do tipo de índice, incluindo argumentos, por exemplo, `bloom_filter(0.01)`. |
| `expression`  | `String` | Expressão do índice, como escrita em `CREATE HYPOTHETICAL INDEX`.                     |
| `granularity` | `UInt64` | Número de grânulos de dados por grânulo de índice.                                    |

<div id="example">
  ## Exemplo
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` é o nome do tipo básico, e `type_full` inclui os argumentos, para que os usuários possam diferenciar variantes parametrizadas como `bloom_filter(0.01)` e `bloom_filter(0.001)`.

<div id="see-also">
  ## Veja também
</div>

* [`CREATE HYPOTHETICAL INDEX`](/pt-BR/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif)