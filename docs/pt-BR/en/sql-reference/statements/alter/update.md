---
description: 'Documentação das Instruções ALTER TABLE ... UPDATE'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'Instruções ALTER TABLE ... UPDATE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

Manipula dados que correspondem à expressão de filtro especificada. Implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

:::note
O prefixo `ALTER TABLE` faz com que essa sintaxe seja diferente da maioria dos outros sistemas com suporte a SQL. Ele serve para indicar que, ao contrário de consultas semelhantes em bancos de dados OLTP, esta é uma operação pesada, não projetada para uso frequente.
:::

`filter_expr` deve ser do tipo `UInt8`. Esta consulta atualiza os valores das colunas especificadas para os valores das expressões correspondentes nas linhas em que `filter_expr` assume um valor diferente de zero. Os valores são convertidos para o tipo da coluna usando o operador `CAST`. Não há suporte para atualizar colunas usadas no cálculo da chave primária ou da chave de partição.

Uma consulta pode conter vários comandos separados por vírgulas.

A sincronicidade do processamento da consulta é definida pela configuração [mutations&#95;sync](/pt-BR/operations/settings/settings.md/#mutations_sync). Por padrão, ela é assíncrona.

**Veja também**

* [Mutações](/pt-BR/sql-reference/statements/alter/index.md#mutations)
* [Sincronicidade de consultas ALTER](/pt-BR/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* Configuração [mutations&#95;sync](/pt-BR/operations/settings/settings.md/#mutations_sync)
* [`UPDATE` leve](/pt-BR/sql-reference/statements/update) - Alternativa de atualização leve usando partes de patch
* [`APPLY PATCHES`](/pt-BR/sql-reference/statements/alter/apply-patches) - Aplicar patches manualmente de atualizações leves

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Como gerenciar atualizações e exclusões no ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)