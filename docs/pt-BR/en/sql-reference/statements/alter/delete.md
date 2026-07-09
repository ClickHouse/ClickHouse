---
description: 'Documentação da instrução DELETE do ALTER TABLE'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'Instrução DELETE do ALTER TABLE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Exclui dados que correspondem à expressão de filtro especificada. Implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

:::note
O prefixo `ALTER TABLE` faz com que essa sintaxe seja diferente da maioria dos outros sistemas compatíveis com SQL. Ele serve para indicar que, ao contrário de consultas semelhantes em bancos de dados OLTP, esta é uma operação pesada e não foi projetada para uso frequente. `ALTER TABLE` é considerado uma operação pesada que exige que os dados subjacentes sejam mesclados antes de serem excluídos. Para tabelas MergeTree, considere usar a [consulta `DELETE FROM`](/pt-BR/sql-reference/statements/delete.md), que realiza uma exclusão leve e pode ser consideravelmente mais rápida.
:::

`filter_expr` deve ser do tipo `UInt8`. A consulta exclui linhas da tabela para as quais essa expressão assume um valor diferente de zero.

Uma consulta pode conter vários comandos separados por vírgulas.

A sincronia do processamento da consulta é definida pela configuração [mutations&#95;sync](/pt-BR/operations/settings/settings.md/#mutations_sync). Por padrão, ela é assíncrona.

**Veja também**

* [Mutações](/pt-BR/sql-reference/statements/alter/index.md#mutations)
* [Sincronia de consultas ALTER](/pt-BR/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* configuração [mutations&#95;sync](/pt-BR/operations/settings/settings.md/#mutations_sync)

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Como tratar atualizações e exclusões no ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)