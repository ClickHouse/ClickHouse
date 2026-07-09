---
description: 'Documentação da instrução CHECK GRANT'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'Instrução CHECK GRANT'
doc_type: 'reference'
---

A consulta `CHECK GRANT` é usada para verificar se o usuário ou role atual recebeu um privilégio específico.

<div id="syntax">
  ## Sintaxe
</div>

A sintaxe básica da consulta é a seguinte:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — Tipo de privilégio.

<div id="examples">
  ## Exemplos
</div>

Se o privilégio tiver sido concedido ao usuário, a resposta `check_grant` será `1`. Caso contrário, a resposta `check_grant` será `0`.

Se `table_1.col1` existir e o usuário atual tiver recebido o privilégio `SELECT`/`SELECT(con)` ou uma role (com privilégio), a resposta será `1`.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

Se `table_2.col2` não existir ou se o usuário atual não tiver o privilégio `SELECT`/`SELECT(con)` concedido, nem uma role (com esse privilégio), a resposta será `0`.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## Curinga
</div>

Ao especificar privilégios, você pode usar um asterisco (`*`) em vez de uma tabela ou do nome do banco de dados. Consulte [GRANTS COM CURINGA](../../sql-reference/statements/grant.md#wildcard-grants) para conhecer as regras de curinga.