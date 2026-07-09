---
description: 'Documentação sobre operações com TTL da tabela'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'Operações com TTL da tabela'
doc_type: 'reference'
---

:::note
Se você está procurando detalhes sobre como usar TTL para gerenciar dados antigos, consulte o guia do usuário [Gerenciar dados com TTL](/pt-BR/guides/developer/ttl.md). A documentação abaixo mostra como alterar ou remover uma regra de TTL existente.
:::

<div id="modify-ttl">
  ## MODIFICAR TTL
</div>

Você pode alterar o [TTL da tabela](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) usando uma consulta no seguinte formato:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## REMOVER TTL
</div>

A propriedade TTL pode ser removida da tabela com a consulta a seguir:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Exemplo**

Considere a tabela com `TTL` no nível da tabela:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Execute `OPTIMIZE` para forçar a limpeza do `TTL`:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

A segunda linha foi excluída da tabela.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Agora, remova o `TTL` da tabela com a consulta a seguir:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Reinsira a linha excluída e force novamente a limpeza do `TTL` com `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

O `TTL` não existe mais, então a segunda linha não é excluída:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**Veja também**

* Mais informações sobre a [expressão TTL](../../../sql-reference/statements/create/table.md#ttl-expression).
* Modifique a coluna [com TTL](/pt-BR/sql-reference/statements/alter/ttl).