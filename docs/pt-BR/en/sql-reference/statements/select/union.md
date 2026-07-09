---
description: 'Documentação sobre a cláusula UNION'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'Cláusula UNION'
doc_type: 'referência'
---

Você pode usar `UNION` especificando explicitamente `UNION ALL` ou `UNION DISTINCT`.

Se você não especificar `ALL` ou `DISTINCT`, isso dependerá da configuração `union_default_mode`. A diferença entre `UNION ALL` e `UNION DISTINCT` é que `UNION DISTINCT` elimina duplicatas no resultado da união; isso é equivalente a um `SELECT DISTINCT` sobre uma subconsulta que contém `UNION ALL`.

Você pode usar `UNION` para combinar qualquer número de consultas `SELECT`, concatenando seus resultados. Exemplo:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

As colunas de resultado são correspondidas pelo índice delas (ordem dentro de `SELECT`). Se os nomes das colunas não coincidirem, os nomes do resultado final serão obtidos da primeira consulta.

A conversão de tipos é realizada para uniões. Por exemplo, se duas consultas combinadas tiverem o mesmo campo com tipos `Nullable` e não `Nullable` de um tipo compatível, o `UNION` resultante terá um campo do tipo `Nullable`.

As consultas que fazem parte de `UNION` podem ser colocadas entre `()`. [ORDER BY](../../../sql-reference/statements/select/order-by.md) e [LIMIT](../../../sql-reference/statements/select/limit.md) são aplicados a consultas separadas, não ao resultado final. Se você precisar aplicar uma conversão ao resultado final, poderá colocar todas as consultas com `UNION` em uma subconsulta na cláusula [FROM](../../../sql-reference/statements/select/from.md).

Se você usar `UNION` sem especificar explicitamente `UNION ALL` ou `UNION DISTINCT`, poderá definir o union mode usando a configuração [union&#95;default&#95;mode](/pt-BR/operations/settings/settings#union_default_mode). Os valores da configuração podem ser `ALL`, `DISTINCT` ou uma string vazia. No entanto, se você usar `UNION` com a configuração `union_default_mode` definida como uma string vazia, isso gerará uma exceção. Os exemplos a seguir demonstram os resultados de consultas com diferentes valores dessa configuração.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Consultas que fazem parte de `UNION/UNION ALL/UNION DISTINCT` podem ser executadas simultaneamente, e seus resultados podem ser combinados.

**Veja também**

* Configuração [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default).
* Configuração [union&#95;default&#95;mode](/pt-BR/operations/settings/settings#union_default_mode).