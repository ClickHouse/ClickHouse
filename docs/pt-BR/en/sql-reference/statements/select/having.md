---
description: 'Documentação sobre a cláusula HAVING'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'Cláusula HAVING'
doc_type: 'reference'
---

Permite filtrar os resultados da agregação produzidos por [GROUP BY](/pt-BR/sql-reference/statements/select/group-by). É semelhante à cláusula [WHERE](../../../sql-reference/statements/select/where.md), mas a diferença é que `WHERE` é aplicada antes da agregação, enquanto `HAVING` é aplicada depois.

É possível referenciar, na cláusula `HAVING`, os resultados da agregação da cláusula `SELECT` usando seus aliases. Como alternativa, a cláusula `HAVING` também pode filtrar os resultados de agregações adicionais que não são retornados pela consulta.

<div id="example">
  ## Exemplo
</div>

Se você tiver uma tabela `sales` como a seguir:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

Você pode consultá-lo assim:

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

Isso listará os vendedores com mais de 10.000 em vendas totais na respectiva região.

<div id="limitations">
  ## Limitações
</div>

`HAVING` não pode ser usado se não houver agregação. Use `WHERE` em vez disso.