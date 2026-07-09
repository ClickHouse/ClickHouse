---
description: 'Documentação da cláusula LIMIT BY'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'Cláusula LIMIT BY'
doc_type: 'reference'
---

Uma consulta com a cláusula `LIMIT n BY expressions` seleciona as primeiras `n` linhas para cada valor distinto de `expressions`. A chave de `LIMIT BY` pode conter qualquer número de [expressions](/pt-BR/sql-reference/syntax#expressions).

O ClickHouse oferece suporte às seguintes variantes de sintaxe:

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

Durante o processamento da consulta, o ClickHouse seleciona os dados ordenados pela chave de ordenação. A chave de ordenação é definida explicitamente por uma cláusula [ORDER BY](/pt-BR/sql-reference/statements/select/order-by) ou implicitamente como uma propriedade do mecanismo de tabela (a ordem das linhas só é garantida ao usar [ORDER BY](/pt-BR/sql-reference/statements/select/order-by); caso contrário, os blocos de linhas não ficarão ordenados devido ao multithreading). Em seguida, o ClickHouse aplica `LIMIT n BY expressions` e retorna as primeiras `n` linhas para cada combinação distinta de `expressions`. Se `OFFSET` for especificado, para cada bloco de dados que pertença a uma combinação distinta de `expressions`, o ClickHouse ignora `offset_value` linhas a partir do início do bloco e retorna, no máximo, `n` linhas como resultado. Se `offset_value` for maior que o número de linhas no bloco de dados, o ClickHouse retornará zero linhas desse bloco.

:::note
`LIMIT BY` não está relacionado a [LIMIT](../../../sql-reference/statements/select/limit.md). Ambos podem ser usados na mesma consulta.
:::

Se você quiser usar números de colunas em vez de nomes de colunas na cláusula `LIMIT BY`, habilite a configuração [enable&#95;positional&#95;arguments](/pt-BR/operations/settings/settings#enable_positional_arguments).

<div id="examples">
  ## Exemplos
</div>

Tabela de exemplo:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consultas:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

A consulta `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` retorna o mesmo resultado.

A consulta a seguir retorna as 5 principais origens de referência para cada par `domain, device_type`, com no máximo 100 linhas no total (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` também funciona com limites negativos e offsets. Assim como na [cláusula LIMIT negativa](/pt-BR/sql-reference/statements/select/limit#negative-limits), você pode usar valores negativos com `LIMIT BY` para selecionar linhas do *fim* de cada grupo.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

Retorna as 2 últimas linhas para cada `id`. Para `id = 1`, obtemos as linhas `11` e `12`; para `id = 2`, ambas as linhas são retornadas porque o grupo tem apenas 2 linhas.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

Retorna a penúltima linha de cada `id`: o `OFFSET -1` ao final remove a última linha de cada grupo, e o `-1` inicial então mantém a última linha do que restou.

Também é possível combinar `LIMIT` e `OFFSET` com sinais diferentes. Por exemplo, para remover a primeira linha de cada grupo e então manter as 2 últimas do que restou:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Para `id = 1`, a primeira linha (`10`) é ignorada; as 2 últimas, `11` e `12`, são retornadas. Para `id = 2`, a primeira linha (`20`) é ignorada, restando apenas `21`.

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

`LIMIT BY ALL` equivale a listar, no `SELECT`, todas as expressões que não são funções de agregação.

Por exemplo:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

é igual a

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

Em um caso especial, se houver uma função que tenha tanto funções de agregação quanto outros campos como argumentos, as chaves de `LIMIT BY` conterão o maior número possível de campos não agregados que pudermos extrair dela.

Por exemplo:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

é o mesmo que

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## Exemplos
</div>

Tabela de exemplo:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consultas:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

A consulta `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` retorna o mesmo resultado.

Usando `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

Isso equivale a:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```