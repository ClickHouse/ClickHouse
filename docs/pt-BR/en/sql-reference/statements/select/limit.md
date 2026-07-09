---
description: 'Documentação da cláusula LIMIT'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'Cláusula LIMIT'
doc_type: 'reference'
---

A cláusula `LIMIT` controla quantas linhas são retornadas pela sua consulta.

<div id="basic-syntax">
  ## Sintaxe básica
</div>

**Selecione as primeiras linhas:**

```sql
LIMIT m
```

Retorna as primeiras `m` linhas do resultado, ou todos os registros se houver menos de `m`.

**Sintaxe alternativa de TOP (compatível com o MS SQL Server):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

Isso é equivalente a `LIMIT m` e pode ser usado para manter a compatibilidade com consultas do Microsoft SQL Server.

**SELECT com OFFSET:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

Ignora as primeiras `n` linhas e, em seguida, retorna as `m` linhas seguintes.

Em ambas as formas, `n` e `m` devem ser inteiros não negativos.

<div id="negative-limits">
  ## Limites negativos
</div>

Selecione linhas a partir do *fim* do conjunto de resultados usando valores negativos:

| Sintaxe              | Resultado                                             |
| -------------------- | ----------------------------------------------------- |
| `LIMIT -m`           | Últimas `m` linhas                                    |
| `LIMIT -m OFFSET -n` | Últimas `m` linhas após pular as últimas `n` linhas   |
| `LIMIT m OFFSET -n`  | Primeiras `m` linhas após pular as últimas `n` linhas |
| `LIMIT -m OFFSET n`  | Últimas `m` linhas após pular as primeiras `n` linhas |

A sintaxe `LIMIT -n, -m` é equivalente a `LIMIT -m OFFSET -n`.

<div id="fractional-limits">
  ## Limites fracionários
</div>

Use valores decimais entre 0 e 1 para selecionar uma porcentagem de linhas:

| Sintaxe                 | Resultado                                                     |
| ----------------------- | ------------------------------------------------------------- |
| `LIMIT 0.1`             | Primeiras 10% das linhas                                      |
| `LIMIT 1 OFFSET 0.5`    | A linha mediana                                               |
| `LIMIT 0.25 OFFSET 0.5` | Terceiro quartil (25% das linhas após pular os primeiros 50%) |

:::note

* As frações devem ser valores [Float64](../../data-types/float.md) maiores que 0 e menores que 1.
* Quantidades fracionárias de linhas são arredondadas para o próximo número inteiro.
  :::

<div id="combining-limit-types">
  ## Combinando tipos de LIMIT
</div>

Você pode combinar inteiros padrão com offsets fracionários ou negativos:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

O modificador `WITH TIES` inclui linhas adicionais que têm os mesmos valores de `ORDER BY` da última linha dentro do limite.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

Com `WITH TIES`, todas as linhas que correspondem ao último valor são incluídas:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

A linha 6 está incluída porque tem o mesmo valor (`2`) da linha 5.

O mesmo vale quando o offset é especificado com a palavra-chave `OFFSET`:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Ignorar as 2 primeiras linhas e retornar 3 normalmente produziria `1, 1, 2`, mas o segundo `2` é incluído porque empata com a última linha.

`WITH TIES` também funciona com limites e offsets negativos. Ele inclui linhas adicionais que têm os mesmos valores de `ORDER BY` que a primeira linha selecionada:

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Sem `WITH TIES`, o resultado seria `1, 1, 2, 2`. Com `WITH TIES`, três linhas adicionais com o valor `1` são incluídas porque empatam com a primeira linha selecionada.

Esse modificador pode ser combinado com o modificador [`ORDER BY ... WITH FILL`](/pt-BR/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier).

<div id="considerations">
  ## Considerações
</div>

**Resultados não determinísticos:** Sem uma cláusula [`ORDER BY`](../../../sql-reference/statements/select/order-by.md), as linhas retornadas podem ser arbitrárias e variar entre execuções da consulta.

**Limite do lado do servidor:** O número de linhas retornadas também pode ser afetado pela configuração [limit](../../../operations/settings/settings.md#limit).

<div id="see-also">
  ## Veja também
</div>

* [LIMIT BY](/pt-BR/sql-reference/statements/select/limit-by) — Limita as linhas por grupo de valores, útil para obter os N principais resultados em cada categoria.