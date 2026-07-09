---
description: 'Documentação dos tipos de dados de ponto flutuante no ClickHouse: Float32,
  Float64 e BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Tipos Float32 | Float64 | BFloat16'
doc_type: 'reference'
---

:::note
Se você precisa de cálculos exatos, especialmente ao trabalhar com dados financeiros ou comerciais que exigem alta precisão, considere usar [Decimal](../data-types/decimal.md).

[Números de ponto flutuante](https://en.wikipedia.org/wiki/IEEE_754) podem levar a resultados imprecisos, como mostrado abaixo:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

Os tipos equivalentes no ClickHouse e em C são apresentados abaixo:

* `Float32` — `float`.
* `Float64` — `double`.

Os tipos Float no ClickHouse têm os seguintes aliases:

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

Ao criar tabelas, parâmetros numéricos para números de ponto flutuante podem ser definidos (por exemplo, `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), mas o ClickHouse os ignora.

<div id="using-floating-point-numbers">
  ## Usando números de ponto flutuante
</div>

* Cálculos com números de ponto flutuante podem gerar erros de arredondamento.

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* O resultado do cálculo depende do método de cálculo (o tipo de processador e a arquitetura do sistema computacional).
* Cálculos de ponto flutuante podem resultar em números como infinito (`Inf`) e &quot;não é um número&quot; (`NaN`). Isso deve ser levado em conta ao processar os resultados dos cálculos.
* Ao fazer o parsing de números de ponto flutuante a partir de texto, o resultado pode não ser o valor representável pela máquina mais próximo.

<div id="nan-and-inf">
  ## NaN e Inf
</div>

Ao contrário do SQL padrão, o ClickHouse oferece suporte às seguintes categorias de números de ponto flutuante:

* `Inf` – infinito.

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — Infinito negativo.

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — Não é um número.

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

Consulte as regras de ordenação de `NaN` na seção [cláusula ORDER BY](../../sql-reference/statements/select/order-by.md).

<div id="nan-values-in-set-semantics">
  ## Valores `NaN` na semântica de conjuntos
</div>

O padrão IEEE 754 define `NaN` de modo que a comparação escalar `NaN = NaN` retorne `false`.
O ClickHouse segue essa regra para o operador `=`.

No entanto, `NaN` não é um único valor; pode ser qualquer padrão de bits cujo expoente seja todo composto por uns e cuja
mantissa seja diferente de zero. Operações diferentes e arquiteturas de CPU diferentes podem produzir valores `NaN`
com bits de sinal diferentes ou payloads diferentes na mantissa. Por exemplo:

* `0./0.` produz um `NaN` cujo bit de sinal é 1 na maioria das plataformas x86.
* O literal `nan` produz um `NaN` cujo bit de sinal é 0.
* Após a [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230), o caminho NEON em AArch64 de
  `log` retorna um `NaN` cujo bit de sinal difere do `log` escalar da glibc para entradas negativas.

As tabelas hash no ClickHouse comparam chaves byte a byte, portanto diferentes padrões de bits de `NaN` têm hash para
buckets diferentes e são tratados como valores distintos por operações com semântica de conjunto, incluindo
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct` e equi-`JOIN` em uma chave `Float`:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

Isso está de acordo com a IEEE 754 (todo `NaN` é diferente de qualquer outro valor, inclusive de si mesmo)
mas pode ser inesperado. Se você precisar que operações com semântica de conjunto tratem todos os valores `NaN` como iguais,
converta-os para a forma canônica na consulta:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

A mesma abordagem funciona para as chaves de `DISTINCT`, `GROUP BY` e `JOIN`.

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` é um tipo de dado de ponto flutuante de 16 bits com expoente de 8 bits, sinal e mantissa de 7 bits.
Ele é útil para aplicações de aprendizado de máquina e IA.

O ClickHouse oferece suporte a conversões entre `Float32` e `BFloat16`, que
podem ser feitas usando as funções [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) ou [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16).

:::note
A maioria das outras operações não tem suporte.
:::