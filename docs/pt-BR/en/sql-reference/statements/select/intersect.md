---
description: 'Documentação da cláusula INTERSECT'
sidebar_label: 'INTERSECT'
slug: /sql-reference/statements/select/intersect
title: 'Cláusula INTERSECT'
doc_type: 'reference'
---

A cláusula `INTERSECT` retorna apenas as linhas que estão presentes tanto no resultado da primeira consulta quanto no da segunda. As consultas devem corresponder em número de colunas, ordem e tipo. O resultado de `INTERSECT` pode conter linhas duplicadas.

Múltiplas instruções `INTERSECT` são executadas da esquerda para a direita se os parênteses não forem especificados. O operador `INTERSECT` tem prioridade mais alta do que as cláusulas `UNION` e `EXCEPT`.

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```

A condição pode ser qualquer expressão, dependendo dos seus requisitos.

<div id="examples">
  ## Exemplos
</div>

Aqui está um exemplo simples que faz a interseção dos números de 1 a 10 com os números de 3 a 8:

```sql title="Query"
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,8);
```

```response title="Response"
┌─number─┐
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
└────────┘
```

`INTERSECT` é útil quando você tem duas tabelas que compartilham uma ou mais colunas. Você pode fazer a interseção dos resultados de duas consultas, desde que eles contenham as mesmas colunas. Por exemplo, suponha que tenhamos alguns milhões de linhas de dados históricos de criptomoedas com preços de negociação e volume:

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

Agora, suponha que temos uma tabela chamada `holdings` que contém uma lista das criptomoedas que possuímos, junto com a quantidade de moedas:

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10);
   ('Bitcoin Diamond', 5000);
```

Podemos usar `INTERSECT` para responder a perguntas como **&quot;Quais moedas que temos foram negociadas a um preço superior a $100?&quot;**:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT
SELECT crypto_name FROM crypto_prices
WHERE price > 100
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
│ Ethereum    │
│ Ethereum    │
└─────────────┘
```

Isso significa que, em algum momento, Bitcoin e Ethereum foram negociados acima de $100, enquanto DOGEFI e Bitcoin Diamond nunca foram negociados acima de $100 (pelo menos com base nos dados que temos aqui neste exemplo).

<div id="intersect-distinct">
  ## INTERSECT DISTINCT
</div>

Observe que, na consulta anterior, tínhamos várias posições em Bitcoin e Ethereum negociadas acima de $100. Pode ser útil remover as linhas duplicadas (já que elas apenas repetem o que já sabemos). Você pode adicionar `DISTINCT` a `INTERSECT` para eliminar linhas duplicadas do resultado:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price > 100;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Ethereum    │
└─────────────┘
```

**Veja também**

* [UNION](/pt-BR/sql-reference/statements/select/union)
* [EXCEPT](/pt-BR/sql-reference/statements/select/except)