---
description: 'Visão geral das estruturas de dados aninhadas no ClickHouse'
sidebar_label: 'Nested(Name1 Type1, Name2 Type2, ...)'
sidebar_position: 57
slug: /sql-reference/data-types/nested-data-structures/nested
title: 'Nested(name1 Type1, Name2 Type2, ...)'
doc_type: 'guide'
---

Uma estrutura de dados aninhada é como uma tabela dentro de uma célula. Os parâmetros de uma estrutura de dados aninhada — os nomes e tipos das colunas — são especificados da mesma forma que em uma consulta [CREATE TABLE](../../../sql-reference/statements/create/table.md). Cada linha da tabela pode corresponder a qualquer quantidade de linhas em uma estrutura de dados aninhada.

:::tip[Evite usar pontos em nomes de colunas]
Nomes de colunas que contêm pontos, colunas que compartilham um prefixo comum seguido de ponto e colunas do tipo `Array` podem ser interpretados como parte de uma estrutura `Nested` achatada quando `flatten_nested = 1` (o padrão). Isso pode causar validação inesperada do comprimento dos arrays durante inserções e restrições de renomeação.

Evite usar pontos em nomes de colunas sempre que possível.
Use underscores (`_`) ou outro separador no lugar de pontos nos nomes das colunas, a menos que você realmente precise da semântica de `Nested`.
:::

Exemplo:

```sql
CREATE TABLE test.visits(
  CounterID UInt32,
  StartDate Date,
  Sign Int8,
  IsNew UInt8,
  VisitID UInt64,
  UserID UInt64,
--highlight-start
  Goals Nested(
    ID UInt32,
    Serial UInt32,
    EventTime DateTime,
    Price Int64,
    OrderID String,
    CurrencyID UInt32
  )
--highlight-end
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY (StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID));

INSERT INTO test.visits
(CounterID, StartDate, Sign, IsNew, VisitID, UserID, Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-17', 1, 1, 1001, 100001, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 16:38:10', '2014-03-17 16:38:48', '2014-03-17 16:42:27'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1002, 100002, [1073752], [1], ['2014-03-17 00:28:25'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 0, 1003, 100003, [1073752], [1], ['2014-03-17 10:46:20'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 1, 1004, 100004, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:59:20', '2014-03-17 22:17:55', '2014-03-17 22:18:07', '2014-03-17 22:18:51'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1005, 100005, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1006, 100006, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 11:37:06', '2014-03-17 14:07:47', '2014-03-17 14:36:21'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1007, 100007, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1008, 100008, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 1, 1009, 100009, [591325, 1073752], [1, 2], ['2014-03-17 00:46:05', '2014-03-17 00:46:05'], [0, 0], ['', ''], [0, 0]),
    (101500, '2014-03-17', 1, 1, 1010, 100010, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:28:33', '2014-03-17 13:30:26', '2014-03-17 18:51:21', '2014-03-17 18:51:45'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]);
```

A instrução DDL `CREATE TABLE` acima declara a estrutura de dados aninhada `Goals`, que contém dados sobre conversões, ou metas atingidas.
Cada linha da tabela &#39;visits&#39; corresponde a zero ou mais conversões.

Quando a configuração [`flatten_nested`](/pt-BR/operations/settings/settings#flatten_nested) é definida como `0` (`flatten_nested=1` por padrão), há suporte a níveis arbitrários de aninhamento.

Na maioria dos casos, ao trabalhar com uma estrutura de dados aninhada, suas colunas são especificadas com nomes de colunas separados por ponto.
Essas colunas formam um array de tipos correspondentes.
Todos os arrays de colunas de uma mesma estrutura de dados aninhada têm o mesmo comprimento.

Por exemplo:

```sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID
LIMIT 10
```

```text
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goals.ID                       ┃ Goals.EventTime                                                                           ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
 1. │ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 2. │ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 3. │ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 4. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 5. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 6. │ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 7. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 8. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 9. │ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
10. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
    └────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
É mais fácil pensar em uma estrutura de dados aninhada como um conjunto de vários arrays de colunas com o mesmo comprimento.
:::

<div id="filtering-nested-columns-in-where">
  ### Filtrando colunas Nested no WHERE
</div>

Como cada coluna de uma estrutura `Nested` é armazenada como um `Array`, referenciá-la em uma cláusula `WHERE` retorna o array completo de cada linha, e não um elemento individual. Não é possível comparar diretamente uma coluna aninhada com um valor escalar, então você precisa usar [funções de array](/pt-BR/sql-reference/functions/array-functions).

Por exemplo, esta consulta **não** simplesmente retorna zero linhas — ela gera uma exceção, porque `Goals.ID` tem o tipo `Array(UInt32)` e `equals(Array(UInt32), UInt32)` não é uma comparação válida:

```sql
-- WRONG: compares the entire Array to a scalar
SELECT * FROM test.visits
WHERE Goals.ID = 591325;
```

```text
Code: 43. DB::Exception: Illegal types of arguments (`Array(UInt32)`, `UInt32`)
of function `equals`. (ILLEGAL_TYPE_OF_ARGUMENT)
```

Use [`has`](/pt-BR/sql-reference/functions/array-functions#has) para verificar se um array contém um valor específico:

```sql
-- Find visits that have at least one goal with ID 591325
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE has(Goals.ID, 591325);
```

Use [`arrayExists`](/pt-BR/sql-reference/functions/array-functions#arrayExists) quando a condição for mais complexa:

```sql
-- Find visits that have at least one goal with ID greater than 1000000
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE arrayExists(id -> id > 1000000, Goals.ID);
```

Você pode filtrar pelo tamanho do array com `length` ou excluir arrays vazios com `notEmpty`:

```sql
-- Visits with at least 3 goals
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE length(Goals.ID) >= 3;

-- Visits with at least one goal (non-empty array)
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE notEmpty(Goals.ID);
```

Para filtrar elementos individuais de uma estrutura aninhada, em vez de linhas inteiras, use `ARRAY JOIN` para expandir os arrays primeiro.
Após `ARRAY JOIN`, cada elemento passa a ser uma linha separada, então a cláusula `WHERE` se aplica a valores escalares.
Para mais informações, consulte a [cláusula `ARRAY JOIN`](/pt-BR/sql-reference/statements/select/array-join). Exemplo:

```sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID, Goal.Serial
LIMIT 10
```

```text
    ┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goal.ID ┃      Goal.EventTime ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
 1. │ 1073752 │ 2014-03-17 16:38:10 │
    ├─────────┼─────────────────────┤
 2. │  591325 │ 2014-03-17 16:38:48 │
    ├─────────┼─────────────────────┤
 3. │  591325 │ 2014-03-17 16:42:27 │
    ├─────────┼─────────────────────┤
 4. │ 1073752 │ 2014-03-17 00:28:25 │
    ├─────────┼─────────────────────┤
 5. │ 1073752 │ 2014-03-17 10:46:20 │
    ├─────────┼─────────────────────┤
 6. │ 1073752 │ 2014-03-17 13:59:20 │
    ├─────────┼─────────────────────┤
 7. │  591325 │ 2014-03-17 22:17:55 │
    ├─────────┼─────────────────────┤
 8. │  591325 │ 2014-03-17 22:18:07 │
    ├─────────┼─────────────────────┤
 9. │  591325 │ 2014-03-17 22:18:51 │
    ├─────────┼─────────────────────┤
10. │ 1073752 │ 2014-03-17 11:37:06 │
    └─────────┴─────────────────────┘
```

Não é possível fazer `SELECT` de uma estrutura de dados aninhada inteira. Você só pode listar explicitamente as colunas individuais que fazem parte dela.

<div id="inserting-data">
  ### Inserção de dados
</div>

Para uma consulta `INSERT`, você deve passar separadamente os arrays de cada coluna componente de uma estrutura de dados aninhada (como se fossem arrays de colunas individuais). Durante a inserção, o sistema verifica se todos têm o mesmo tamanho.

Cada subcoluna aninhada aparece na lista de colunas usando notação de ponto (`Goals.ID`, `Goals.Serial`, ...), e os valores correspondentes são arrays:

```sql
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    -- A visit with two goals: each nested sub-column gets an array of length 2
    (101500, '2014-03-18', 1, 1, 2001, 200001,
     [1073752, 591325], [1, 2],
     ['2014-03-18 10:00:00', '2014-03-18 10:05:00'],
     [100, 200], ['order_a', 'order_b'], [1, 2]),
    -- A visit with no goals: all nested sub-columns get empty arrays
    (101500, '2014-03-18', 1, 0, 2002, 200002,
     [], [], [], [], [], []);
```

Todos os arrays de subcolunas aninhadas em uma única linha devem ter o mesmo comprimento. Comprimentos diferentes causam um erro:

```sql
-- ERROR: Goals.ID has 2 elements, but Goals.Serial has 1
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-18', 1, 1, 2003, 200003,
     [1073752, 591325], [1],
     ['2014-03-18 12:00:00'], [0], [''], [0]);
```

Em uma consulta `DESCRIBE`, as colunas de uma estrutura de dados aninhada também são listadas separadamente da mesma forma.

<div id="alter-limitations">
  ### Limitações de ALTER
</div>

As consultas `ALTER` em estruturas de dados aninhadas apresentam as seguintes limitações:

**Adicionar subcolunas** funciona normalmente. Você pode adicionar uma nova subcoluna a uma estrutura `Nested` existente:

```sql
ALTER TABLE test.visits ADD COLUMN Goals.Revenue Float64;
```

**A remoção de subcolunas** funciona para subcolunas individuais:

```sql
ALTER TABLE test.visits DROP COLUMN Goals.Revenue;
```

**Alterar o tipo** de uma subcoluna funciona e aciona uma mutação (reescrita de dados):

```sql
ALTER TABLE test.visits MODIFY COLUMN Goals.Price Int32;
```

**A renomeação** tem restrições. Você pode renomear uma subcoluna dentro da mesma estrutura aninhada:

```sql
-- OK: stays within the Goals structure
ALTER TABLE test.visits RENAME COLUMN Goals.Price TO Goals.Amount;
```

No entanto, você **não pode**:

* Renomear toda a estrutura aninhada em si (por exemplo, `Goals` para `Conversions`).
* Mover uma subcoluna para outra estrutura aninhada (por exemplo, `Goals.ID` para `OtherNested.ID`).
* Mover uma subcoluna para fora de uma estrutura aninhada ou para dentro dela (por exemplo, `Goals.ID` para `GoalID` ou vice-versa).