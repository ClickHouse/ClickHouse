---
description: 'Documentação da função de janela ntile'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

Divide as linhas ordenadas dentro de uma partição em um número especificado de grupos, com tamanhos o mais uniforme possível, e retorna o número do grupo ao qual a linha atual pertence. Os grupos são numerados a partir de 1. Em cada partição, as linhas são atribuídas aos grupos em ordem: se o número de linhas não for divisível pelo número de grupos, os primeiros grupos recebem uma linha a mais do que os últimos.

**Sintaxe**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

O argumento `buckets` deve ser um inteiro positivo constante.

A cláusula `ORDER BY` é obrigatória. O frame da janela deve abranger toda a partição (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`), que também é o frame padrão usado quando nada é especificado explicitamente.

Para mais detalhes sobre a sintaxe das funções de janela, consulte: [Funções de janela - Sintaxe](./index.md/#syntax).

**Valor retornado**

* O número do bucket da linha atual dentro da sua partição. [UInt64](../data-types/int-uint.md).

**Exemplo**

O exemplo a seguir divide os jogadores em quatro buckets, ordenados por salário em ordem decrescente.

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

Aqui há sete linhas e quatro buckets; portanto, os três primeiros buckets contêm duas linhas cada, e o último bucket contém apenas uma linha.