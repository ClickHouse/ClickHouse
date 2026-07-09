---
description: 'Documentação da consulta SELECT'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'Consulta SELECT'
doc_type: 'reference'
---

As consultas `SELECT` são usadas para recuperar dados. Por padrão, os dados solicitados são retornados ao cliente, mas, em conjunto com [INSERT INTO](../../../sql-reference/statements/insert-into.md), podem ser redirecionados para outra tabela.

<div id="syntax">
  ## Sintaxe
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

Todas as cláusulas são opcionais, exceto a lista obrigatória de expressões logo após `SELECT`, abordada em mais detalhes [abaixo](#select-clause).

As particularidades de cada cláusula opcional são abordadas em seções separadas, listadas na mesma ordem em que são executadas:

* [cláusula WITH](../../../sql-reference/statements/select/with.md)
* [cláusula SELECT](#select-clause)
* [cláusula DISTINCT](../../../sql-reference/statements/select/distinct.md)
* [cláusula FROM](../../../sql-reference/statements/select/from.md)
* [cláusula SAMPLE](../../../sql-reference/statements/select/sample.md)
* [cláusula JOIN](../../../sql-reference/statements/select/join.md)
* [cláusula PREWHERE](../../../sql-reference/statements/select/prewhere.md)
* [cláusula WHERE](../../../sql-reference/statements/select/where.md)
* [cláusula WINDOW](../../../sql-reference/window-functions/index.md)
* [cláusula GROUP BY](/pt-BR/sql-reference/statements/select/group-by)
* [cláusula LIMIT BY](../../../sql-reference/statements/select/limit-by.md)
* [cláusula HAVING](../../../sql-reference/statements/select/having.md)
* [cláusula QUALIFY](../../../sql-reference/statements/select/qualify.md)
* [cláusula LIMIT](../../../sql-reference/statements/select/limit.md)
* [cláusula OFFSET](../../../sql-reference/statements/select/offset.md)
* [cláusula UNION](../../../sql-reference/statements/select/union.md)
* [cláusula INTERSECT](../../../sql-reference/statements/select/intersect.md)
* [cláusula EXCEPT](../../../sql-reference/statements/select/except.md)
* [cláusula INTO OUTFILE](../../../sql-reference/statements/select/into-outfile.md)
* [cláusula FORMAT](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## Cláusula SELECT
</div>

As [expressões](/pt-BR/sql-reference/syntax#expressions) especificadas na cláusula `SELECT` são calculadas depois que todas as operações das cláusulas descritas acima são concluídas. Essas expressões funcionam como se fossem aplicadas a linhas individuais no resultado. Se as expressões na cláusula `SELECT` contiverem funções de agregação, o ClickHouse processará as funções de agregação e as expressões usadas como seus argumentos durante a agregação [GROUP BY](/pt-BR/sql-reference/statements/select/group-by).

Se você quiser incluir todas as colunas no resultado, use o símbolo de asterisco (`*`). Por exemplo, `SELECT * FROM ...`.

<div id="dynamic-column-selection">
  ### Seleção dinâmica de colunas
</div>

A seleção dinâmica de colunas (também conhecida como expressão COLUMNS) permite corresponder algumas colunas em um resultado usando uma [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) expressão regular.

```sql
COLUMNS('regexp')
```

Por exemplo, considere a tabela:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

A consulta a seguir seleciona dados de todas as colunas que contêm o símbolo `a` no nome.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

As colunas selecionadas não são retornadas em ordem alfabética.

Você pode usar várias expressões `COLUMNS` em uma consulta e aplicar funções a elas.

Por exemplo:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Cada coluna retornada pela expressão `COLUMNS` é passada para a função como um argumento separado. Você também pode passar outros argumentos para a função, se ela os suportar. Tenha cuidado ao usar funções. Se uma função não suportar o número de argumentos que você passou para ela, o ClickHouse lança uma exceção.

Por exemplo:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

Neste exemplo, `COLUMNS('a')` retorna duas colunas: `aa` e `ab`. `COLUMNS('c')` retorna a coluna `bc`. O operador `+` não pode ser aplicado a 3 argumentos, então ClickHouse lança uma exceção com a mensagem correspondente.

As colunas que correspondem à expressão `COLUMNS` podem ter tipos de dados diferentes. Se `COLUMNS` não corresponder a nenhuma coluna e for a única expressão em `SELECT`, ClickHouse lança uma exceção.

<div id="select-columns-with-like-or-ilike">
  #### Selecione colunas com `LIKE` ou `ILIKE`
</div>

Você também pode selecionar colunas fazendo a correspondência de seus nomes com um padrão após `*`, usando `LIKE`, que diferencia maiúsculas de minúsculas, ou `ILIKE`, que não diferencia maiúsculas de minúsculas:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Os padrões `LIKE` e `ILIKE` seguem a semântica de `LIKE`, não a de expressões regulares. O caractere `%` corresponde a qualquer sequência de caracteres, o caractere `_` corresponde a qualquer caractere individual, e `\` escapa `%`, `_` e `\`. A única diferença entre os dois é que `LIKE` corresponde a nomes de colunas de forma sensível a maiúsculas e minúsculas, enquanto `ILIKE` não diferencia maiúsculas de minúsculas. Por exemplo:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

A consulta seleciona colunas com nomes de dois caracteres que começam com `a`, como `aa` e `ab`.

`* LIKE` e `* ILIKE` também suportam asteriscos qualificados e transformadores de colunas:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### Asterisco
</div>

Você pode colocar um asterisco em qualquer parte de uma consulta no lugar de uma expressão. Quando a consulta é analisada, o asterisco é expandido para a lista de todas as colunas da tabela (excluindo as colunas `MATERIALIZED` e `ALIAS`). Há apenas alguns casos em que o uso de um asterisco se justifica:

* Ao criar um dump da tabela.
* Para tabelas que contêm apenas algumas colunas, como tabelas de sistema.
* Para obter informações sobre quais colunas existem em uma tabela. Nesse caso, defina `LIMIT 1`. Mas é melhor usar a consulta `DESC TABLE`.
* Quando há uma forte filtragem em um pequeno número de colunas usando `PREWHERE`.
* Em subconsultas (já que as colunas que não são necessárias para a consulta externa são excluídas das subconsultas).

Em todos os outros casos, não recomendamos usar o asterisco, pois ele só traz as desvantagens de um SGBD colunar, em vez das vantagens. Em outras palavras, o uso do asterisco não é recomendado.

<div id="extreme-values">
  ### Valores Extremos
</div>

Além dos resultados, você também pode obter os valores mínimo e máximo das colunas do resultado. Para isso, defina a configuração **extremes** como 1. Os valores mínimos e máximos são calculados para tipos numéricos, datas e valores de data/hora. Para as demais colunas, são exibidos os valores padrão.

São calculadas duas linhas extras — uma com os mínimos e outra com os máximos. Essas duas linhas extras são exibidas nos [formatos](../../../interfaces/formats.md) `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` e `Pretty*`, separadas das demais linhas. Elas não são exibidas em outros formatos.

Nos formatos `JSON*` e `XML`, os valores extremos são exibidos em um campo separado chamado &#39;extremes&#39;. Nos formatos `TabSeparated*`, `CSV*` e `Vertical`, a linha vem após o resultado principal e, se houver, após &#39;totals&#39;. Ela é precedida por uma linha vazia (depois dos outros dados). Nos formatos `Pretty*`, a linha é exibida como uma tabela separada após o resultado principal e, se houver, após `totals`. No formato `Template`, os valores extremos são exibidos de acordo com o modelo especificado.

Os valores extremos são calculados para as linhas antes de `LIMIT`, mas depois de `LIMIT BY`. No entanto, ao usar `LIMIT offset, size`, as linhas anteriores a `offset` são incluídas em `extremes`. Em requisições de streaming, o resultado também pode incluir um pequeno número de linhas que passaram por `LIMIT`.

<div id="notes">
  ### Notas
</div>

Você pode usar sinônimos (aliases `AS`) em qualquer parte de uma consulta.

As cláusulas `GROUP BY`, `ORDER BY` e `LIMIT BY` podem aceitar argumentos posicionais. Para habilitar isso, ative a configuração [enable&#95;positional&#95;arguments](/pt-BR/operations/settings/settings#enable_positional_arguments). Assim, por exemplo, `ORDER BY 1,2` ordenará as linhas da tabela pela primeira e depois pela segunda coluna.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

Se a consulta omitir as cláusulas `DISTINCT`, `GROUP BY` e `ORDER BY`, bem como as subconsultas `IN` e `JOIN`, ela será processada totalmente em fluxo, usando uma quantidade de RAM de O(1). Caso contrário, a consulta poderá consumir muita RAM se as restrições apropriadas não forem especificadas:

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

Para mais informações, consulte a seção &quot;Configurações&quot;. É possível usar ordenação externa (salvando tabelas temporárias no disco) e agregação externa.

<div id="select-modifiers">
  ## Modificadores do SELECT
</div>

Você pode usar os seguintes modificadores em consultas `SELECT`.

| Modificador                        | Descrição                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`APPLY`](./apply_modifier.md)     | Permite invocar uma função para cada linha retornada por uma expressão de tabela externa de uma consulta.                                                                                                                                                                                                                                                                                          |
| [`EXCEPT`](./except_modifier.md)   | Especifica os nomes de uma ou mais colunas a serem excluídas do resultado. Todos os nomes de colunas correspondentes são omitidos da saída.                                                                                                                                                                                                                                                        |
| [`REPLACE`](./replace_modifier.md) | Especifica um ou mais [aliases de expressão](/pt-BR/sql-reference/syntax#expression-aliases). Cada alias deve corresponder ao nome de uma coluna da instrução `SELECT *`. Na lista de colunas de saída, a coluna correspondente ao alias é substituída pela expressão desse `REPLACE`. Esse modificador não altera os nomes nem a ordem das colunas. No entanto, pode alterar o valor e o tipo do valor. |

<div id="modifier-combinations">
  ### Combinações de modificadores
</div>

Você pode usar cada modificador individualmente ou combiná-los.

**Exemplos:**

Uso do mesmo modificador várias vezes.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Usar vários modificadores em uma única consulta.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SETTINGS na consulta SELECT
</div>

Você pode especificar as configurações necessárias diretamente na consulta `SELECT`. O valor da configuração é aplicado somente a essa consulta e é redefinido para o valor `default` ou para o valor anterior após a execução da consulta.

Para conhecer outras formas de definir configurações, veja [aqui](/pt-BR/operations/settings/overview).

Para configurações booleanas definidas como true, você pode usar uma sintaxe abreviada, omitindo a atribuição do valor. Quando apenas o nome da configuração é especificado, ela é definida automaticamente como `1` (true).

**Exemplo**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```