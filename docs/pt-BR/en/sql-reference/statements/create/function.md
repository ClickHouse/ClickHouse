---
description: 'Documentação da FUNCTION'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - função definida pelo usuário (UDF)'
doc_type: 'referência'
---

Cria uma função definida pelo usuário (UDF) com base em uma expressão lambda. A expressão deve ser composta por parâmetros de função, constantes, operadores ou outras chamadas de função.

**Sintaxe**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

Uma função pode ter um número arbitrário de parâmetros.

Há algumas restrições:

* O nome de uma função deve ser único entre as funções definidas pelo usuário e as funções do sistema.
* Funções recursivas não são permitidas.
* Todas as variáveis usadas por uma função devem ser especificadas na lista de parâmetros.

Se alguma restrição for violada, uma exceção será lançada.

**Exemplo**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

Uma [função condicional](../../../sql-reference/functions/conditional-functions.md) é usada em uma função definida pelo usuário na consulta a seguir:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

Substitua uma UDF existente:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## Conteúdo relacionado
</div>

<div id="executable-udfs">
  ### [UDFs executáveis](/pt-BR/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [Funções definidas pelo usuário no ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
