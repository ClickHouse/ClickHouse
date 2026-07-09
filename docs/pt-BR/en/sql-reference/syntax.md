---
description: 'Documentação sobre Sintaxe'
sidebar_label: 'Sintaxe'
sidebar_position: 2
slug: /sql-reference/syntax
title: 'Sintaxe'
doc_type: 'reference'
---

Nesta seção, veremos a sintaxe SQL do ClickHouse.
O ClickHouse usa uma sintaxe baseada em SQL, mas oferece diversas extensões e otimizações.

<div id="query-parsing">
  ## Análise de consultas
</div>

Há dois tipos de parser no ClickHouse:

* *Um parser SQL completo* (um parser descendente recursivo).
* *Um parser de formato de dados* (um parser de fluxo rápido).

O parser SQL completo é usado em todos os casos, exceto na consulta `INSERT`, que usa ambos os parsers.

Vamos examinar a consulta abaixo:

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

Como já mencionado, a consulta `INSERT` usa ambos os parsers.
O fragmento `INSERT INTO t VALUES` é analisado pelo parser completo,
e os dados `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` são analisados pelo parser de formato de dados, ou parser de fluxo rápido.

<details>
  <summary>Ativando o parser completo</summary>

  Você também pode ativar o parser completo para os dados
  usando a configuração [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions).

  Quando a configuração mencionada acima é definida como `1`,
  o ClickHouse primeiro tenta analisar os valores com o parser de fluxo rápido.
  Se isso falhar, o ClickHouse tenta usar o parser completo para os dados, tratando-os como uma [expressão](#expressions) SQL.
</details>

Os dados podem ter qualquer formato.
Quando uma consulta é recebida, o servidor mantém na RAM no máximo [max&#95;query&#95;size](../operations/settings/settings.md#max_query_size) bytes da solicitação
(por padrão, 1 MB), e o restante é analisado em fluxo.
Isso ajuda a evitar problemas com consultas `INSERT` grandes, que é a forma recomendada de inserir dados no ClickHouse.

Ao usar o formato [`Values`](/pt-BR/interfaces/formats/Values) em uma consulta `INSERT`,
pode parecer que os dados são analisados da mesma forma que as expressões em uma consulta `SELECT`, mas não é esse o caso.
O formato `Values` é bem mais limitado.

O restante desta seção trata do parser completo.

:::note
Para mais informações sobre parsers de formato, consulte a seção [Formatos](../interfaces/formats.md).
:::

<div id="spaces">
  ## Espaços
</div>

* Pode haver qualquer quantidade de caracteres de espaço entre construções sintáticas (incluindo o início e o fim de uma consulta).
* Os caracteres de espaço incluem espaço, tabulação, quebra de linha, CR e avanço de página.

<div id="comments">
  ## Comentários
</div>

O ClickHouse oferece suporte a comentários no estilo SQL e no estilo C:

* Comentários no estilo SQL começam com `--`, `#!` ou `# ` e vão até o fim da linha. O espaço após `--` e `#!` pode ser omitido.
* Comentários no estilo C:
  * `//` (ou mais de 2 caracteres `/`) seguido de texto até o fim da linha. Não é necessário espaço após `/`.
  * Podem abranger de `/*` a `*/` no caso de comentários em várias linhas. Também não é necessário usar espaços.
  * Comentários no estilo C podem ser aninhados.

Por exemplo:

```sql
/*
 * Compute the number of days between two dates.
 * /* Returns NULL if either argument is NULL */
 */
SELECT
    dateDiff('day', toDate('2024-01-01'), toDate('2024-12-31')) AS days_in_year, -- 365
    dateDiff('day', toDate('2020-01-01'), today()) AS days_since  #! since 2020
    ///////////////////////////////////////////////////////////////////
    # TODO: add hour/minute variants
```

<div id="keywords">
  ## Palavras-chave
</div>

As palavras-chave no ClickHouse podem ser *case-sensitive* ou *case-insensitive*, dependendo do contexto.

As palavras-chave são **case-insensitive** quando correspondem a:

* ao padrão SQL. Por exemplo, `SELECT`, `select` e `SeLeCt` são todos válidos.
* à implementação de alguns SGBDs populares (MySQL ou Postgres). Por exemplo, `DateTime` é o mesmo que `datetime`.

:::note
Você pode verificar se o nome de um tipo de dado é case-sensitive na tabela [system.data&#95;type&#95;families](/pt-BR/operations/system-tables/data_type_families).
:::

Em contraste com o SQL padrão, todas as demais palavras-chave (incluindo nomes de funções) são **case-sensitive**.

Além disso, palavras-chave não são reservadas.
Elas são tratadas dessa forma apenas no contexto correspondente.
Se você usar [identificadores](#identifiers) com o mesmo nome das palavras-chave, coloque-os entre aspas duplas ou backticks.

Por exemplo, a consulta a seguir é válida se a tabela `table_name` tiver uma coluna com o nome `"FROM"`:

```sql
SELECT "FROM" FROM table_name
```

<div id="identifiers">
  ## Identificadores
</div>

Os identificadores são:

* Nomes de cluster, banco de dados, tabela, partição e coluna.
* [Funções](#functions).
* [Tipos de dados](../sql-reference/data-types/index.md).
* [Aliases de expressão](#expression-aliases).

Os identificadores podem estar entre aspas ou sem aspas, embora a segunda opção seja preferível.

Identificadores sem aspas devem corresponder à regex `^[a-zA-Z_][0-9a-zA-Z_]*$` e não podem ser iguais a [palavras-chave](#keywords).
Veja a tabela abaixo com exemplos de identificadores válidos e inválidos:

| Identificadores válidos                        | Identificadores inválidos              |
| ---------------------------------------------- | -------------------------------------- |
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

Se você quiser usar identificadores iguais a palavras-chave ou quiser usar outros símbolos em identificadores, coloque-os entre aspas duplas ou backticks, por exemplo, `"id"`, `` `id` ``.

:::note
As mesmas regras aplicáveis ao escape em identificadores entre aspas também se aplicam a literais de string. Veja [String](#string) para mais detalhes.
:::

:::tip[Evite usar pontos em nomes de colunas]
Nomes de colunas que contêm pontos, colunas que compartilham o mesmo prefixo com ponto e colunas com o tipo `Array` podem ser interpretados como parte de uma estrutura `Nested` achatada quando `flatten_nested = 1` (o padrão). Isso pode causar validação inesperada do tamanho de arrays em inserções e restrições de renomeação.

Evite usar pontos em nomes de colunas, se possível.
Use underscores (`_`) ou outro separador no lugar de pontos em nomes de colunas, a menos que você precise intencionalmente da semântica de `Nested`.
:::

<div id="literals">
  ## Literais
</div>

No ClickHouse, um literal é um valor representado diretamente em uma consulta.
Em outras palavras, é um valor fixo que não muda durante a execução da consulta.

Os literais podem ser:

* [String](#string)
* [Numéricos](#numeric)
* [Compostos](#compound)
* [`NULL`](#null)
* [Heredocs](#heredoc) (literais de string personalizados)

Veremos cada um deles com mais detalhes nas seções abaixo.

<div id="string">
  ### String
</div>

Literais de String devem estar entre aspas simples. Aspas duplas não são suportadas.

O escaping pode ser feito de uma destas formas:

* usando uma aspa simples adicional, caso em que o caractere de aspas simples `'` (e somente ele) pode ser escapado como `''`, ou
* usando uma barra invertida antes, com as sequências de escape suportadas listadas na tabela abaixo.

:::note
A barra invertida perde seu significado especial, ou seja, é interpretada literalmente se preceder caracteres diferentes dos listados abaixo.
:::

| Escape suportado                           | Descrição                                                                                         |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `\xHH`                                     | Especificação de caractere de 8 bits seguida por qualquer quantidade de dígitos hexadecimais (H). |
| `\N`                                       | reservado, não faz nada (por exemplo, `SELECT 'a\Nb'` retorna `ab`)                               |
| `\a`                                       | alerta                                                                                            |
| `\b`                                       | retrocesso                                                                                        |
| `\e`                                       | caractere de escape                                                                               |
| `\f`                                       | avanço de página                                                                                  |
| `\n`                                       | quebra de linha                                                                                   |
| `\r`                                       | retorno de carro                                                                                  |
| `\t`                                       | tabulação horizontal                                                                              |
| `\v`                                       | tabulação vertical                                                                                |
| `\0`                                       | caractere nulo                                                                                    |
| `\\`                                       | barra invertida                                                                                   |
| `\'` (or `''`)                             | aspas simples                                                                                     |
| `\"`                                       | aspas duplas                                                                                      |
| `` ` ``                                    | crase                                                                                             |
| `\/`                                       | barra normal                                                                                      |
| `\=`                                       | sinal de igual                                                                                    |
| caracteres de controle ASCII (c &lt;= 31). |                                                                                                   |

:::note
Em literais de String, é necessário escapar pelo menos `'` e `\` usando os códigos de escape `\'` (ou `''`) e `\\`.
:::

<div id="numeric">
  ### Numérico
</div>

Os literais numéricos são analisados da seguinte forma:

* Se o literal for precedido por um sinal de menos `-`, o token é ignorado e o resultado é negado após a análise.
* O literal numérico é analisado primeiro como um inteiro sem sinal de 64 bits, usando a função [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul).
  * Se o valor tiver o prefixo `0b` ou `0x`/`0X`, o número será analisado como binário ou hexadecimal, respectivamente.
  * Se o valor for negativo e sua magnitude absoluta for maior que 2<sup>63</sup>, um erro será retornado.
* Se isso falhar, o valor será analisado em seguida como um número de ponto flutuante usando a função [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof).
* Caso contrário, um erro será retornado.

Os valores literais são convertidos para o menor tipo em que se encaixam.
Por exemplo:

* `1` é analisado como `UInt8`
* `256` é analisado como `UInt16`.

:::note Importante
Valores inteiros maiores que 64 bits (`UInt128`, `Int128`, `UInt256`, `Int256`) devem ser convertidos para um tipo maior para serem analisados corretamente:

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

Isso ignora o algoritmo acima e analisa o inteiro com uma rotina que suporta precisão arbitrária.

Caso contrário, o literal será convertido em um número de ponto flutuante e, portanto, estará sujeito à perda de precisão por truncamento.
:::

Para mais informações, consulte [Tipos de dados](../sql-reference/data-types/index.md).

Os sublinhados `_` em literais numéricos são ignorados e podem ser usados para melhorar a legibilidade.

Há suporte aos seguintes literais numéricos:

| Literal numérico                                   | Exemplos                                        |
| -------------------------------------------------- | ----------------------------------------------- |
| **Inteiros**                                       | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **Decimais**                                       | `0.1`                                           |
| **Notação exponencial**                            | `1e100`, `-1e-100`                              |
| **Números de ponto flutuante**                     | `123.456`, `inf`, `nan`                         |
| **Hexadecimal**                                    | `0xc0fe`                                        |
| **String hexadecimal compatível com o padrão SQL** | `x'c0fe'`                                       |
| **Binário**                                        | `0b1101`                                        |
| **String binária compatível com o padrão SQL**     | `b'1101'`                                       |

:::note
Literais octais não são aceitos, para evitar erros acidentais de interpretação.
:::

<div id="compound">
  ### Compostos
</div>

Arrays são criados com `[]`: `[1, 2, 3]`. Tuplas são criadas com `()`: `(1, 'Hello, world!', 2)`.
Tecnicamente, eles não são literais, mas expressões com o operador de criação de array e o operador de criação de tupla, respectivamente.
Um array deve consistir em pelo menos um elemento, e uma tupla deve ter pelo menos dois elementos.

:::note
Há um caso à parte em que tuplas aparecem na cláusula `IN` de uma consulta `SELECT`.
Os resultados da consulta podem incluir tuplas, mas tuplas não podem ser armazenadas em um banco de dados (exceto em tabelas que usam o motor [Memory](../engines/table-engines/special/memory.md)).
:::

<div id="null">
  ### NULL
</div>

`NULL` é usado para indicar que um valor está ausente.
Para armazenar `NULL` em um campo de uma tabela, ele deve ser do tipo [Nullable](../sql-reference/data-types/nullable.md).

:::note
É importante observar o seguinte sobre `NULL`:

* Dependendo do formato de dados (entrada ou saída), `NULL` pode ter uma representação diferente. Para mais informações, consulte [formatos de dados](/pt-BR/interfaces/formats).
* O processamento de `NULL` tem suas particularidades. Por exemplo, se pelo menos um dos argumentos de uma operação de comparação for `NULL`, o resultado dessa operação também será `NULL`. O mesmo vale para multiplicação, adição e outras operações. Recomendamos ler a documentação de cada operação.
* Em consultas, você pode verificar `NULL` usando os operadores [`IS NULL`](/pt-BR/sql-reference/functions/functions-for-nulls#isNull) e [`IS NOT NULL`](/pt-BR/sql-reference/functions/functions-for-nulls#isNotNull), além das funções relacionadas `isNull` e `isNotNull`.
  :::

<div id="heredoc">
  ### Heredoc
</div>

Um [heredoc](https://en.wikipedia.org/wiki/Here_document) é uma maneira de definir uma string (geralmente multilinha), mantendo a formatação original.
Um heredoc é definido como um literal de string personalizado, colocado entre dois símbolos `$`.

Por exemplo:

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note

* Um valor entre dois heredocs é processado &quot;tal como está&quot;.
  :::

:::tip

* Você pode usar um heredoc para incorporar trechos de código SQL, HTML ou XML, etc.
  :::

<div id="defining-and-using-query-parameters">
  ## Definindo e usando parâmetros de consulta
</div>

Os parâmetros de consulta permitem escrever consultas genéricas que contêm placeholders abstratos em vez de identificadores concretos.
Quando uma consulta com parâmetros de consulta é executada,
todos os placeholders são resolvidos e substituídos pelos valores reais dos parâmetros de consulta.

Os parâmetros de consulta podem ser definidos de várias formas:

* `SET param_<name>=<value>` — usando um comando `SET` em uma consulta.
* `--param_<name>='<value>'` — como argumento para o `clickhouse-client` na linha de comando.
* `param_<name>=<value>` — como um parâmetro da string de consulta da URL para a interface HTTP.

Um parâmetro de consulta pode ser referenciado em uma consulta usando `{<name>: <datatype>}`, em que `<name>` é o nome do parâmetro de consulta e `<datatype>` é o tipo de dado para o qual ele será convertido.

<details>
  <summary>Exemplo com comando SET</summary>

  Por exemplo, o SQL a seguir define parâmetros chamados `a`, `b`, `c` e `d` — cada um com um tipo de dado diferente:

  ```sql
  SET param_a = 13;
  SET param_b = 'str';
  SET param_c = '2022-08-04 18:30:53';
  SET param_d = {'10': [11, 12], '13': [14, 15]};

  SELECT
     {a: UInt32},
     {b: String},
     {c: DateTime},
     {d: Map(String, Array(UInt8))};

  13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
  ```
</details>

<details>
  <summary>Exemplo com clickhouse-client</summary>

  Se você estiver usando o `clickhouse-client`, os parâmetros serão especificados como `--param_name=value`. Por exemplo, o parâmetro a seguir tem o nome `message` e é recuperado como `String`:

  ```bash
  clickhouse-client --param_message='hello' --query="SELECT {message: String}"

  hello
  ```

  Se o parâmetro de consulta representar o nome de um banco de dados, tabela, função ou outro identificador, use `Identifier` como tipo. Por exemplo, a consulta a seguir retorna linhas de uma tabela chamada `uk_price_paid`:

  ```sql
  SET param_mytablename = "uk_price_paid";
  SELECT * FROM {mytablename:Identifier};
  ```
</details>

<details>
  <summary>Exemplo com a interface HTTP</summary>

  Os parâmetros de consulta podem ser passados como parâmetros da string de consulta da URL com o prefixo `param_`. Por exemplo:

  ```bash
  curl -s "http://localhost:8123/?param_message=hello" --data-binary "SELECT {message: String}"

  hello
  ```
</details>

<details>
  <summary>Exemplo com a interface web</summary>

  A interface web integrada (`play.html`) detecta automaticamente os placeholders de parâmetro `{name:Type}` na consulta e exibe campos de entrada identificados para cada parâmetro. Os valores dos parâmetros são incluídos na requisição HTTP e também persistidos na URL da página para permitir favoritos e compartilhamento.
</details>

:::note
Os parâmetros de consulta não são substituições genéricas de texto que possam ser usadas em locais arbitrários de consultas SQL arbitrárias.
Eles foram projetados principalmente para funcionar em instruções `SELECT`, no lugar de identificadores ou literais.
:::

<div id="functions">
  ## Funções
</div>

As chamadas de função são escritas como um identificador com uma lista de argumentos (possivelmente vazia) entre `()`.
Ao contrário do SQL padrão, os parênteses são obrigatórios, mesmo para uma lista de argumentos vazia.
Por exemplo:

```sql
now()
```

Há ainda:

* [Funções regulares](/pt-BR/sql-reference/functions/overview).
* [Funções de agregação](/pt-BR/sql-reference/aggregate-functions).

Algumas funções de agregação podem conter duas listas de argumentos entre parênteses. Por exemplo:

```sql
quantile (0.9)(x) 
```

Essas funções de agregação são chamadas de funções &quot;paramétricas&quot;,
e os argumentos da primeira lista são chamados de &quot;parâmetros&quot;.

:::note
A sintaxe das funções de agregação sem parâmetros é a mesma das funções regulares.
:::

<div id="operators">
  ## Operadores
</div>

Os operadores são convertidos nas funções correspondentes durante o parsing da consulta, levando em conta sua prioridade e associatividade.

Por exemplo, a expressão

```text
1 + 2 * 3 + 4
```

é convertido em

```text
plus(plus(1, multiply(2, 3)), 4)`
```

<div id="data-types-and-database-table-engines">
  ## Tipos de dados e motores de tabela do banco de dados
</div>

Os tipos de dados e os motores de tabela na consulta `CREATE` são escritos da mesma maneira que identificadores ou funções.
Em outras palavras, eles podem ou não conter uma lista de argumentos entre parênteses.

Para mais informações, consulte as seções:

* [Tipos de dados](/pt-BR/sql-reference/data-types/index.md)
* [Motores de tabela](/pt-BR/engines/table-engines/index.md)
* [CREATE](/pt-BR/sql-reference/statements/create/index.md).

<div id="expressions">
  ## Expressões
</div>

Uma expressão pode ser qualquer um dos seguintes:

* uma função
* um identificador
* um literal
* a aplicação de um operador
* uma expressão entre parênteses
* uma subconsulta
* um asterisco

Ela também pode conter um [alias](#expression-aliases).

Uma lista de expressões consiste em uma ou mais expressões separadas por vírgulas.
Funções e operadores, por sua vez, podem ter expressões como argumentos.

Uma expressão constante é uma expressão cujo resultado é conhecido durante a análise da consulta, isto é, antes da execução.
Por exemplo, expressões compostas por literais são expressões constantes.

<div id="expression-aliases">
  ## Aliases de expressões
</div>

Um alias é um nome atribuído pelo usuário a uma [expressão](#expressions) em uma consulta.

```sql
expr AS alias
```

As partes da sintaxe acima são explicadas abaixo.

| Parte da sintaxe | Descrição                                                                                                                                                                  | Exemplo                                                                 | Notas                                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `AS`             | A palavra-chave usada para definir aliases. Você pode definir o alias de um nome de tabela ou de um nome de coluna em uma cláusula `SELECT` sem usar a palavra-chave `AS`. | `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | Na função [CAST](/pt-BR/sql-reference/functions/type-conversion-functions#CAST), a palavra-chave `AS` tem outro significado. Consulte a descrição da função. |
| `expr`           | Qualquer expressão compatível com o ClickHouse.                                                                                                                            | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                                                                        |
| `alias`          | Nome para `expr`. Os aliases devem seguir a sintaxe de [identificadores](#identifiers).                                                                                    | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                                                                        |

<div id="notes-on-usage">
  ### Notas de uso
</div>

* Aliases têm escopo global em uma consulta ou subconsulta, e você pode definir um alias em qualquer parte da consulta para qualquer expressão. Por exemplo:

```sql
SELECT (1 AS n) + 2, n`.
```

* Aliases não são visíveis em subconsultas nem entre elas. Por exemplo, ao executar a consulta a seguir, o ClickHouse gera a exceção `Unknown identifier: num`:

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

* Se um alias for definido para as colunas de resultado na cláusula `SELECT` de uma subconsulta, essas colunas ficam visíveis na consulta externa. Por exemplo:

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

* Tenha cuidado com aliases que tenham o mesmo nome que colunas ou tabelas. Vamos considerar o exemplo a seguir:

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

No exemplo anterior, declaramos a tabela `t` com a coluna `b`.
Em seguida, ao consultar os dados, definimos o alias `sum(b) AS b`.
Como os aliases são globais,
o ClickHouse substituiu o literal `b` na expressão `argMax(a, b)` pela expressão `sum(b)`.
Essa substituição causou a exceção.

:::note
Você pode alterar esse comportamento padrão definindo [prefer&#95;column&#95;name&#95;to&#95;alias](/pt-BR/operations/settings/settings#prefer_column_name_to_alias) como `1`.
:::

<div id="asterisk">
  ## Asterisco
</div>

Em uma consulta `SELECT`, um asterisco pode substituir a expressão.
Para mais informações, consulte a seção [SELECT](/pt-BR/sql-reference/statements/select/index.md#asterisk).