---
alias: ['TSV']
description: 'Documentação do formato TSV'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     | `TSV` |

<div id="description">
  ## Descrição
</div>

No formato TabSeparated, os dados são gravados linha por linha. Cada linha contém valores separados por tabulações. Cada valor é seguido por uma tabulação, exceto o último valor da linha, que é seguido por uma quebra de linha. Assume-se estritamente o uso de quebras de linha Unix em todos os casos. A última linha também deve terminar com uma quebra de linha. Os valores são gravados em formato de texto, sem aspas ao redor, e com caracteres especiais escapados.

Esse formato também está disponível com o nome `TSV`.

O formato `TabSeparated` é conveniente para processar dados usando programas próprios e scripts. Ele é usado por padrão na interface HTTP e no modo batch do cliente de linha de comando. Esse formato também permite transferir dados entre diferentes SGBDs. Por exemplo, você pode obter um dump do MySQL e carregá-lo no ClickHouse, ou vice-versa.

O formato `TabSeparated` oferece suporte à saída de valores totais (ao usar WITH TOTALS) e valores extremos (quando &#39;extremes&#39; está definido como 1). Nesses casos, os valores totais e os extremos são exibidos depois dos dados principais. O resultado principal, os valores totais e os extremos são separados entre si por uma linha vazia. Exemplo:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## Formatação de dados
</div>

Números inteiros são escritos na forma decimal. Os números podem conter um caractere &quot;+&quot; extra no início (ignorado durante o parsing e não registrado durante a formatação). Números não negativos não podem conter o sinal de menos. Na leitura, é permitido interpretar uma string vazia como zero ou (para tipos com sinal) uma string composta apenas por um sinal de menos como zero. Números que não cabem no tipo de dado correspondente podem ser convertidos em um número diferente, sem mensagem de erro.

Números de ponto flutuante são escritos na forma decimal. O ponto é usado como separador decimal. Entradas em notação exponencial são compatíveis, assim como &#39;inf&#39;, &#39;+inf&#39;, &#39;-inf&#39; e &#39;nan&#39;. Uma entrada de números de ponto flutuante pode começar ou terminar com um separador decimal.
Durante a formatação, pode haver perda de precisão em números de ponto flutuante.
Durante o parsing, não é estritamente necessário ler o número representável pela máquina mais próximo.

Datas são escritas no formato YYYY-MM-DD e convertidas no mesmo formato, mas com quaisquer caracteres como separadores.
Datas com horas são escritas no formato `YYYY-MM-DD hh:mm:ss` e convertidas no mesmo formato, mas com quaisquer caracteres como separadores.
Tudo isso ocorre no fuso horário do sistema no momento em que o cliente ou o servidor é iniciado (dependendo de qual deles formata os dados). Para datas com horas, o horário de verão não é especificado. Portanto, se um dump tiver horários durante o horário de verão, o dump não corresponderá de forma inequívoca aos dados, e o parsing selecionará um dos dois horários.
Durante uma operação de leitura, datas incorretas e datas com horas podem ser convertidas com overflow natural ou como datas e horas nulas, sem mensagem de erro.

Como exceção, o parsing de datas com horas também é compatível com o formato Unix timestamp, se ele consistir em exatamente 10 dígitos decimais. O resultado não depende do fuso horário. Os formatos `YYYY-MM-DD hh:mm:ss` e `NNNNNNNNNN` são diferenciados automaticamente.

Strings são geradas com caracteres especiais escapados com barra invertida. As seguintes sequências de escape são usadas na saída: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. O parsing também é compatível com as sequências `\a`, `\v` e `\xHH` (sequências de escape hexadecimais) e quaisquer sequências `\c`, em que `c` é qualquer caractere (essas sequências são convertidas em `c`). Assim, a leitura de dados é compatível com formatos em que uma quebra de linha pode ser escrita como `\n`, como `\`, ou como uma quebra de linha. Por exemplo, a string `Hello world` com uma quebra de linha entre as palavras em vez de um espaço pode ser convertida em qualquer uma das seguintes variações:

```text
Hello\nworld

Hello\
world
```

A segunda variante é suportada porque o MySQL a usa ao gravar dumps separados por tabulação.

O conjunto mínimo de caracteres que você precisa escapar ao passar dados no formato TabSeparated: tabulação, quebra de linha (LF) e barra invertida.

Apenas um pequeno conjunto de símbolos é escapado. Você pode facilmente se deparar com um valor de string que seu terminal vai corromper na saída.

Arrays são gravados como uma lista de valores separados por vírgulas em `[]`. Itens numéricos no array são formatados normalmente. Os tipos `Date` e `DateTime` são gravados entre aspas simples. Strings são gravadas entre aspas simples com as mesmas regras de escape acima.

[NULL](/pt-BR/sql-reference/syntax.md) é formatado de acordo com a configuração [format&#95;tsv&#95;null&#95;representation](/pt-BR/operations/settings/settings-formats.md/#format_tsv_null_representation) (o valor padrão é `\N`).

Nos dados de entrada, os valores de ENUM podem ser representados como nomes ou como IDs. Primeiro, tentamos corresponder o valor de entrada ao nome do ENUM. Se isso falhar e o valor de entrada for um número, tentamos corresponder esse número ao ID do ENUM.
Se os dados de entrada contiverem apenas IDs de ENUM, é recomendável ativar a configuração [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) para otimizar o parsing de ENUM.

Cada elemento das estruturas [Nested](/pt-BR/sql-reference/data-types/nested-data-structures/index.md) é representado como um array.

Por exemplo:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="inserting-data">
  ### Inserindo dados
</div>

Use o seguinte arquivo TSV, chamado `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insira os dados:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### Leitura de dados
</div>

Leia os dados no formato `TabSeparated`:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

A saída estará em formato separado por tabulação:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                                                                                             | Descrição                                                                                                                                                                                                                                                                  | Padrão  |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [`format_tsv_null_representation`](/pt-BR/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Representação personalizada de NULL no formato TSV.                                                                                                                                                                                                                        | `\N`    |
| [`input_format_tsv_empty_as_default`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | Trata campos vazios na entrada TSV como valores padrão. Para expressões padrão complexas, [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/pt-BR/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) também deve estar habilitado. | `false` |
| [`input_format_tsv_enum_as_number`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | Trata valores enum inseridos em formatos TSV como índices de enum.                                                                                                                                                                                                         | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | Usa alguns ajustes e heurísticas para inferir o esquema no formato TSV. Se estiver desabilitado, todos os campos serão inferidos como Strings.                                                                                                                             | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/pt-BR/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | Se estiver definido como true, o fim de linha no formato de saída TSV será `\r\n` em vez de `\n`.                                                                                                                                                                          | `false` |
| [`input_format_tsv_crlf_end_of_line`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | Se estiver definido como true, o fim de linha no formato de entrada TSV será `\r\n` em vez de `\n`.                                                                                                                                                                        | `false` |
| [`input_format_tsv_skip_first_lines`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | Ignora o número especificado de linhas no início dos dados.                                                                                                                                                                                                                | `0`     |
| [`input_format_tsv_detect_header`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | Detecta automaticamente o cabeçalho com nomes e tipos no formato TSV.                                                                                                                                                                                                      | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | Ignora linhas vazias no final dos dados.                                                                                                                                                                                                                                   | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/pt-BR/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | Permite um número variável de colunas no formato TSV, ignora colunas extras e usa valores padrão para colunas ausentes.                                                                                                                                                    | `false` |