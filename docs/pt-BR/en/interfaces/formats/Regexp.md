---
alias: []
description: 'Documentação do formato Regexp'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✗     |       |

<div id="description">
  ## Descrição
</div>

O formato `Regex` analisa cada linha dos dados importados de acordo com a expressão regular fornecida.

**Uso**

A expressão regular da configuração [format&#95;regexp](/pt-BR/operations/settings/settings-formats.md/#format_regexp) é aplicada a cada linha dos dados importados. O número de subpadrões na expressão regular deve ser igual ao número de colunas no conjunto de dados importado.

As linhas dos dados importados devem ser separadas pelo caractere de nova linha `'\n'` ou pela nova linha no estilo DOS `"\r\n"`.

O conteúdo de cada subpadrão correspondente é analisado usando o método do tipo de dado correspondente, de acordo com a configuração [format&#95;regexp&#95;escaping&#95;rule](/pt-BR/operations/settings/settings-formats.md/#format_regexp_escaping_rule).

Se a expressão regular não corresponder à linha e [format&#95;regexp&#95;skip&#95;unmatched](/pt-BR/operations/settings/settings-formats.md/#format_regexp_escaping_rule) estiver definido como 1, a linha será ignorada silenciosamente. Caso contrário, uma exceção será lançada.

<div id="example-usage">
  ## Exemplo de uso
</div>

Considere o arquivo `data.tsv`:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

e a tabela `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Vamos inserir na tabela acima os dados do arquivo mencionado anteriormente usando a seguinte consulta:

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Agora podemos executar `SELECT` nos dados da tabela para ver como o formato `Regex` interpretou os dados do arquivo:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>

Ao trabalhar com o formato `Regexp`, você pode usar as seguintes configurações:

* `format_regexp` — [String](/pt-BR/sql-reference/data-types/string.md). Contém a expressão regular no formato [re2](https://github.com/google/re2/wiki/Syntax).

* `format_regexp_escaping_rule` — [String](/pt-BR/sql-reference/data-types/string.md). Há suporte às seguintes regras de escape:

  * CSV (de modo semelhante a [CSV](/pt-BR/interfaces/formats/CSV)
  * JSON (de modo semelhante a [JSONEachRow](/pt-BR/interfaces/formats/JSONEachRow)
  * Escaped (de modo semelhante a [TSV](/pt-BR/interfaces/formats/TabSeparated)
  * Quoted (de modo semelhante a [Values](/pt-BR/interfaces/formats/Values)
  * Raw (extrai os subpadrões como um todo, sem regras de escape, de modo semelhante a [TSVRaw](/pt-BR/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/pt-BR/sql-reference/data-types/int-uint.md). Define se uma exceção deve ser lançada caso a expressão `format_regexp` não corresponda aos dados importados. Pode ser definido como `0` ou `1`.