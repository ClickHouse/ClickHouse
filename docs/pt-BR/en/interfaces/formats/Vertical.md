---
alias: []
description: 'Documentação do formato Vertical'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Imprime cada valor em uma linha separada, com o nome da coluna especificado. Esse formato é conveniente para imprimir apenas uma ou algumas linhas quando cada linha contém um grande número de colunas.

Observe que [`NULL`](/pt-BR/sql-reference/syntax.md) é exibido como `ᴺᵁᴸᴸ` para facilitar a distinção entre o valor de string `NULL` e a ausência de valor. Colunas JSON serão exibidas em formato Pretty, e `NULL` é exibido como `null`, pois esse é um valor JSON válido e facilmente distinguível de `"null"`.

<div id="example-usage">
  ## Exemplo de uso
</div>

Exemplo:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

As linhas não recebem escape no formato Vertical:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Este formato é apropriado apenas para gerar a saída do resultado de uma consulta, mas não para parsing (recuperar dados para inseri-los em uma tabela).

<div id="format-settings">
  ## Configurações de formato
</div>
