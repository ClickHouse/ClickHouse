---
alias: []
description: 'Documentação sobre o formato Pretty'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `Pretty` exibe os dados como tabelas em Unicode,
usando sequências de escape ANSI para mostrar cores no terminal.
A grade completa da tabela é desenhada, e cada linha da tabela ocupa duas linhas no terminal.
Cada bloco de resultados é exibido como uma tabela separada.
Isso é necessário para que os blocos possam ser exibidos sem bufferização dos resultados (a bufferização seria necessária para pré-calcular a largura visível de todos os valores).

[NULL](/pt-BR/sql-reference/syntax.md) é exibido como `ᴺᵁᴸᴸ`.

<div id="example-usage">
  ## Exemplo de uso
</div>

Exemplo (mostrado no formato [`PrettyCompact`](./PrettyCompact.md)):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Não há escape nas linhas em nenhum dos formatos `Pretty`. O exemplo a seguir é mostrado para o formato [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Para evitar despejar dados demais no terminal, apenas as primeiras `10,000` linhas são exibidas.
Se o número de linhas for maior ou igual a `10,000`, a mensagem &quot;Showed first 10 000&quot; será exibida.

:::note
Este formato só é apropriado para exibir o resultado de uma consulta, mas não para fazer parsing de dados.
:::

O formato Pretty oferece suporte à saída de valores totais (ao usar `WITH TOTALS`) e extremos (quando &#39;extremes&#39; está definido como 1).
Nesses casos, os valores totais e os valores extremos são exibidos após os dados principais, em tabelas separadas.
Isso é mostrado no exemplo a seguir, que usa o formato [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />