---
title: Diretrizes para entradas de changelog
---

<div id="changelog-entry-guidelines">
  # Diretrizes para entradas de changelog
</div>

Boas entradas de changelog ajudam os usuários a entender rapidamente o que há de novo e como isso os afeta. Pedimos aos colaboradores que preencham uma entrada de changelog em linguagem acessível ao usuário, que será incluída no changelog de cada lançamento.

A seguir, apresentamos algumas diretrizes simples para escrever uma boa entrada de changelog.

<div id="write-with-the-user-in-mind-not-the-developer">
  ## Escreva com o usuário em mente, não com o desenvolvedor
</div>

A entrada de changelog tem como objetivo comunicar a mudança ao *usuário*, e não apenas ao *desenvolvedor*.
Ao escrever a entrada, quando apropriado, tente comunicar não apenas ***o que*** mudou, mas também ***por que*** essa mudança é útil para o usuário ou ***como*** ela o afeta.

Por exemplo, em vez de:

> Adiciona a tabela `system.iceberg_history`

Escreva:

> Agora os usuários podem visualizar snapshots históricos de tabelas Iceberg usando a nova tabela `system.iceberg_history`.

Em vez de:

> Adiciona as funções `stringBytesUniq` e `stringBytesEntropy` para procurar dados possivelmente aleatórios ou criptografados.

Escreva:

> Agora você pode detectar dados potencialmente criptografados ou aleatórios em suas strings usando as novas funções `stringBytesUniq` e `stringBytesEntropy`, o que ajuda a identificar problemas de qualidade de dados ou questões de segurança.

<div id="keep-it-simple">
  ## Mantenha a simplicidade
</div>

Evite jargão técnico que o usuário talvez não entenda sem explicação. Prefira entre 1 e 5 frases e
não tenha medo de usar um LLM para ajudar a identificar erros de digitação, erros gramaticais ou reformular a entrada de uma
forma mais amigável para o usuário (não é trapaça, eu prometo!)

Em vez de:

> Dar suporte a subconsultas correlacionadas como argumento da expressão `EXISTS`

Escreva:

> Agora você pode usar subconsultas que fazem referência a colunas da consulta externa em cláusulas `EXISTS`.

Um exemplo de uma entrada clara e simples:

> Torna as configurações de cache de páginas ajustáveis no nível de cada consulta. Isso é necessário para permitir experimentação mais rápida e ajuste fino em consultas de alta taxa de transferência e baixa latência.

<div id="follow-a-few-simple-formatting-guidelines">
  ## Siga algumas diretrizes simples de formatação
</div>

<div id="write-in-full-sentences-and-in-the-present-tense">
  ### Escreva frases completas e no presente
</div>

Em vez de:

> Corrigido um crash: se uma exceção é lançada ao tentar remover um arquivo temporário

Escreva:

> Corrige um crash em que uma exceção é lançada ao tentar remover um arquivo temporário.

<div id="use-backticks-where-necessary">
  ### Use backticks quando necessário
</div>

Coloque entre backticks elementos de código, como settings, nomes de funções, instruções SQL, nomes de formatos e tipos de dados. Em geral,
qualquer coisa que você digitaria no `clickhouse-client` deve ser colocada entre backticks. Isso ajuda a tornar as entradas do changelog mais legíveis.

Em vez de:

> As configurações use&#95;skip&#95;indexes&#95;if&#95;final e use&#95;skip&#95;indexes&#95;if&#95;final&#95;exact&#95;mode agora têm True como valor padrão

Escreva:

> As configurações `use_skip_indexes_if_final` e `use_skip_indexes_if_final_exact_mode` agora têm `True` como valor padrão

<div id="try-to-follow-a-consistent-format">
  ### Tente seguir um formato consistente
</div>

Tente seguir o mesmo formato: O que faz → Por que isso importa para o usuário → Como usar (se necessário). Isso torna as entradas fáceis de consultar e previsíveis para os leitores.

Por exemplo:

> Agora você pode filtrar os resultados da busca vetorial antes ou depois da operação de busca, o que dá mais controle sobre o equilíbrio entre desempenho e precisão. Use a nova configuração `vector_search_filter_mode` para escolher a abordagem de sua preferência.