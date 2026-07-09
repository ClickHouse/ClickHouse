---
description: 'Documentação da UI Web SQL (Play), a interface de consulta integrada no navegador disponível em `/play`'
sidebar_label: 'UI Web SQL'
sidebar_position: 21
slug: /interfaces/web-sql
title: 'UI Web SQL (Play)'
doc_type: 'reference'
---

A UI Web SQL (Play) é a interface de consulta integrada do ClickHouse no navegador. Ela é disponibilizada em qualquer porta HTTP do ClickHouse no caminho `/play` (por exemplo, `http://localhost:8123/play`). Ela permite escrever e executar consultas, visualizar os resultados como tabela ou gráfico e compartilhar uma consulta copiando sua URL.

Toda a interface está contida em `programs/server/play.html`, uma única página autônoma servida diretamente pelo binário do ClickHouse, sem frameworks nem etapa de compilação. A única exceção é a renderização de gráficos: a biblioteca de gráficos `uPlot` é carregada sob demanda a partir de uma CDN de terceiros na primeira vez que um resultado é exibido como gráfico, portanto os gráficos não ficam disponíveis em implantações offline ou com restrição de egress.

<div id="query-tabs">
  ## Abas de consulta
</div>

As abas permitem manter várias consultas lado a lado, em vez de alternar entre elas em um único editor ou depender do histórico do navegador.

Cada aba tem seu próprio texto da consulta, título, parâmetros da consulta e último resultado. As configurações de conexão (URL, usuário e senha) permanecem globais e são compartilhadas por todas as abas.

<div id="when-the-tab-bar-appears">
  ### Quando a barra de abas aparece
</div>

A barra de abas aparece assim que uma consulta é executada ou quando há mais de uma aba. Uma única aba sem resultados fica exatamente como a página era antes de existirem abas, portanto a barra de abas só fica visível quando é necessária.

A aba ativa se integra visualmente à página: seu plano de fundo segue a cor do hash de cada consulta (a mesma cor que o plano de fundo da página já usa), com um gradiente mais saturado na parte superior no tema claro e mais brilhante na parte superior no tema escuro. As abas inativas recebem uma tonalidade baseada no hash do texto da própria consulta, de modo que abas diferentes sejam automaticamente diferenciadas pela cor.

<div id="creating-closing-and-renaming-tabs">
  ### Criando, fechando e renomeando abas
</div>

* Crie uma nova aba com o botão `[+]`, à direita das abas.
* Feche uma aba pelo ícone `x` na própria aba.
* As novas abas recebem nomes padrão como `Query A`, `Query B` e assim por diante.
* Clique no título da aba ativa para editá-lo ali mesmo; o campo de edição se expande para se ajustar ao texto.

<div id="switching-tabs">
  ### Alternar entre abas
</div>

* Clique em uma aba inativa para alternar para ela.
* Role a roda do mouse sobre o painel de abas para alternar entre elas: rolar para cima muda para a aba à esquerda, e rolar para baixo, para a aba à direita (se houver). Tanto a rolagem vertical quanto a horizontal da roda funcionam.

A barra de abas é fixa na horizontal — ela permanece à esquerda durante a rolagem horizontal da página, como o logo do ClickHouse na parte inferior — e, na vertical, rola junto com o restante da página.

<div id="persistence-and-browser-history">
  ### Persistência e histórico do navegador
</div>

O workspace — as abas, seus títulos, a aba ativa, sua ordem e pequenos snapshots de resultados — é persistido no IndexedDB e restaurado ao recarregar. A persistência funciona na medida do possível: se o IndexedDB não estiver disponível, o workspace passa a usar estado em memória na sessão atual.

As abas também se integram à History API do navegador e à URL:

* O estado do histórico armazena a aba ativa, para que os botões de voltar e avançar do navegador alternem entre as abas.
* A URL passa a incluir um parâmetro `tab=<name>`. Ao carregar, a query string da URL e o parâmetro `tab` são reconciliados com as abas salvas: uma aba existente com esse nome é reutilizada (e sua consulta é substituída), ou uma nova aba é criada quando o nome não é encontrado ou está sem nome. Isso permite abrir uma URL com uma nova consulta sem perder suas próprias abas salvas.

<div id="limitations">
  ### Limitações
</div>

Mudar de aba enquanto uma consulta está em execução descarta o estado de execução dessa consulta.

Apenas resultados pequenos têm snapshot para restauração. Um resultado grande (acima do limite de tamanho do snapshot) ou um resultado em imagem não é persistido: após mudar de aba ou recarregar, a aba mantém a consulta, mas não o resultado renderizado, e executar a consulta novamente o reproduz. Isso se aplica tanto a resultados de uma única consulta quanto à saída combinada de uma execução &quot;Run all&quot; (multiconsulta).