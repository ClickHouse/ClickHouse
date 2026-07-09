---
description: 'Documentação dos modos de codificação por cores por coluna na interface web SQL integrada (`/play`)'
sidebar_label: 'Codificação por cores da interface web'
sidebar_position: 23
slug: /interfaces/web-ui-color-coding
title: 'Codificação por cores da interface web'
doc_type: 'reference'
sidebar: false
---

A interface web SQL integrada (`play.html`, disponibilizada no caminho [`/play`](/pt-BR/interfaces/http) de qualquer porta HTTP do ClickHouse) pode colorir as células de resultado para facilitar a visualização imediata de padrões em uma coluna. Cada coluna tem seu próprio modo de codificação por cores, que pode ser alternado de forma independente.

<div id="switching-the-mode">
  ## Alternar o modo
</div>

Um ícone 🌈 aparece à direita de cada cabeçalho de coluna. Clique nele para alternar a coluna entre os modos disponíveis. Em dispositivos com um ponteiro que permite hover (um mouse), o ícone é exibido apenas quando o cursor está sobre o cabeçalho, para não atrapalhar; em dispositivos touch e outros com ponteiro de baixa precisão, que não têm hover, o ícone é sempre exibido para poder ser tocado diretamente.

O conjunto de modos que uma coluna oferece depende do seu tipo:

* Colunas numéricas e colunas `Date`/`DateTime`/`Date32`/`DateTime64` alternam entre `bar` → `heatmap` → `categorical` → `none`.
* Todas as outras colunas alternam entre `none` e `categorical`.

O modo padrão é `bar` para colunas numéricas e `none` para todas as outras colunas, incluindo colunas de data e hora.

<div id="modes">
  ## Modos
</div>

* **`bar`** — desenha uma barra horizontal na célula, proporcional ao valor. Para colunas numéricas, a barra cresce a partir do zero; para colunas `Date`/`DateTime`, ela passa a cobrir o intervalo `min`..`max` da coluna, já que zero não é uma referência significativa para timestamps.
* **`heatmap`** — preenche todo o fundo da célula com uma cor que representa o valor, escalado entre o mínimo e o máximo da coluna.
* **`categorical`** — preenche o fundo da célula com uma cor derivada do hash do valor da célula, de modo que valores iguais recebam a mesma cor e valores diferentes recebam cores diferentes. Isso funciona para qualquer tipo de coluna.
* **`none`** — sem codificação por cores.

As colunas `Date`, `DateTime`, `Date32` e `DateTime64` são coloridas com base no seu valor temporal, convertido em UTC para que a escala seja independente do fuso horário do navegador do usuário.

As cores de fundo de `heatmap` e `categorical` usam o espaço de cor `oklch`, variando apenas a matiz e mantendo a luminosidade e o croma fixos por tema, para que o texto da célula continue legível tanto no tema claro quanto no escuro. O fundo preenche toda a célula mesmo quando uma linha ocupa mais de uma linha.

<div id="categorical-emphasis">
  ## Ênfase categórica na seleção
</div>

No modo `categorical`, ao selecionar uma célula, as outras células com o mesmo valor são destacadas com uma fonte mais pesada e uma cor de texto de contraste total (branco puro no tema escuro, preto puro no tema claro). A própria célula selecionada não recebe destaque. Isso facilita ver em que outros pontos um determinado valor aparece na coluna.

<div id="persistence">
  ## Persistência
</div>

Os modos escolhidos são armazenados por coluna na URL da página e no histórico do navegador, de modo que, ao recarregar a página, compartilhar o link ou navegar para trás e para a frente, eles são preservados. Apenas as opções diferentes do padrão são armazenadas, para manter compactos a URL e o estado do histórico.

<div id="limitations">
  ## Limitações
</div>

* O layout vertical (transposto) de uma única linha não exibe codificação por cores.
* Diferenças em `DateTime64(9)` inferiores a um microssegundo não são distinguidas na escala de cores, o que não faz sentido visualmente em um gradiente.