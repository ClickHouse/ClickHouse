---
description: 'Documentação sobre otimização guiada por perfil'
sidebar_label: 'Otimização guiada por perfil (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'Otimização guiada por perfil'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # Otimização guiada por perfil
</div>

A Otimização Guiada por Perfil (PGO) é uma técnica de otimização de compilador em que um programa é otimizado com base no perfil de execução.

De acordo com os testes, o PGO ajuda a melhorar o desempenho do ClickHouse. Neles, observamos ganhos de até 15% em QPS na suíte de testes ClickBench. Os resultados mais detalhados estão disponíveis [aqui](https://pastebin.com/xbue3HMU). Os ganhos de desempenho dependem da sua carga de trabalho típica — os resultados podem ser melhores ou piores.

Mais informações sobre PGO no ClickHouse podem ser encontradas no [issue](https://github.com/ClickHouse/ClickHouse/issues/44567) correspondente no GitHub.

<div id="how-to-build-clickhouse-with-pgo">
  ## Como compilar o ClickHouse com PGO?
</div>

Há dois tipos principais de PGO: [Instrumentação](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) e [Amostragem](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (também conhecida como AutoFDO). Este guia descreve o PGO por Instrumentação com o ClickHouse.

1. Compile o ClickHouse no modo instrumentado. No Clang, isso pode ser feito passando a opção `-fprofile-generate` para `CXXFLAGS`.
2. Execute o ClickHouse instrumentado com uma carga de trabalho representativa. Aqui, você precisa usar sua carga de trabalho habitual. Uma das abordagens possíveis é usar o [ClickBench](https://github.com/ClickHouse/ClickBench) como carga de trabalho de exemplo. O ClickHouse no modo de instrumentação pode funcionar mais lentamente, então esteja preparado para isso e não execute o ClickHouse instrumentado em ambientes sensíveis a desempenho.
3. Recompile o ClickHouse mais uma vez com as flags do compilador `-fprofile-use` e os perfis coletados na etapa anterior.

Um guia mais detalhado sobre como aplicar PGO está na [documentação](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization) do Clang.

Se você pretende coletar uma carga de trabalho de exemplo diretamente de um ambiente de produção, recomendamos usar PGO por Amostragem.