---
description: 'Ao gravar em uma tabela `Null`, os dados são ignorados. Ao ler de uma
  tabela `Null`, a resposta é vazia.'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Motor de tabela Null'
doc_type: 'reference'
---

Ao gravar dados em uma tabela `Null`, os dados são ignorados.
Ao ler de uma tabela `Null`, a resposta é vazia.

O motor de tabela `Null` é útil para transformações de dados em que você não precisa mais dos dados originais depois que eles foram transformados.
Para essa finalidade, você pode criar uma visão materializada sobre uma tabela `Null`.
Os dados gravados na tabela serão consumidos pela visão, mas os dados brutos originais serão descartados.