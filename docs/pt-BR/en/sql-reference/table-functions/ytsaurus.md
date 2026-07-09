---
description: 'A função de tabela permite a leitura de dados do cluster YTsaurus.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # ytsaurus Função de tabela
</div>

<ExperimentalBadge />

A função de tabela permite ler dados do cluster YTsaurus.

<div id="syntax">
  ## Sintaxe
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
Este é um recurso experimental que pode mudar no futuro de formas incompatíveis com versões anteriores.
Habilite o uso da função de tabela YTsaurus
com a configuração [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/pt-BR/operations/settings/settings#allow_experimental_ytsaurus_table_engine).
Digite o comando `set allow_experimental_ytsaurus_table_function = 1`.
:::

<div id="arguments">
  ## Argumentos
</div>

* `http_proxy_url` — URL do proxy HTTP do YTsaurus.
* `cypress_path` — Caminho do Cypress para a fonte de dados.
* `oauth_token` — Token OAuth.
* `format` — O [formato](/pt-BR/interfaces/formats) da fonte de dados.

**Valor retornado**

Uma tabela com a estrutura especificada para ler dados do caminho do Cypress especificado no cluster YTsaurus.

**Veja também**

* [motor YTsaurus](/pt-BR/engines/table-engines/integrations/ytsaurus.md)