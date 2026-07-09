---
description: 'Página de visão geral das configurações.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'Visão geral das configurações'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

:::note
Perfis de configurações baseados em XML e [arquivos de configuração](/pt-BR/operations/configuration-files) ainda não são compatíveis com o ClickHouse Cloud. Para especificar configurações para seu serviço no ClickHouse Cloud, você deve usar [Perfis de configurações via SQL](/pt-BR/operations/access-rights#settings-profiles-management).
:::

Há os seguintes grupos principais de configurações do ClickHouse:

* Configurações globais do servidor
* Configurações de sessão
* Configurações de consulta
* Configurações de operações em segundo plano

As configurações globais se aplicam por padrão, a menos que sejam substituídas em níveis mais específicos. As configurações de sessão podem ser especificadas por meio de perfis, da configuração do usuário e de comandos SET. As configurações de consulta podem ser fornecidas por meio da cláusula SETTINGS e são aplicadas a consultas individuais. As configurações de operações em segundo plano são aplicadas a mutações, merges e possivelmente outras operações, executadas de forma assíncrona em segundo plano.

<div id="see-non-default-settings">
  ## Visualizando configurações diferentes do padrão
</div>

Para ver quais configurações foram alteradas em relação ao valor padrão, você pode consultar a
tabela `system.settings`:

```sql
SELECT name, value FROM system.settings WHERE changed
```

Se nenhuma configuração tiver sido alterada em relação ao valor padrão, o ClickHouse
não retornará nada.

Para verificar o valor de uma configuração específica, você pode especificar o `name` da
configuração na consulta:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

O que retornará algo assim:

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## Leitura adicional
</div>

* Consulte [configurações globais do servidor](/pt-BR/operations/server-configuration-parameters/settings.md) para saber mais sobre como configurar seu
  servidor ClickHouse no nível global do servidor.
* Consulte [configurações de sessão](/pt-BR/operations/settings/settings-query-level.md) para saber mais sobre como configurar seu servidor ClickHouse
  no nível da sessão.
* Consulte [hierarquia de contexto](/pt-BR/development/architecture.md#context) para saber mais sobre o processamento da configuração no ClickHouse.