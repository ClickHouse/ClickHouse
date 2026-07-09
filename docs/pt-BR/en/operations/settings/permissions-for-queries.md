---
description: 'Configurações de permissões para consultas.'
sidebar_label: 'Permissões para consultas'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: 'Permissões para consultas'
doc_type: 'reference'
---

As consultas no ClickHouse podem ser divididas em vários tipos:

1. Consultas de leitura de dados: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. Consultas de gravação de dados: `INSERT`, `OPTIMIZE`.
3. Consultas de alteração de configurações: `SET`, `USE`.
4. Consultas [DDL](https://en.wikipedia.org/wiki/Data_definition_language): `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

As configurações a seguir regulam as permissões do usuário de acordo com o tipo de consulta:

<div id="readonly">
  ## readonly
</div>

Restringe as permissões para consultas de leitura de dados, gravação de dados e alteração de configurações.

Quando definido como 1, permite:

* Todos os tipos de consultas de leitura (como SELECT e consultas equivalentes).
* Consultas que modificam apenas o contexto da sessão (como USE).

Quando definido como 2, permite o acima, além de:

* SET e CREATE TEMPORARY TABLE

  :::tip
  Consultas como EXISTS, DESCRIBE, EXPLAIN, SHOW PROCESSLIST etc. são equivalentes a SELECT, porque apenas executam SELECT em tabelas do sistema.
  :::

Valores possíveis:

* 0 — Consultas de leitura, gravação e alteração de configurações são permitidas.
* 1 — Apenas consultas de leitura de dados são permitidas.
* 2 — Consultas de leitura de dados e alteração de configurações são permitidas.

Valor padrão: 0

:::note
Após definir `readonly = 1`, o usuário não pode alterar as configurações `readonly` e `allow_ddl` na sessão atual.

Ao usar o método `GET` na [interface HTTP](/pt-BR/interfaces/http), `readonly = 1` é definido automaticamente. Para modificar dados, use o método `POST`.

Definir `readonly = 1` impede o usuário de alterar configurações. Há uma forma de impedir que o usuário altere apenas configurações específicas. Também há uma forma de permitir a alteração apenas de configurações específicas sob as restrições de `readonly = 1`. Para mais detalhes, consulte [Restrições nas configurações](../../operations/settings/constraints-on-settings.md).
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

Permite ou bloqueia consultas [DDL](https://en.wikipedia.org/wiki/Data_definition_language).

Valores possíveis:

* 0 — consultas DDL não são permitidas.
* 1 — consultas DDL são permitidas.

Valor padrão: 1

:::note
Você não pode executar `SET allow_ddl = 1` se `allow_ddl = 0` na sessão atual.
:::

:::note KILL QUERY
`KILL QUERY` pode ser executado com qualquer combinação das configurações readonly e allow&#95;ddl.
:::