---
description: 'Documentação sobre a instrução EXECUTE AS'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'Instrução EXECUTE AS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # Instrução EXECUTE AS
</div>

Permite executar consultas como outro usuário.

<div id="syntax">
  ## Sintaxe
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

A primeira forma (sem `subquery`) define que todas as consultas subsequentes na sessão atual serão executadas em nome do `target_user` especificado.

A segunda forma (com `subquery`) executa apenas a `subquery` especificada em nome do `target_user` especificado.

Para que ambas as formas funcionem, é necessário que a configuração `access_control_improvements.allow_impersonate_user`
esteja definida como `1` e que o privilégio `IMPERSONATE` tenha sido concedido. Por exemplo, os comandos a seguir

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

permite ao usuário `user2` executar comandos `EXECUTE AS user1 ...` e também permite ao usuário `user3` executar comandos em nome de qualquer usuário.

Ao se passar por outro usuário, a função [currentUser()](/pt-BR/sql-reference/functions/other-functions#currentUser) retorna o nome desse outro usuário,
e a função [authenticatedUser()](/pt-BR/sql-reference/functions/other-functions#authenticatedUser) retorna o nome do usuário que foi efetivamente autenticado.

<div id="examples">
  ## Exemplos
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```