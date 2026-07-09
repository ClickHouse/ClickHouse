---
description: 'Documentação para Função'
sidebar_label: 'ROLE'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

Cria novas [funções](../../../guides/sre/user-management/index.md#role-management). Uma função é um conjunto de [privilégios](/pt-BR/sql-reference/statements/grant#granting-privilege-syntax). Um [usuário](../../../sql-reference/statements/create/user.md) ao qual uma função é atribuída recebe todos os privilégios dessa função.

Sintaxe:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## Gerenciando funções
</div>

Um usuário pode receber várias funções. Os usuários podem aplicar suas funções atribuídas em combinações arbitrárias por meio da instrução [SET ROLE](../../../sql-reference/statements/set-role.md). O escopo final dos privilégios é a combinação de todos os privilégios de todas as funções aplicadas. Se um usuário tiver privilégios concedidos diretamente à sua conta de usuário, eles também serão combinados com os privilégios concedidos pelas funções.

Um usuário pode ter funções padrão, que são aplicadas quando ele faz login. Para definir funções padrão, use a instrução [SET DEFAULT ROLE](/pt-BR/sql-reference/statements/set-role#set-default-role) ou a instrução [ALTER USER](/pt-BR/sql-reference/statements/alter/user).

Para revogar uma função, use a instrução [REVOKE](../../../sql-reference/statements/revoke.md).

Para excluir uma função, use a instrução [DROP ROLE](/pt-BR/sql-reference/statements/drop#drop-role). A função excluída é automaticamente revogada de todos os usuários e funções aos quais foi atribuída.

<div id="examples">
  ## Exemplos
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Esta sequência de consultas cria a função `accountant`, que tem o privilégio de ler dados do banco de dados `db`.

Atribuindo a função ao usuário `mira`:

```sql
GRANT accountant TO mira;
```

Depois que a função é atribuída, o usuário pode usá-la e executar as consultas permitidas. Por exemplo:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```