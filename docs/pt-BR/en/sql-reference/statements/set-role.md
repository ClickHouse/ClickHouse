---
description: 'Documentação da instrução SET ROLE'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'Instrução SET ROLE'
doc_type: 'referência'
---

Ativa funções para o usuário atual.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

Define as funções padrão de um usuário.

As funções padrão são ativadas automaticamente quando o usuário faz login. Você pode definir como padrão apenas as funções concedidas anteriormente. Se a função não tiver sido concedida ao usuário, o ClickHouse lançará uma exceção.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## Exemplos
</div>

Configure várias funções padrão para um usuário:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Defina todas as funções concedidas como padrão para um usuário:

```sql
SET DEFAULT ROLE ALL TO user
```

Remova as funções padrão de um usuário:

```sql
SET DEFAULT ROLE NONE TO user
```

Defina como padrão todas as funções concedidas, exceto as funções específicas `role1` e `role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```