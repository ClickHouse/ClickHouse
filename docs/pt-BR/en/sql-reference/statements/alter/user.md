---
description: 'Documentação do USER'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'reference'
---

Altera contas de usuário do ClickHouse.

Sintaxe:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

Para usar `ALTER USER`, você deve ter o privilégio [ALTER USER](../../../sql-reference/statements/grant.md#access-management).

`SET variable = value` é um alias para `MODIFY SETTING variable = value`: ele altera uma única configuração, mantendo as demais. Prefira-o (ou `MODIFY SETTING`) em vez da cláusula `SETTINGS` sem modificador, que substitui toda a lista de configurações e também remove todos os perfis herdados (do perfil pai).

<div id="grantees-clause">
  ## Cláusula GRANTEES
</div>

Especifica os usuários ou funções que podem receber [privilégios](../../../sql-reference/statements/grant.md#privileges) deste usuário, desde que este usuário também tenha todos os acessos necessários concedidos com [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Opções da cláusula `GRANTEES`:

* `user` — Especifica um usuário ao qual este usuário pode conceder privilégios.
* `role` — Especifica uma função à qual este usuário pode conceder privilégios.
* `ANY` — Este usuário pode conceder privilégios a qualquer usuário ou função. Esta é a configuração padrão.
* `NONE` — Este usuário não pode conceder privilégios a nenhum usuário ou função.

Você pode excluir qualquer usuário ou função usando a expressão `EXCEPT`. Por exemplo, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. Isso significa que, se `user1` tiver privilégios concedidos com `GRANT OPTION`, ele poderá conceder esses privilégios a qualquer usuário ou função, exceto `user2`.

<div id="examples">
  ## Exemplos
</div>

Defina as funções atribuídas como padrão:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

Se nenhuma função tiver sido atribuída previamente a um usuário, o ClickHouse lança uma exceção.

Defina todas as funções atribuídas como padrão:

```sql
ALTER USER user DEFAULT ROLE ALL
```

Se uma função for atribuída a um usuário no futuro, ela se tornará padrão automaticamente.

Defina como padrão todas as funções atribuídas, exceto `role1` e `role2`:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

Permite que o usuário da conta `john` conceda seus privilégios ao usuário da conta `jack`:

```sql
ALTER USER john GRANTEES jack;
```

Adiciona novos métodos de autenticação ao usuário, mantendo os já existentes:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Observações:

1. Versões mais antigas do ClickHouse podem não oferecer suporte à sintaxe de múltiplos métodos de autenticação. Portanto, se o servidor do ClickHouse contiver esses usuários e passar por um downgrade para uma versão que não ofereça esse suporte, esses usuários se tornarão inutilizáveis, e algumas operações relacionadas a usuários deixarão de funcionar. Para fazer o downgrade sem problemas, é necessário configurar todos os usuários para que tenham um único método de autenticação antes do downgrade. Como alternativa, se o servidor tiver passado por downgrade sem o procedimento adequado, os usuários com problema deverão ser removidos.
2. `no_password` não pode coexistir com outros métodos de autenticação por motivos de segurança.
   Por isso, não é possível `ADD` um método de autenticação `no_password`. A consulta abaixo gerará um erro:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

Se você quiser remover os métodos de autenticação de um usuário e usar `no_password`, deverá especificar isso no formulário de substituição abaixo.

Redefine os métodos de autenticação e adiciona os especificados na consulta (efeito de um IDENTIFIED inicial sem a palavra-chave ADD):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Redefina os métodos de autenticação e mantenha apenas o adicionado mais recentemente:

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## Cláusula VALID UNTIL
</div>

Permite especificar a data de expiração e, opcionalmente, a hora de um método de autenticação. Aceita uma string como parâmetro. Recomenda-se usar o formato `YYYY-MM-DD [hh:mm:ss] [timezone]` para data e hora. Por padrão, esse parâmetro é igual a `'infinity'`.
A cláusula `VALID UNTIL` só pode ser especificada junto com um método de autenticação, exceto no caso em que nenhum método de autenticação tenha sido especificado na consulta. Nesse cenário, a cláusula `VALID UNTIL` será aplicada a todos os métodos de autenticação existentes.

Exemplos:

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`