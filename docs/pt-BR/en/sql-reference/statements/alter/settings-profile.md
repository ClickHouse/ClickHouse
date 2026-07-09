---
description: 'Documentação para perfis de configuração'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

Altera os perfis de configuração.

Sintaxe:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

A cláusula `ON CLUSTER` permite alterar perfis de configuração em um cluster; consulte [DDL distribuído](../../../sql-reference/distributed-ddl.md).

<div id="replacing-vs-modifying">
  ## Substituição vs. modificação de configurações
</div>

`ALTER SETTINGS PROFILE` oferece duas formas diferentes de alterar as configurações e os perfis pai (herdados) de um perfil. O comportamento de cada uma é bem diferente, por isso é importante escolher a opção certa.

<div id="replacing-form">
  ### Forma de substituição: `SETTINGS` / `INHERIT` sem modificadores
</div>

Uma cláusula `SETTINGS` sem modificadores (sem `ADD`, `MODIFY` ou `DROP`) **substitui toda a lista de configurações e todos os perfis herdados** do perfil exatamente pelo que você listar. Tudo o que existia antes, mas não estiver listado, é removido silenciosamente — não há aviso.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
Como a forma simples `SETTINGS` substitui tudo por completo, usá-la para &quot;sobrescrever uma configuração&quot; em um perfil base já populado removerá todas as outras configurações (e todos os perfis pai) desse perfil. Se você quiser alterar apenas uma configuração e manter as demais, use a forma incremental `MODIFY`/`ADD`/`DROP` descrita abaixo.
:::

Esse é o mesmo comportamento de `SETTINGS` em [`CREATE SETTINGS PROFILE`](../create/settings-profile.md): a cláusula define a lista completa de configurações.

<div id="incremental-form">
  ### Forma incremental: `ADD` / `MODIFY` / `DROP`
</div>

As palavras-chave `ADD`, `MODIFY` e `DROP` alteram entradas individuais, deixando todo o restante do perfil inalterado:

* `ADD SETTINGS variable = value [constraints]` — adiciona uma configuração que ainda não está presente.
* `MODIFY SETTINGS variable = value [constraints]` — substitui a entrada de uma única configuração. A entrada inteira (valor e restrições) é sobrescrita; portanto, especifique novamente `MIN`/`MAX`/`READONLY`/etc. se quiser mantê-los.
* `DROP SETTINGS variable [,...]` — remove as configurações listadas.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — adiciona ou remove perfis pai (herdados).
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — remove todas as configurações ou todos os perfis pai.

Várias dessas cláusulas podem ser combinadas em uma única instrução, por exemplo, `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` é um alias para `MODIFY SETTINGS variable = value`. Ele é oferecido porque `SET` soa natural e porque digitar a cláusula `SETTINGS` de substituição quando a intenção era fazer uma alteração incremental é um erro comum.

<div id="examples">
  ## Exemplos
</div>

Sobrescreva uma única configuração, mantendo o restante de um perfil preenchido:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

Adicione uma nova configuração restrita e remova outra:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

Gerencie os perfis pai incrementalmente:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

Sempre verifique o resultado com [`SHOW CREATE SETTINGS PROFILE`](../show.md):

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## Incremental vs substituição total
</div>

:::warning
Uma cláusula `SETTINGS` isolada **remove todas as configurações existentes e todos os perfis herdados (perfis pai)** do perfil antes de aplicar as novas.
:::

Para alterar uma única configuração e manter as demais, use `ADD SETTINGS` ou `MODIFY SETTINGS` (veja os exemplos abaixo).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

Tanto `ADD SETTINGS` quanto `MODIFY SETTINGS` preservam as outras configurações do perfil, mas tratam de forma diferente uma entrada existente para a *mesma* configuração:

* `ADD SETTINGS variable = value ...` primeiro remove qualquer entrada existente para `variable` e depois insere a nova. Portanto, **substitui o valor junto com todas as restrições** dessa configuração. Qualquer `MIN`, `MAX` ou permissão de escrita (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) previamente definido para `variable` que você não repetir será descartado.
* `MODIFY SETTINGS variable = value ...` **mescla campo por campo**: sobrescreve apenas os campos que você realmente especifica (o valor, ou `MIN`, ou `MAX`, ou a permissão de escrita) e mantém os demais campos dessa configuração como estavam.

:::tip
Em resumo, use `MODIFY SETTINGS` quando quiser ajustar apenas um aspecto de uma configuração (por exemplo, só o valor, mantendo um `MAX` existente); use `ADD SETTINGS` quando quiser redefinir uma configuração do zero.
:::

<div id="examples">
  ## Exemplos
</div>

Crie um perfil para usar nos exemplos abaixo:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

Adicione ou altere uma única configuração, mantendo as demais:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Como `MODIFY` combina campo por campo, alterar apenas o valor de uma configuração mantém as restrições existentes:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

Adicione uma configuração (mantendo as demais), redefinindo-a completamente caso já exista:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

Ao contrário de `MODIFY`, ao executar `ADD` novamente informando apenas um valor, as restrições definidas anteriormente para esse parâmetro são removidas:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

Remova uma ou mais configurações especificadas pelo nome:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Remova todas as configurações de uma só vez:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### Trabalhando com perfis herdados
</div>

Adicione ou remova perfis pai (herdados) sem afetar as configurações do próprio perfil:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```