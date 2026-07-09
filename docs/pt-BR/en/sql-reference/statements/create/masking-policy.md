---
description: 'Documentação sobre política de mascaramento'
sidebar_label: 'POLÍTICA DE MASCARAMENTO'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

Cria uma política de mascaramento, que permite transformar ou mascarar dinamicamente valores de colunas para usuários ou papéis específicos quando consultam uma tabela.

:::tip
As políticas de mascaramento fornecem segurança de dados em nível de coluna ao transformar dados sensíveis em tempo de consulta sem modificar os dados armazenados.
:::

Sintaxe:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## Cláusula `UPDATE`
</div>

A cláusula `UPDATE` especifica quais colunas devem ser mascaradas e como transformá-las. Você pode mascarar várias colunas em uma única política.

Exemplos:

* Mascaramento simples: `UPDATE email = '***masked***'`
* Mascaramento parcial: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* Mascaramento baseado em hash: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* Várias colunas: `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## Cláusula WHERE
</div>

A cláusula `WHERE` opcional permite o mascaramento condicional com base nos valores de cada linha. Somente as linhas que atenderem à condição terão o mascaramento aplicado.

Exemplo:

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## Cláusula TO
</div>

Na seção `TO`, especifique a quais usuários e papéis a política deve se aplicar.

* `TO user1, user2`: Aplica-se a usuários/papéis específicos
* `TO ALL`: Aplica-se a todos os usuários
* `TO ALL EXCEPT user1, user2`: Aplica-se a todos os usuários, exceto aos especificados

:::note
Ao contrário das políticas de linha, as políticas de mascaramento não afetam os usuários aos quais a política não se aplica. Se nenhuma política de mascaramento se aplicar a um usuário, ele verá os dados originais.
:::

<div id="priority-clause">
  ## Cláusula PRIORITY
</div>

Quando várias políticas de mascaramento têm como alvo a mesma coluna para um usuário, a cláusula `PRIORITY` determina a ordem de aplicação. As políticas são aplicadas da maior para a menor prioridade.

A prioridade padrão é 0. Políticas com a mesma prioridade são aplicadas em ordem indefinida.

Exemplo:

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note Considerações sobre desempenho

* As políticas de mascaramento podem afetar o desempenho da consulta, dependendo da complexidade da expressão
* Algumas otimizações podem ser desativadas para tabelas com políticas de mascaramento ativas
  :::