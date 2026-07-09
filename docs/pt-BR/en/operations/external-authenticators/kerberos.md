---
description: 'Usuários existentes do ClickHouse, devidamente configurados, podem ser autenticados
  por meio do protocolo de autenticação Kerberos.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

Usuários existentes do ClickHouse, devidamente configurados, podem ser autenticados por meio do protocolo de autenticação Kerberos.

No momento, o Kerberos só pode ser usado como autenticador externo para usuários existentes, definidos em `users.xml` ou em caminhos locais de controle de acesso. Esses usuários só podem usar requisições HTTP e devem ser capazes de se autenticar com o mecanismo GSS-SPNEGO.

Para essa abordagem, o Kerberos deve estar configurado no sistema e habilitado na configuração do ClickHouse.

<div id="enabling-kerberos-in-clickhouse">
  ## Habilitando o Kerberos no ClickHouse
</div>

Para habilitar o Kerberos, inclua a seção `kerberos` no `config.xml`. Essa seção pode conter parâmetros adicionais.

<div id="parameters">
  #### Parâmetros
</div>

* `principal` - nome canônico do principal de serviço que será obtido e usado ao aceitar contextos de segurança.
  * Este parâmetro é opcional; se for omitido, o principal `default` será usado.

* `realm` - realm que será usado para restringir a autenticação apenas às requisições cujo realm do iniciador corresponda a ele.
  * Este parâmetro é opcional; se for omitido, nenhuma filtragem adicional por realm será aplicada.

* `keytab` - caminho para o arquivo keytab do serviço.
  * Este parâmetro é opcional; se for omitido, o caminho para o arquivo keytab do serviço deverá ser definido na variável de ambiente `KRB5_KTNAME`.

Exemplo (em `config.xml`):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

Com especificação do principal:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

Com filtro por realm:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
Você pode definir apenas uma seção `kerberos`. A presença de várias seções `kerberos` fará com que o ClickHouse desative a autenticação Kerberos.
:::

:::note
As seções `principal` e `realm` não podem ser especificadas ao mesmo tempo. A presença simultânea das seções `principal` e `realm` fará com que o ClickHouse desative a autenticação Kerberos.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos como autenticador externo para usuários existentes
</div>

O Kerberos pode ser usado como método para verificar a identidade de usuários definidos localmente (usuários definidos em `users.xml` ou em caminhos locais de controle de acesso). No momento, **apenas** requisições pela interface HTTP podem ser *kerberizadas* (por meio do mecanismo GSS-SPNEGO).

O formato do nome do principal do Kerberos geralmente segue este padrão:

* *primary/instance@REALM*

A parte */instance* pode ocorrer zero ou mais vezes. **Espera-se que a parte *primary* do nome canônico do principal do iniciador corresponda ao nome de usuário kerberizado para que a autenticação seja bem-sucedida**.

<div id="enabling-kerberos-in-users-xml">
  ### Habilitando o Kerberos em `users.xml`
</div>

Para habilitar a autenticação Kerberos para o usuário, especifique a seção `kerberos` em vez de `password` ou de seções semelhantes na definição do usuário.

Parâmetros:

* `realm` - um realm que será usado para restringir a autenticação apenas às requisições cujo realm do iniciador corresponda a ele.
  * Este parâmetro é opcional; se for omitido, nenhum filtro adicional por realm será aplicado.

Exemplo (insira em `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
Observe que a autenticação Kerberos não pode ser usada em conjunto com nenhum outro mecanismo de autenticação. A presença de qualquer outra seção, como `password`, junto com `kerberos` fará com que o ClickHouse seja encerrado.
:::

:::info Lembrete
Observe que, agora, quando o usuário `my_user` usa `kerberos`, o Kerberos deve estar habilitado no arquivo principal `config.xml`, conforme descrito anteriormente.
:::

<div id="enabling-kerberos-using-sql">
  ### Ativando o Kerberos usando SQL
</div>

Quando o [Controle de acesso e gerenciamento de contas orientado por SQL](/pt-BR/operations/access-rights#access-control-usage) está habilitado no ClickHouse, usuários identificados pelo Kerberos também podem ser criados usando instruções SQL.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...ou, sem filtrar por realm:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```