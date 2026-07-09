---
description: 'Guia de autenticação baseada em JWT e usuários efêmeros no ClickHouse Cloud'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'Autenticação JWT'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

O ClickHouse pode autenticar usuários usando JSON Web Tokens (JWTs). Diferentemente de outros autenticadores externos, como [LDAP](/pt-BR/operations/external-authenticators/ldap) ou [Kerberos](/pt-BR/operations/external-authenticators/kerberos), a autenticação por JWT não verifica a identidade de usuários preexistentes. Em vez disso, ela cria dinamicamente **usuários efêmeros** com base nas claims contidas em cada token. Esses usuários existem apenas na memória, recebem permissões de acesso derivadas das claims do token e são removidos automaticamente após a expiração do token.

Isso torna a autenticação por JWT fundamentalmente diferente dos métodos baseados em senha ou certificado: não existe nenhuma instrução `CREATE USER ... IDENTIFIED WITH jwt`, e tentar usá-la gera uma exceção. Os usuários JWT são totalmente gerenciados pelo ciclo de vida do token.

<div id="overview">
  ## Visão geral
</div>

O fluxo de authentication funciona da seguinte forma:

1. Um client apresenta um JWT assinado por meio de um dos mecanismos de transporte compatíveis (cabeçalho HTTP `Authorization: Bearer`, o protocolo nativo TCP ou o campo gRPC `jwt`).
2. O ClickHouse valida a assinatura do token.
3. As claims obrigatórias (`exp`, `iat`, `iss`, `sub`, `aud`) são verificadas.
4. Um usuário efêmero é criado na memória com permissões de acesso derivadas das claims `clickhouse:grants` e `clickhouse:roles` do token, resultantes da interseção com um limite de permissões.
5. Quando o token expira, uma tarefa de coleta de lixo em segundo plano remove o usuário.

<div id="token-claims">
  ## Claims do token
</div>

<div id="required-claims">
  ### Claims obrigatórias
</div>

Todo JWT apresentado ao ClickHouse deve conter as seguintes claims:

| Claim | Description                                                                                           |
| ----- | ----------------------------------------------------------------------------------------------------- |
| `alg` | Algoritmo de assinatura (claim do cabeçalho). Valores suportados: `HS256`, `RS256`, `ES256`.          |
| `exp` | Horário de expiração. Define o `valid_until` do usuário efêmero.                                      |
| `iat` | Horário de emissão. Usado para impedir a reutilização de tokens mais antigos para a mesma identidade. |
| `iss` | Emissor. Comparado com o emissor esperado do provedor.                                                |
| `sub` | Subject. Passa a fazer parte do nome de usuário gerado.                                               |
| `aud` | Audiência. Comparada com a audiência esperada do provedor.                                            |

A claim de cabeçalho `kid` (ID da chave) também é obrigatória quando a resolução de chaves baseada em JWKS é usada.

:::note O modo JWKS oferece suporte apenas a chaves RSA
Enquanto provedores com chave estática aceitam qualquer um entre `HS256`, `RS256` ou `ES256`, provedores baseados em JWKS aceitam apenas JWKs cujo `kty` seja `RSA` (ou seja, tokens assinados com `RS256`). Tokens assinados com chaves HMAC (`HS256`) ou EC (`ES256`) não podem ser verificados em um endpoint JWKS e serão rejeitados.
:::

<div id="other-recognized-claims">
  ### Outras claims reconhecidas
</div>

| Claim | Descrição                                                                                                                            |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `nbf` | Horário de início de validade. Essa claim não é obrigatória, mas, se estiver presente, os tokens são rejeitados antes desse horário. |
| `jti` | Reservada. Aceita em tokens, mas atualmente não é validada nem usada.                                                                |

<div id="optional-claims">
  ### Claims opcionais
</div>

| Claim                                                                                                                                                           | Nome padrão         | Descrição                                                                                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Grants                                                                                                                                                          | `clickhouse:grants` | Um array JSON de fragmentos SQL de `GRANT`, por exemplo `["SELECT ON db.*", "INSERT ON db.table1"]`. Cada elemento é interpretado como o corpo de uma instrução `GRANT`. |
| Roles                                                                                                                                                           | `clickhouse:roles`  | Um array JSON de nomes de roles a serem atribuídos, por exemplo `["analyst", "reader"]`.                                                                                 |
| Os nomes padrão das claims podem ser remapeados para nomes de claims personalizados se o seu provedor de identidade usar convenções de nomenclatura diferentes. |                     |                                                                                                                                                                          |

<div id="example-token-header-and-payload">
  ### Exemplo de token, cabeçalho e payload
</div>

```json
{
  "alg": "RS256",
  "kid": "my-key-id"
}
```

```json
{
  "iss": "https://idp.example.com",
  "sub": "jane.doe",
  "aud": "my-clickhouse-cluster",
  "exp": 1719504000,
  "iat": 1719500400,
  "clickhouse:grants": ["SELECT ON analytics.*", "INSERT ON analytics.events"],
  "clickhouse:roles": ["analyst"]
}
```

<div id="ephemeral-user-behavior">
  ## Comportamento do usuário efêmero
</div>

Os usuários JWT diferem dos usuários comuns do ClickHouse em vários aspectos importantes.

<div id="identity-and-naming">
  ### Identidade e nomenclatura
</div>

Cada usuário JWT recebe um UUID determinístico calculado a partir das `claims` `iss`, `sub` e `aud`. Esse UUID é **estável** entre diferentes logins. Um usuário que faz login várias vezes com tokens diferentes (mas com o mesmo emissor, subject e audiência) sempre recebe o mesmo UUID.

O nome de usuário, no entanto, é **volátil**. Ele é construído da seguinte forma:

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

A parte `<claims_hash>` muda sempre que as claims `clickhouse:roles` ou `clickhouse:grants` são alteradas. Isso significa que tokens com diferentes conjuntos de roles ou de grants geram nomes de usuário diferentes, mesmo para a mesma identidade.

<div id="access-rights">
  ### Direitos de acesso
</div>

Os direitos de acesso efetivos são calculados como:

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

Em que `permission_limit` é o conjunto de direitos de acesso detidos por um role de referência ou usuário configurado como limite superior. Os direitos solicitados pelo token que excedem esse limite são descartados sem aviso.

<div id="token-freshness">
  ### Recência do token
</div>

O ClickHouse rastreia a claim `iat` (issued-at) do token autenticado mais recentemente para cada identidade estável. Se for apresentado um token com `iat` igual ao valor armazenado ou anterior a ele, o servidor reutilizará o usuário efêmero existente sem reavaliar as claims. Isso impede que tokens mais antigos reduzam as permissões de um usuário.

<div id="lifetime-and-garbage-collection">
  ### Ciclo de vida e coleta de lixo
</div>

Usuários efêmeros são criados quando um token é autenticado pela primeira vez e removidos por uma tarefa de coleta de lixo em segundo plano após a expiração de `valid_until` (derivado de `exp`). O intervalo do GC é controlado pelo parâmetro `gc_interval` (padrão: 5 minutos).

Entre as execuções do GC, usuários expirados ainda podem aparecer em `system.users`, mas não podem mais se autenticar.

<div id="persistent-access-assignments">
  ### Atribuições de acesso persistentes
</div>

Como o UUID é estável, você pode atribuir perfis de configurações, quotas, políticas de linha e políticas de mascaramento de colunas a um usuário JWT usando instruções SQL. Essas atribuições persistem no armazenamento do controle de acesso (em disco ou no ZooKeeper) e permanecem mesmo após a expiração do token e uma nova autenticação.

Refira-se ao usuário pelo nome de usuário atual:

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
O nome de usuário e o UUID de uma determinada identidade podem ser encontrados nas colunas `name` e `id` de `system.users` enquanto o usuário estiver ativo.
:::

Observe que `ALTER USER` não funciona diretamente com usuários JWT, pois eles são somente leitura. Para atribuir perfis de configurações, cotas ou políticas, use as instruções `ALTER SETTINGS PROFILE`, `ALTER QUOTA` ou `ALTER ROW POLICY`, como mostrado acima.

<div id="differences-from-regular-users">
  ## Diferenças em relação aos usuários regulares
</div>

| Funcionalidade                        | Usuários JWT                                                       | Usuários regulares                                   |
| ------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------- |
| Criação                               | Automática com base nas claims do token                            | instrução `CREATE USER`                              |
| Armazenamento                         | Somente em memória (efêmero)                                       | Disco, ZooKeeper ou arquivo de configuração          |
| `CREATE USER ... IDENTIFIED WITH jwt` | Não suportado (gera exceção)                                       | Todos os outros tipos de autenticação são suportados |
| `ALTER USER` / `DROP USER`            | Não suportado                                                      | Suportado                                            |
| Backup e restauração                  | Não incluídos                                                      | Incluídos                                            |
| Nome de usuário                       | Gerado automaticamente, volátil                                    | Escolhido pelo administrador, fixo                   |
| UUID                                  | Determinístico a partir de `iss`+`sub`+`aud`                       | Aleatório no momento da criação                      |
| Ciclo de vida                         | Limitado pelo `exp` do token                                       | Até ser explicitamente removido                      |
| Direitos de acesso                    | Derivados das claims do token, limitados pelo limite de permissões | Concedidos explicitamente via `GRANT`                |
| Restrições de host                    | Configuração de rede por provedor                                  | cláusula `HOST` por usuário                          |
| Perfis de configuração                | Atribuíveis por UUID (persistentes)                                | Configuráveis diretamente                            |
| Quotas e políticas de linha           | Atribuíveis por UUID (persistentes)                                | Configuráveis diretamente                            |
| Funções padrão                        | Não configuráveis                                                  | Configuráveis                                        |

<div id="sql-security-definer-views">
  ## Views com SQL SECURITY DEFINER
</div>

Quando um usuário JWT efêmero cria uma view com `SQL SECURITY DEFINER`, o servidor cria automaticamente uma cópia sombra persistente do usuário para atuar como definidor da view. Esse usuário sombra:

* Tem o nome `<original_jwt_username>:definer`
* Tem `NO_AUTHENTICATION` (não pode ser usado para fazer login)
* Mantém os mesmos direitos de acesso do usuário JWT original no momento em que a view foi criada

Isso garante que a view continue funcionando depois que o token do usuário efêmero expirar e o usuário original for removido pelo coletor de lixo.

<div id="client-usage">
  ## Uso do cliente
</div>

<div id="passing-token-directly">
  ### Informando um token diretamente
</div>

Use a flag `--jwt` no `clickhouse-client` para se autenticar com um token obtido previamente:

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
A opção `--jwt` é mutuamente exclusiva com `--user`. Quando `--jwt` é especificada, o nome de usuário é derivado do token.
:::

<div id="http-interface">
  ### interface HTTP
</div>

Envie o token como Bearer token no cabeçalho `Authorization`:

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
Sempre envie JWTs por HTTPS. Um Bearer token enviado por HTTP sem criptografia fica exposto a qualquer pessoa no caminho de rede e equivale ao vazamento da credencial.
:::

<div id="oauth2-device-code-login">
  ### Login por código de dispositivo OAuth2
</div>

O `clickhouse-client` oferece suporte a um fluxo interativo de login por código de dispositivo OAuth2 por meio da flag `--login`. Para endpoints do ClickHouse Cloud, o cliente realiza automaticamente a troca de token para obter um JWT específico do ClickHouse. Os tokens são atualizados de forma transparente durante a sessão. Quando um novo token é obtido, o cliente se reconecta automaticamente.

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## Autenticador JWT integrado do ClickHouse Cloud
</div>

Todo serviço do ClickHouse Cloud vem com um autenticador JWT predefinido, usado pelo SQL Console e pelo fluxo `--login` do `clickhouse-client`. Esse autenticador é configurado com:

| Parâmetro         | Valor                                                 |
| ----------------- | ----------------------------------------------------- |
| `iss` (emissor)   | `ClickHouse`                                          |
| `aud` (audiência) | O UUID do serviço (visível na URL do Cloud Console)   |
| `sub` (subject)   | O endereço de e-mail da sua conta do ClickHouse Cloud |

O autenticador integrado tem um limite de permissões definido para a role `default_role` e o usuário `default`. Isso significa que os direitos efetivos de qualquer usuário JWT ficam limitados à interseção com os grants dessas duas entidades, de modo que um token nunca pode elevar privilégios além do que `default_role` e `default` têm permissão para fazer.

Você não precisa configurar nada para usar esse autenticador. Ele é provisionado automaticamente quando o serviço é criado.

<div id="interserver-communication">
  ## Comunicação entre servidores
</div>

Quando uma consulta é encaminhada para outro shard ou réplica, o token JWT é incluído no protocolo interservidor. O nó remoto autentica o token novamente de forma independente, criando seu próprio usuário efêmero.

<div id="troubleshooting">
  ## Solução de problemas
</div>

* **Nenhuma permissão de acesso concedida:** O role ou usuário referenciado pode não ter os grants necessários. Verifique se os roles referenciados em `clickhouse:roles` existem e incluem os grants apropriados.
* **Token rejeitado:** Verifique se `iss`, `aud` e o algoritmo de assinatura do seu token correspondem ao que o provedor JWT espera. Se JWKS for usado, verifique se o `kid` do token corresponde a uma chave no conjunto de chaves do provedor.
* **O usuário desaparece entre consultas:** Usuários efêmeros são removidos após a expiração do token. Use um cliente com suporte à renovação de token (por exemplo, modo `--login`) para sessões de longa duração.
* **`CREATE USER ... IDENTIFIED WITH jwt` falha:** Isso é esperado. Usuários JWT não podem ser criados via DDL. Eles são gerenciados inteiramente pelo ciclo de vida do token.