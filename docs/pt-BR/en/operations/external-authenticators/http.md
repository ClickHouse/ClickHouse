---
description: 'Documentação do HTTP'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

O servidor HTTP pode ser usado para autenticar usuários do ClickHouse. A autenticação HTTP só pode ser usada como autenticador externo para usuários existentes, definidos em `users.xml` ou em caminhos locais de controle de acesso. Atualmente, há suporte ao esquema de autenticação [Basic](https://datatracker.ietf.org/doc/html/rfc7617) com o método GET.

<div id="http-auth-server-definition">
  ## Definição de servidor de autenticação HTTP
</div>

Para definir um servidor de autenticação HTTP, adicione a seção `http_authentication_servers` ao `config.xml`.

**Exemplo**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

Observe que você pode definir vários servidores HTTP na seção `http_authentication_servers` usando nomes distintos.

**Parâmetros**

* `uri` - URI para fazer a solicitação de autenticação

Timeouts em milissegundos no socket usado para se comunicar com o servidor:

* `connection_timeout_ms` - Padrão: 1000 ms.
* `receive_timeout_ms` - Padrão: 1000 ms.
* `send_timeout_ms` - Padrão: 1000 ms.

Parâmetros de retry:

* `max_tries` - O número máximo de tentativas para fazer uma solicitação de autenticação. Padrão: 3
* `retry_initial_backoff_ms` - O intervalo inicial de backoff na nova tentativa. Padrão: 50 ms
* `retry_max_backoff_ms` - O intervalo máximo de backoff. Padrão: 1000 ms

Encaminhamento de cabeçalhos:

Esta parte define quais cabeçalhos serão encaminhados dos cabeçalhos da solicitação do cliente para o autenticador HTTP externo. Observe que os cabeçalhos serão comparados com os configurados sem diferenciar maiúsculas de minúsculas, mas serão encaminhados como estão, ou seja, sem modificações.

<div id="enabling-http-auth-in-users-xml">
  ### Habilitando a autenticação HTTP em `users.xml`
</div>

Para habilitar a autenticação HTTP para o usuário, especifique a seção `http_authentication` em vez de `password` ou de seções semelhantes na definição do usuário.

Parâmetros:

* `server` - Nome do servidor de autenticação HTTP configurado no arquivo principal `config.xml`, conforme descrito anteriormente.
* `scheme` - Esquema de autenticação HTTP. No momento, apenas `Basic` é compatível. Padrão: `Basic`

Exemplo (deve ser inserido em `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
Observe que a autenticação HTTP não pode ser usada em conjunto com nenhum outro mecanismo de autenticação. A presença de qualquer outra seção, como `password`, junto com `http_authentication` fará com que o ClickHouse seja encerrado.
:::

<div id="enabling-http-auth-using-sql">
  ### Habilitando a autenticação HTTP usando SQL
</div>

Quando o [controle de acesso e gerenciamento de contas orientado por SQL](/pt-BR/operations/access-rights#access-control-usage) está habilitado no ClickHouse, também é possível criar, usando instruções SQL, usuários identificados por autenticação HTTP.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...ou `Basic` é o padrão sem definir explicitamente o esquema

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### Passando configurações de sessão
</div>

Se o corpo da resposta do servidor de autenticação HTTP estiver em formato JSON e contiver o subobjeto `settings`, o ClickHouse tentará interpretar seus pares chave-valor como valores de string e defini-los como configurações de sessão para a sessão atual do usuário autenticado. Se a análise falhar, o corpo da resposta do servidor será ignorado.