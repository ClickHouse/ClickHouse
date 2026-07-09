---
description: 'Guia para configurar a autenticação LDAP no ClickHouse'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

O servidor LDAP pode ser usado para autenticar usuários do ClickHouse. Há duas abordagens diferentes para isso:

* Usar o LDAP como autenticador externo para usuários existentes, definidos em `users.xml` ou em caminhos locais de controle de acesso.
* Usar o LDAP como diretório externo de usuários e permitir a autenticação de usuários não definidos localmente, caso existam no servidor LDAP.

Em ambas as abordagens, é preciso definir na config do ClickHouse um servidor LDAP com um nome interno, para que outras partes da config possam se referir a ele.

<div id="ldap-server-definition">
  ## Definição do servidor LDAP
</div>

Para definir o servidor LDAP, você deve adicionar a seção `ldap_servers` ao arquivo `config.xml`.

**Exemplo**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

Observe que é possível definir vários servidores LDAP na seção `ldap_servers` usando nomes distintos.

**Parâmetros**

| Parâmetro                      | Padrão        | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                         | —             | Hostname ou IP do servidor LDAP. Este parâmetro é obrigatório e não pode ficar vazio.                                                                                                                                                                                                                                                                                                                                                                                           |
| `port`                         | `636` / `389` | Porta do servidor LDAP. O padrão é `636` se `enable_tls` estiver definido como `yes`; caso contrário, `389`.                                                                                                                                                                                                                                                                                                                                                                    |
| `bind_dn`                      | —             | Template usado para construir o DN ao qual será feito o bind. O DN resultante será construído substituindo todas as substrings `{user_name}` do template pelo nome de usuário real durante cada tentativa de autenticação.                                                                                                                                                                                                                                                      |
| `auth_dn_prefix`               | —             | **Descontinuado.** Uma alternativa a `bind_dn`. Não pode ser usado junto com `bind_dn`. Quando especificado, o bind DN é construído como `auth_dn_prefix + {user_name} + auth_dn_suffix`. Por exemplo, definir `auth_dn_prefix` como `uid=` e `auth_dn_suffix` como `,ou=users,dc=example,dc=com` equivale a definir `bind_dn` como `uid={user_name},ou=users,dc=example,dc=com`.                                                                                               |
| `auth_dn_suffix`               | —             | **Descontinuado.** Consulte `auth_dn_prefix`.                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `verification_cooldown`        | `0`           | Um período de tempo, em segundos, após uma tentativa de bind bem-sucedida, durante o qual será considerado que o usuário foi autenticado com sucesso em todas as solicitações subsequentes, sem contatar o servidor LDAP. Especifique `0` para desabilitar o cache e forçar o contato com o servidor LDAP em cada solicitação de autenticação.                                                                                                                                  |
| `follow_referrals`             | `false`       | Uma flag para permitir que a biblioteca cliente LDAP siga automaticamente os referrals LDAP retornados pelo servidor. Isso é relevante principalmente para ambientes Microsoft Active Directory, nos quais buscas em subárvore em um `base DN` de alto nível (por exemplo, `DC=example,DC=com`) podem retornar referrals/referências de busca (por exemplo, `DC=DomainDnsZones,...`). Defina como `true` somente quando você precisar explicitamente de buscas entre partições. |
| `enable_tls`                   | `yes`         | Uma flag para acionar o uso de conexão segura com o servidor LDAP. Especifique `no` para o protocolo `ldap://` em texto simples (não recomendado), `yes` para o protocolo LDAP sobre SSL/TLS `ldaps://` (recomendado) ou `starttls` para o protocolo legado StartTLS (protocolo `ldap://` em texto simples, atualizado para TLS).                                                                                                                                               |
| `tls_minimum_protocol_version` | `tls1.2`      | A versão mínima do protocolo SSL/TLS. Valores aceitos: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`.                                                                                                                                                                                                                                                                                                                                                                            |
| `tls_require_cert`             | `demand`      | Comportamento da verificação de certificados do peer SSL/TLS. Valores aceitos: `never`, `allow`, `try`, `demand`.                                                                                                                                                                                                                                                                                                                                                               |
| `tls_cert_file`                | —             | Caminho para o arquivo do certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `tls_key_file`                 | —             | Caminho para o arquivo da chave do certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_ca_cert_file`             | —             | Caminho para o arquivo do certificado da CA.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `tls_ca_cert_dir`              | —             | Caminho para o diretório que contém os certificados da CA.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_cipher_suite`             | —             | Conjunto de cifras permitido (na notação OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `search_limit`                 | `256`         | Número máximo de entradas que podem ser retornadas por consultas de busca LDAP executadas por esta definição de servidor (para detecção de DN de usuário e mapeamento de função).                                                                                                                                                                                                                                                                                               |

**Subparâmetros de `user_dn_detection`**

Seção com parâmetros de busca LDAP para detectar o DN real do usuário autenticado por bind. Isso é usado principalmente em filtros de busca para mapeamento de função adicional quando o servidor é Active Directory. O DN de usuário resultante será usado ao substituir substrings `{user_dn}` onde quer que isso seja permitido. Por padrão, o DN de usuário é definido como igual ao bind DN, mas, assim que a busca é executada, ele será atualizado para o valor real do DN de usuário detectado.

| Parâmetro       | Padrão    | Descrição                                                                                                                                                                                                                                                                                                                               |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | Template usado para construir o `base DN` para a busca LDAP. O DN resultante será construído substituindo todas as substrings `{user_name}` e `{bind_dn}` do template pelo nome de usuário real e pelo bind DN durante a busca LDAP.                                                                                                    |
| `scope`         | `subtree` | Escopo da busca LDAP. Valores aceitos: `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                      |
| `search_filter` | —         | Template usado para construir o filtro de busca da busca LDAP. O filtro resultante será construído substituindo todas as substrings `{user_name}`, `{bind_dn}` e `{base_dn}` do template pelo nome de usuário real, bind DN e `base DN` durante a busca LDAP. Observe que caracteres especiais devem ser escapados corretamente em XML. |

<div id="ldap-external-authenticator">
  ## Autenticador externo LDAP
</div>

Um servidor LDAP remoto pode ser usado como método para verificar senhas de usuários definidos localmente (usuários definidos em `users.xml` ou em caminhos locais de controle de acesso). Para isso, especifique o nome de um servidor LDAP definido anteriormente em vez de `password` ou seções semelhantes na definição do usuário.

A cada tentativa de login, o ClickHouse tenta fazer &quot;bind&quot; no DN especificado pelo parâmetro `bind_dn` na [definição do servidor LDAP](#ldap-server-definition) usando as credenciais fornecidas e, se for bem-sucedido, o usuário será considerado autenticado. Isso costuma ser chamado de método de &quot;simple bind&quot;.

**Exemplo**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

Observe que o usuário `my_user` se refere ao `my_ldap_server`. Esse servidor LDAP deve ser configurado no arquivo principal `config.xml`, conforme descrito anteriormente.

Quando o [Controle de acesso e gerenciamento de contas](/pt-BR/operations/access-rights#access-control-usage) orientado por SQL está habilitado, usuários autenticados por servidores LDAP também podem ser criados com a instrução [CREATE USER](/pt-BR/sql-reference/statements/create/user).

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## Diretório externo de usuários LDAP
</div>

Além dos usuários definidos localmente, um servidor LDAP remoto pode ser usado como fonte de definições de usuários. Para isso, especifique o nome de um servidor LDAP previamente definido (consulte [Definição do servidor LDAP](#ldap-server-definition)) na seção `ldap` dentro da seção `users_directories` do arquivo `config.xml`.

A cada tentativa de login, o ClickHouse tenta localizar a definição do usuário localmente e autenticá-lo como de costume. Se o usuário não estiver definido, o ClickHouse presumirá que a definição existe no diretório LDAP externo e tentará fazer &quot;bind&quot; no DN especificado no servidor LDAP usando as credenciais fornecidas. Se isso for bem-sucedido, o usuário será considerado existente e autenticado. Serão atribuídas ao usuário as funções da lista especificada na seção `roles`. Além disso, uma operação de &quot;search&quot; no LDAP pode ser realizada, e os resultados podem ser transformados e tratados como nomes de funções, sendo então atribuídos ao usuário se a seção `role_mapping` também estiver configurada. Tudo isso implica que o [Controle de acesso e gerenciamento de contas](/pt-BR/operations/access-rights#access-control-usage) orientado por SQL esteja habilitado e que as funções sejam criadas usando a instrução [CREATE ROLE](/pt-BR/sql-reference/statements/create/role).

**Exemplo**

Vai em `config.xml`.

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

Observe que `my_ldap_server`, mencionado na seção `ldap` dentro da seção `user_directories`, deve ser um servidor LDAP previamente definido e configurado no `config.xml` (consulte [Definição do servidor LDAP](#ldap-server-definition)).

**Parâmetros**

| Parameter | Default | Description                                                                                                                                                                                                                                                                     |
| --------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server`  | —       | Um dos nomes de servidor LDAP definidos na seção de config `ldap_servers` acima. Esse parâmetro é obrigatório e não pode ficar vazio.                                                                                                                                           |
| `roles`   | —       | Seção com uma lista de funções definidas localmente que serão atribuídas a cada usuário obtido do servidor LDAP. Se nenhuma função for especificada aqui nem atribuída durante o mapeamento de função (abaixo), o usuário não poderá realizar nenhuma ação após a autenticação. |

**Subparâmetros de `role_mapping`**

Seção com parâmetros de busca LDAP e regras de mapeamento. Quando um usuário se autentica, com a conexão LDAP ainda ativa, é realizada uma busca LDAP usando `search_filter` e o nome do usuário autenticado. Para cada entrada encontrada nessa busca, o valor do atributo especificado é extraído. Para cada valor de atributo que tenha o prefixo especificado, o prefixo é removido, e o restante do valor se torna o nome de uma função local definida no ClickHouse, que deve ter sido criada previamente pela instrução [CREATE ROLE](/pt-BR/sql-reference/statements/create/role). Pode haver várias seções `role_mapping` definidas dentro da mesma seção `ldap`. Todas elas serão aplicadas.

| Parâmetro       | Padrão    | Descrição                                                                                                                                                                                                                                                                                                                                                       |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | Template usado para construir o base DN da busca LDAP. O DN resultante será construído substituindo todas as substrings `{user_name}`, `{bind_dn}` e `{user_dn}` do template pelo nome de usuário real, bind DN e DN de usuário durante cada busca LDAP.                                                                                                              |
| `scope`         | `subtree` | Escopo da busca LDAP. Valores aceitos: `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                                              |
| `search_filter` | —         | Template usado para construir o filtro de busca da busca LDAP. O filtro resultante será construído substituindo todas as substrings `{user_name}`, `{bind_dn}`, `{user_dn}` e `{base_dn}` do template pelo nome de usuário real, bind DN, DN de usuário e base DN durante cada busca LDAP. Observe que os caracteres especiais devem ser escapados corretamente em XML. |
| `attribute`     | `cn`      | Nome do atributo cujos valores serão retornados pela busca LDAP.                                                                                                                                                                                                                                                                                                |
| `prefix`        | vazio     | Prefixo esperado no início de cada string na lista original de strings retornada pela busca LDAP. O prefixo será removido das strings originais, e as strings resultantes serão tratadas como nomes de funções locais.                                                                                                                                            |