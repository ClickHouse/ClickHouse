---
description: 'Configurações para usuários e roles.'
sidebar_label: 'Configuração do Usuário'
sidebar_position: 63
slug: /operations/settings/settings-users
title: 'Configurações de usuários e roles'
doc_type: 'reference'
---

A seção `users` do arquivo de configuração `users.xml` contém as configurações do usuário.

:::note
O ClickHouse também oferece suporte a [fluxo de trabalho SQL-driven](/pt-BR/operations/access-rights#access-control-usage) para gerenciamento de usuários. Recomendamos utilizá-lo.
:::

Estrutura da seção `users`:

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

A senha pode ser especificada em texto simples ou em SHA256 (formato hexadecimal).

* Para definir uma senha em texto simples (**não recomendado**), coloque-a em um elemento `password`.

  Por exemplo, `<password>qwerty</password>`. A senha pode ser deixada em branco.

<a id="password_sha256_hex" />

* Para definir uma senha usando seu hash SHA256, coloque-a em um elemento `password_sha256_hex`.

  Por exemplo, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

  Exemplo de como gerar uma senha no shell:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  A primeira linha do resultado é a senha. A segunda linha é o hash SHA256 correspondente.

<a id="password_double_sha1_hex" />

* Para compatibilidade com clientes MySQL, a senha pode ser especificada como um hash double SHA1. Coloque-a no elemento `password_double_sha1_hex`.

  Por exemplo, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

  Exemplo de como gerar uma senha no shell:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  A primeira linha do resultado é a senha. A segunda linha é o hash double SHA1 correspondente.

<div id="totp-authentication-configuration">
  ### Configuração da autenticação TOTP
</div>

A senha de uso único baseada em tempo (TOTP) pode ser usada para autenticar usuários do ClickHouse, gerando códigos de acesso temporários válidos por um período limitado.
Esse método de autenticação TOTP está em conformidade com os padrões da [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238), o que o torna compatível com aplicativos TOTP populares, como Google Authenticator, 1Password e ferramentas semelhantes.
Ela pode ser configurada por meio do arquivo de configuração `users.xml`, além da autenticação baseada em senha.
Ela ainda não tem suporte em controle de acesso baseado em SQL.

Para autenticar usando TOTP, os usuários devem fornecer uma senha primária junto com uma senha de uso único gerada pelo aplicativo TOTP, usando a opção de linha de comando `--one-time-password` ou concatenada à senha principal com o caractere &#39;+&#39;.
Por exemplo, se a senha primária for `some_password` e o código TOTP gerado for `345123`, o usuário poderá especificar `--password some_password+345123` ou `--password some_password --one-time-password 345123` ao se conectar ao ClickHouse. Se nenhuma senha for especificada, o `clickhouse-client` a solicitará interativamente.

Para habilitar a autenticação TOTP para um usuário, configure a seção `time_based_one_time_password` em `users.xml`. Essa seção define as configurações de TOTP, como segredo, período de validade, número de dígitos e algoritmo de hash.

**Exemplo**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
````

Este comando produzirá um segredo codificado em base32 que pode ser adicionado ao campo `secret` em users.xml.

Para habilitar o TOTP para um usuário específico, adicione a qualquer campo existente baseado em senha (como `password` ou `password_sha256_hex`) outra seção `time_based_one_time_password`.

A ferramenta [qrencode](https://linux.die.net/man/1/qrencode) pode ser usada para gerar um código QR para o segredo TOTP.

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

Depois de configurar o TOTP para um usuário, uma senha de uso único pode ser utilizada como parte do processo de autenticação, conforme descrito acima.

### username/ssh-key

Esta configuração permite a autenticação com chaves SSH.

Dada uma chave SSH (como a gerada por `ssh-keygen`), por exemplo

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

Espera-se que o elemento `ssh_key` seja

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

Substitua `ssh-ed25519` por `ssh-rsa` ou `ecdsa-sha2-nistp256` no caso dos outros algoritmos compatíveis.

### Vários métodos de autenticação

Um mesmo usuário pode ser configurado com vários métodos de autenticação usando o elemento `<auth_methods>`. Isso permite que ele se autentique com qualquer um dos métodos listados — por exemplo, um usuário pode ter tanto uma senha quanto uma credencial LDAP, e o login com qualquer uma delas será aceito.

Cada elemento filho de `<auth_methods>` é um wrapper com um nome arbitrário que contém exatamente um tipo de autenticação. O nome do wrapper (por exemplo, `<method1>`, `<primary>`, `<a1>`) não importa; apenas o elemento interno de autenticação é utilizado.

**Exemplo: várias senhas**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**Exemplo: tipos mistos de autenticação**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

Os seguintes tipos de autenticação são compatíveis em `<auth_methods>`:

* **`password`** — senha em texto simples
* **`password_sha256_hex`** — hash SHA256 da senha
* **`password_scram_sha256_hex`** — hash SCRAM-SHA-256 da senha
* **`password_double_sha1_hex`** — hash double SHA1 da senha
* **`ldap`** — autenticação do servidor LDAP
* **`kerberos`** — autenticação Kerberos
* **`ssl_certificates`** — autenticação por certificado SSL
* **`ssh_keys`** — autenticação por chave SSH
* **`http_authentication`** — autenticação HTTP

**Regras e restrições:**

* `<auth_methods>` **não pode** ser usado em conjunto com métodos de autenticação especificados no nível do usuário. Use um estilo ou outro, não ambos.
* `<auth_methods>` deve conter pelo menos um método de autenticação.
* Cada elemento contêiner dentro de `<auth_methods>` deve conter exatamente um tipo de autenticação (com exceção de `<ssh_keys>`, que pode conter vários, para compatibilidade com versões anteriores).
* O TOTP (`<time_based_one_time_password>`) é especificado no nível do usuário (fora de `<auth_methods>`) e se aplica a todos os métodos baseados em senha da lista. Pelo menos um método baseado em senha é necessário quando o TOTP está habilitado.

**Exemplo: `auth_methods` com TOTP**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

Neste exemplo, a verificação por TOTP é aplicada ao método baseado em senha (`<password>`), enquanto o método LDAP se autentica no servidor externo de forma independente.

### gerenciamento_de_acesso

Esta configuração habilita ou desabilita o uso de [controle de acesso e gerenciamento de contas](/pt-BR/operations/access-rights#access-control-usage) baseado em SQL para o usuário.

Valores possíveis:

* 0 — Desabilitado.
* 1 — Habilitado.

Valor padrão: 0.

### grants

Esta configuração permite conceder quaisquer privilégios ao usuário selecionado.
Cada elemento da lista deve ser uma consulta `GRANT` sem nenhum beneficiário especificado.

Exemplo:

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

Esta configuração não pode ser especificada ao mesmo tempo que as configurações
`dictionaries`, `access_management`, `named_collection_control`, `show_named_collections_secrets`
e `allow_databases`.

### user_name/networks

Lista de redes a partir das quais o usuário pode se conectar ao servidor ClickHouse.

Cada elemento da lista pode ter uma das seguintes formas:

* `<ip>` — Endereço IP ou máscara de rede.

  Exemplos: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — Hostname.

  Exemplo: `example01.host.ru`.

  Para verificar o acesso, é feita uma consulta DNS, e todos os endereços IP retornados são comparados com o endereço remoto.

* `<host_regexp>` — Expressão regular para hostnames.

  Exemplo: `^example\d\d-\d\d-\d\.host\.ru$`

  Para verificar o acesso, é feita uma [consulta DNS PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) para o endereço remoto e, em seguida, a expressão regular especificada é aplicada. Depois, outra consulta DNS é feita para os resultados da consulta PTR, e todos os endereços recebidos são comparados com o endereço remoto. Recomendamos fortemente que a expressão regular termine com $.

Todos os resultados das consultas DNS são armazenados em cache até que o servidor seja reiniciado.

**Exemplos**

Para permitir acesso ao usuário a partir de qualquer rede, especifique:

```xml
<ip>::/0</ip>
```

:::note
Não é seguro abrir o acesso a partir de qualquer rede, a menos que você tenha um firewall configurado corretamente ou que o servidor não esteja conectado diretamente à Internet.
:::

Para abrir o acesso somente a localhost, especifique:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

Você pode atribuir um perfil de configurações ao usuário. Os perfis de configurações são configurados em uma seção separada do arquivo `users.xml`. Para mais informações, consulte [Perfis de configurações](../../operations/settings/settings-profiles.md).

### user_name/quota

As quotas permitem monitorar ou limitar o uso de recursos ao longo de um período. As quotas são configuradas na seção `quotas`
do arquivo de configuração `users.xml`.

Você pode atribuir um conjunto de quotas ao usuário. Para uma descrição detalhada da configuração de quotas, consulte [Quotas](/pt-BR/operations/quotas).

### user_name/databases

Nesta seção, você pode limitar as linhas retornadas pelo ClickHouse para consultas `SELECT` feitas pelo usuário atual, implementando assim uma segurança básica em nível de linha.

**Exemplo**

A configuração a seguir faz com que o usuário `user1` possa ver apenas as linhas de `table1` retornadas por consultas `SELECT` em que o valor do campo `id` é 1000.

```xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

O `filter` pode ser qualquer expressão que resulte em um valor do tipo [UInt8](../../sql-reference/data-types/int-uint.md). Em geral, ele contém comparações e operadores lógicos. As linhas de `database_name.table1` para as quais o `filter` resulta em 0 não são retornadas para este usuário. A filtragem é incompatível com operações `PREWHERE` e desabilita a otimização `WHERE→PREWHERE`.

## Funções

Você pode criar qualquer uma das funções predefinidas usando a seção `roles` do arquivo de configuração `user.xml`.

Estrutura da seção `roles`:

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

Essas funções também podem ser concedidas a usuários na seção `users`:

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```