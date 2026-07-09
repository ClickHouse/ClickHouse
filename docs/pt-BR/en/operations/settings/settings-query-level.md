---
description: 'Configurações em nível de consulta'
sidebar_label: 'Configurações de sessão em nível de consulta'
slug: /operations/settings/query-level
title: 'Configurações de sessão em nível de consulta'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

Há várias maneiras de executar instruções com configurações específicas.
As configurações são definidas em camadas, e cada camada seguinte redefine os valores anteriores de uma configuração.

<div id="order-of-priority">
  ## Ordem de prioridade
</div>

A ordem de prioridade para definir uma configuração é:

1. Aplicar uma configuração diretamente a um usuário ou em um perfil de configurações

   * SQL (recomendado)
   * adicionar um ou mais arquivos XML ou YAML a `/etc/clickhouse-server/users.d`

2. Configurações de sessão

   * Envie `SET setting=value` pelo console SQL do ClickHouse Cloud ou pelo
     `clickhouse client` no modo interativo. Da mesma forma, você pode usar
     sessões do ClickHouse no protocolo HTTP. Para isso, é necessário especificar o
     parâmetro HTTP `session_id`.

3. Configurações de consulta

   * Ao iniciar o `clickhouse client` no modo não interativo, defina o parâmetro
     de inicialização `--setting=value`.
   * Ao usar a API HTTP, passe parâmetros CGI (`URL?setting_1=value&setting_2=value...`).
   * Defina configurações na
     cláusula
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     da consulta SELECT. O valor da configuração é aplicado somente àquela consulta
     e é redefinido para o valor padrão ou para o valor anterior depois que a consulta é executada.

<div id="converting-a-setting-to-its-default-value">
  ## Convertendo uma configuração para o valor padrão
</div>

Se você alterar uma configuração e quiser revertê-la ao valor padrão, defina o valor como `DEFAULT`. A sintaxe é a seguinte:

```sql
SET setting_name = DEFAULT
```

Por exemplo, o valor padrão de `async_insert` é `0`. Suponha que você altere esse valor para `1`:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

A resposta é:

```response
┌─value──┐
│ 1      │
└────────┘
```

O comando a seguir redefine o valor para 0:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

A configuração agora voltou ao padrão:

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## Configurações personalizadas
</div>

Além das [configurações](/pt-BR/operations/settings/settings.md) comuns, os usuários podem definir configurações personalizadas.
As configurações personalizadas permitem passar **parâmetros específicos da sessão** que podem ser referenciados em consultas, políticas ou funções. Isso é útil quando você precisa:

* Filtrar dados com base na identidade do usuário ou na organização
* Aplicar regras de negócio diferentes com base no contexto
* Manter informações de estado entre consultas em uma sessão

O nome de uma configuração personalizada deve começar com um dos prefixos predefinidos de uma lista que você definir.
A lista de prefixos pode ser especificada usando a configuração de servidor [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes), definida no arquivo de configuração do servidor.

No exemplo abaixo, `SQL_` foi escolhido como prefixo personalizado:

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
No ClickHouse Cloud, não é possível especificar um prefixo personalizado.
Todas as configurações personalizadas de usuário começam com o prefixo `SQL_`.
:::

Para definir uma configuração personalizada, use o comando `SET`:

```sql
SET SQL_a = 123;
```

Para obter o valor atual de uma configuração personalizada, use a função `getSetting()`:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## Exemplos
</div>

Todos estes exemplos definem o valor da configuração `async_insert` como `1` e
mostram como verificar as configurações em um sistema em execução.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### Usando SQL para aplicar uma configuração diretamente a um usuário
</div>

Isso cria o usuário `ingester` com a configuração `async_inset = 1`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### Examine o perfil de configurações e sua atribuição
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### Usando SQL para criar um perfil de configurações e atribuí-lo a um usuário
</div>

Isso cria o perfil `log_ingest` com a configuração `async_inset = 1`:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

Isso cria o usuário `ingester` e atribui a ele o perfil de configurações `log_ingest`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### Criando um perfil de configurações e um usuário com XML
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment-1">
  #### Examine o perfil de configurações e a associação
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### Defina uma configuração para uma sessão
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### Defina uma configuração durante uma consulta
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## Veja também
</div>

* Consulte a página [Configurações](/pt-BR/operations/settings/settings.md) para ver uma descrição das configurações do ClickHouse.
* [Configurações globais do servidor](/pt-BR/operations/server-configuration-parameters/settings.md)