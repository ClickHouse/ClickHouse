---
description: 'Protocolos componíveis permitem uma configuração mais flexível do acesso TCP
  ao servidor ClickHouse.'
sidebar_label: 'Protocolos componíveis'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Protocolos componíveis'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

Os protocolos componíveis permitem configurar com mais flexibilidade o acesso TCP ao
servidor ClickHouse. Essa configuração pode coexistir com a configuração
convencional ou substituí-la.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## Configurando protocolos componíveis
</div>

Os protocolos componíveis podem ser configurados em um arquivo de configuração XML. A seção de protocolos
é identificada pelas tags `protocols` no arquivo de configuração XML:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### Configurando camadas de protocolo
</div>

Você pode definir camadas de protocolo com módulos básicos. Por exemplo, para definir uma
camada HTTP, adicione um novo módulo básico à seção `protocols`:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

Os módulos podem ser configurados da seguinte forma:

* `plain_http` - nome ao qual outra camada pode fazer referência
* `type` - indica o handler de protocolo que será instanciado para processar dados.
  Ele tem o seguinte conjunto de handlers de protocolo predefinidos:
  * `tcp` - handler de protocolo nativo do ClickHouse
  * `http` - handler de protocolo HTTP do ClickHouse
  * `tls` - camada de criptografia TLS
  * `proxy1` - camada PROXYv1
  * `mysql` - handler de protocolo de compatibilidade com MySQL
  * `postgres` - handler de protocolo de compatibilidade com PostgreSQL
  * `prometheus` - handler do protocolo Prometheus
  * `interserver` - handler de interservidor do ClickHouse

:::note
O handler do protocolo `gRPC` não está implementado para `Composable protocols`
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### Configurando endpoints
</div>

Os endpoints (portas de escuta) são representados pelas tags `<port>` e, opcionalmente, `<host>`.
Por exemplo, para configurar um endpoint na camada HTTP adicionada anteriormente, poderíamos
modificar nossa configuração da seguinte forma:

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

Se a tag `<host>` for omitida, o `<listen_host>` da configuração raiz será
usado.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### Configurando sequências de camadas
</div>

As sequências de camadas são definidas com a tag `<impl>` e com a referência a outro
módulo. Por exemplo, para configurar uma camada TLS sobre o nosso módulo plain&#95;http,
podemos ajustar ainda mais nossa configuração da seguinte forma:

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### Associando endpoints a camadas
</div>

Endpoints podem ser associados a qualquer camada. Por exemplo, podemos definir endpoints para
HTTP (porta 8123) e HTTPS (porta 8443):

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### Definição de endpoints adicionais
</div>

É possível definir endpoints adicionais referenciando qualquer módulo e omitindo a
tag `<type>`. Por exemplo, podemos definir o endpoint `another_http` para o
módulo `plain_http` da seguinte forma:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### Handlers HTTP personalizados por endpoint
</div>

Por padrão, todas as entradas do protocolo `type=http` compartilham a mesma configuração
`<http_handlers>`. Você pode sobrescrever isso adicionando uma tag `<handlers>` que aponta
para uma seção de configuração diferente. Isso permite que cada porta HTTP use um
conjunto diferente de regras de roteamento HTTP.

Por exemplo, para executar uma API HTTP alternativa na porta 8124 com seus próprios handlers:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

Neste exemplo, as solicitações para a porta 8123 usam as regras padrão de `<http_handlers>`,
enquanto as solicitações para a porta 8124 usam as regras de `<http_handlers_alt>`. Se `<handlers>`
for omitido, o endpoint volta a usar o `<http_handlers>` padrão.

A seção de handlers personalizados segue o mesmo formato de
[`<http_handlers>`](/pt-BR/docs/operations/server-configuration-parameters/settings#http_handlers).
As alterações na seção de handlers personalizados são detectadas durante a recarga da configuração, e o
endpoint correspondente é reiniciado automaticamente.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### Especificando parâmetros adicionais da camada
</div>

Alguns módulos podem incluir parâmetros adicionais da camada. Por exemplo, a camada TLS
permite especificar uma chave privada (`privateKeyFile`) e arquivos de certificado (`certificateFile`)
da seguinte forma:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```