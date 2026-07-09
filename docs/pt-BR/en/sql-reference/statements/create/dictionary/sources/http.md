---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'Origem de dicionário HTTP(S)'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'Configure um endpoint HTTP ou HTTPS como origem de dicionário no ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Trabalhar com um servidor HTTP(S) depende de [como o dicionário é armazenado na memória](../layouts/). Se o dicionário for armazenado com `cache` e `complex_key_cache`, o ClickHouse solicita as chaves necessárias enviando uma requisição pelo método `POST`.

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Para que o ClickHouse possa acessar um recurso HTTPS, você deve [configurar o OpenSSL](/pt-BR/operations/server-configuration-parameters/settings#openssl) na configuração do servidor.

Campos de configuração:

| Setting       | Description                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| `url`         | A URL de origem.                                                                                         |
| `format`      | O formato do arquivo. Todos os formatos descritos em [Formatos](/pt-BR/sql-reference/formats) são compatíveis. |
| `credentials` | Autenticação HTTP Basic. Opcional.                                                                       |
| `user`        | Nome de usuário necessário para a autenticação.                                                          |
| `password`    | Senha necessária para a autenticação.                                                                    |
| `headers`     | Todos os cabeçalhos HTTP personalizados usados na requisição HTTP. Opcional.                             |
| `header`      | Um único cabeçalho HTTP.                                                                                 |
| `name`        | Nome do identificador usado para o cabeçalho enviado na requisição.                                      |
| `value`       | Valor definido para um nome de identificador específico.                                                 |

Ao criar um dicionário usando o comando DDL (`CREATE DICTIONARY ...`), os hosts remotos de dicionários HTTP são verificados com base no conteúdo da seção `remote_url_allow_hosts` da configuração para impedir que usuários do banco de dados acessem servidores HTTP arbitrários.