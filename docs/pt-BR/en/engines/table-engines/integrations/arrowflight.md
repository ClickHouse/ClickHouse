---
description: 'O motor permite consultar e inserir dados em conjuntos de dados remotos por meio do protocolo Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'Motor de tabela ArrowFlight'
doc_type: 'reference'
---

O motor de tabela ArrowFlight permite que o ClickHouse leia e grave em conjuntos de dados remotos por meio do protocolo [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
Essa integração permite que o ClickHouse interaja com servidores externos compatíveis com Flight em um formato Arrow colunar com alto desempenho.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Parâmetros do motor**

* `host:port` — Endereço do servidor Arrow Flight remoto. Se a porta for omitida, será usada a porta padrão `8815`. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Identificador do conjunto de dados no servidor Flight (usado como descritor PATH ou em uma consulta `SELECT *`, dependendo da configuração `arrow_flight_request_descriptor_type`). [String](../../../sql-reference/data-types/string.md).
* `username` — Nome de usuário para autenticação HTTP básica. [String](../../../sql-reference/data-types/string.md).
* `password` — Senha para autenticação HTTP básica. [String](../../../sql-reference/data-types/string.md).

Se `username` e `password` forem omitidos, a autenticação não será usada (isso só funciona se o servidor Arrow Flight permitir acesso não autenticado).

A lista de colunas é opcional — se for omitida, o esquema será inferido do servidor Arrow Flight remoto por meio de `GetSchema`.

<div id="named-collections">
  ## Coleções nomeadas
</div>

O motor oferece suporte a [coleções nomeadas](/pt-BR/operations/named-collections) para armazenar parâmetros de conexão:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Parâmetros da coleção nomeada:

| Parameter                  | Required        | Default | Description                                                       |
| -------------------------- | --------------- | ------- | ----------------------------------------------------------------- |
| `host` or `hostname`       | No              | `""`    | Hostname do servidor.                                             |
| `port`                     | Yes             | —       | Porta do servidor.                                                |
| `dataset`                  | No              | `""`    | Nome do conjunto de dados ou descritor.                           |
| `use_basic_authentication` | No              | `true`  | Ativa a autenticação básica.                                      |
| `user` or `username`       | If auth enabled | —       | Nome de usuário para autenticação.                                |
| `password`                 | No              | `""`    | Senha para autenticação.                                          |
| `enable_ssl`               | No              | `false` | Ativa a criptografia TLS.                                         |
| `ssl_ca`                   | No              | `""`    | Caminho para o arquivo do certificado da CA para verificação TLS. |
| `ssl_override_hostname`    | No              | `""`    | Substitui o hostname verificado durante a validação de TLS.       |

<div id="settings">
  ## Configurações
</div>

* `arrow_flight_request_descriptor_type` — Controla como o nome do conjunto de dados é enviado ao servidor Flight. Valores possíveis: `path` (padrão, enviado como um descritor PATH) ou `command` (enviado como um descritor CMD com `SELECT * FROM <dataset>`). Use `command` para servidores Flight que esperam comandos SQL (por exemplo, Dremio).

<div id="usage-example">
  ## Exemplo de uso
</div>

Lendo dados de um servidor Arrow Flight remoto:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserindo dados em um servidor remoto do Arrow Flight:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## Notas
</div>

* Se colunas forem especificadas na instrução `CREATE TABLE`, elas deverão corresponder ao esquema retornado pelo servidor Flight.
* Se as colunas forem omitidas, o esquema será inferido automaticamente a partir do servidor remoto.
* Há suporte tanto para leitura (`SELECT`) quanto para escrita (`INSERT`).
* A configuração `arrow_flight_request_descriptor_type` controla se o nome do conjunto de dados é enviado como um descritor PATH ou como um descritor CMD que encapsula uma consulta `SELECT *`.

<div id="see-also">
  ## Veja também
</div>

* [função de tabela arrowFlight](/pt-BR/sql-reference/table-functions/arrowflight)
* [Interface Arrow Flight](/pt-BR/interfaces/arrowflight)
* [especificação do Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [formato Arrow no ClickHouse](/pt-BR/interfaces/formats/Arrow)