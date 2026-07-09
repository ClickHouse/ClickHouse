---
description: 'O engine de banco de dados DataLakeCatalog permite conectar o ClickHouse a catálogos de dados externos e consultar dados em formato de tabela aberto'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

O engine de banco de dados `DataLakeCatalog` permite conectar o ClickHouse a catálogos de dados externos
e consultar dados em formato de tabela aberto sem a necessidade de duplicar dados.
Isso transforma o ClickHouse em um poderoso engine de consulta que funciona perfeitamente com
a infraestrutura do seu lago de dados existente.

<div id="supported-catalogs">
  ## Catálogos compatíveis
</div>

O engine `DataLakeCatalog` oferece suporte aos seguintes catálogos de dados:

* **AWS Glue Catalog** - Para tabelas Iceberg em ambientes AWS
* **Databricks Unity Catalog** - Para tabelas Delta Lake e Iceberg
* **Hive Metastore** - Catálogo tradicional do ecossistema Hadoop
* **REST Catalogs** - Qualquer catálogo compatível com a especificação REST do Iceberg

<div id="creating-a-database">
  ## Criar um banco de dados
</div>

Você precisará habilitar as configurações relevantes abaixo para usar a engine `DataLakeCatalog`:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Bancos de dados com o engine `DataLakeCatalog` podem ser criados com a seguinte sintaxe:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

As configurações a seguir são suportadas:

| Setting                 | Description                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `catalog_type`          | Tipo de catálogo: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg)                                                                                                                                                                                                                                                                                                               |
| `warehouse`             | O nome do warehouse/banco de dados a ser usado no catálogo.                                                                                                                                                                                                                                                                                                                                            |
| `catalog_credential`    | Credencial de autenticação para o catálogo (por exemplo, API key ou token)                                                                                                                                                                                                                                                                                                                             |
| `auth_header`           | HTTP header personalizado para autenticação com o serviço de catálogo                                                                                                                                                                                                                                                                                                                                  |
| `auth_scope`            | Escopo do OAuth2 para autenticação (se estiver usando OAuth)                                                                                                                                                                                                                                                                                                                                           |
| `storage_endpoint`      | URL do endpoint para o armazenamento subjacente                                                                                                                                                                                                                                                                                                                                                        |
| `oauth_server_uri`      | URI do servidor de autorização OAuth2 para autenticação                                                                                                                                                                                                                                                                                                                                                |
| `vended_credentials`    | Valor booleano que indica se devem ser usadas as credenciais fornecidas pelo catálogo (compatível com AWS S3 e Azure ADLS Gen2)                                                                                                                                                                                                                                                                        |
| `aws_access_key_id`     | ID da chave de acesso da AWS para acesso ao S3/Glue (se não estiver usando credenciais fornecidas pelo catálogo)                                                                                                                                                                                                                                                                                       |
| `aws_secret_access_key` | Chave secreta de acesso da AWS para acesso ao S3/Glue (se não estiver usando credenciais fornecidas pelo catálogo)                                                                                                                                                                                                                                                                                     |
| `region`                | Região da AWS para o serviço (por exemplo, `us-east-1`)                                                                                                                                                                                                                                                                                                                                                |
| `dlf_access_key_id`     | ID da chave de acesso para acesso ao DLF                                                                                                                                                                                                                                                                                                                                                               |
| `dlf_access_key_secret` | Chave secreta de acesso para acesso ao DLF                                                                                                                                                                                                                                                                                                                                                             |
| `force_add_bucket`      | Ao construir URLs de armazenamento de objetos a partir da localização da tabela fornecida pelo catálogo e de `storage_endpoint`, adicione o nome do bucket/contêiner no início, mesmo que o endpoint já o contenha. Padrão: `false`. Defina como `true` para catálogos que retornam caminhos sem o bucket e exigem que ele seja adicionado na etapa de construção da URL (caminhos no estilo Polaris). |

<div id="examples">
  ## Exemplos
</div>

Veja nas seções abaixo exemplos de uso do engine `DataLakeCatalog`:

* [Unity Catalog](/pt-BR/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/pt-BR/use-cases/data-lake/glue-catalog)
* Catálogo OneLake
  Pode ser usado com a habilitação de `allow_experimental_database_iceberg` ou `allow_database_iceberg`.

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```