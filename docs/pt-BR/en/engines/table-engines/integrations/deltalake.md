---
description: 'Este motor oferece uma integração somente leitura com tabelas Delta Lake
  existentes no Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'Motor de tabela DeltaLake'
doc_type: 'referência'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # motor de tabela DeltaLake
</div>

Este motor oferece integração com tabelas [Delta Lake](https://github.com/delta-io/delta) existentes no S3, GCP e Azure Storage, com suporte a leituras e gravações (a partir da v25.10).

<div id="create-table">
  ## Criar uma tabela DeltaLake
</div>

Para criar uma tabela DeltaLake, ela já deve existir no armazenamento S3, GCP ou Azure. Os comandos abaixo não aceitam parâmetros DDL para criar uma nova tabela.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **Sintaxe**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **Parâmetros do motor**

    * `url` — URL do bucket com o caminho para a tabela Delta Lake existente.
    * `aws_access_key_id`, `aws_secret_access_key` - Credenciais de longo prazo para o usuário da conta [AWS](https://aws.amazon.com/). Você pode usá-las para autenticar suas solicitações. O parâmetro é opcional. Se as credenciais não forem especificadas, elas serão obtidas do arquivo de configuração.
    * `extra_credentials` - Opcional. Usado para passar um `role_arn` para controle de acesso baseado em funções no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

    Os parâmetros do motor podem ser especificados usando [coleções nomeadas](/pt-BR/operations/named-collections.md).

    **Exemplo**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    Usando coleções nomeadas:

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **Sintaxe**

    ```sql
    -- Usando URL HTTPS (recomendado)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[URI do gsutil sem suporte]
    URIs do gsutil, como `gs://clickhouse-docs-example-bucket`, não têm suporte; use uma URL que comece com `https://storage.googleapis.com`
    :::

    **Argumentos**

    * `url` — URL do bucket do GCS para a tabela Delta Lake. Deve usar o formato `https://storage.googleapis.com/<bucket>/<path>/`
      (o endpoint da API XML do GCS), ou `gs://<bucket>/<path>/`, que é convertido automaticamente.
    * `access_key_id` — Chave de acesso do GCS. Crie-a em Google Cloud Console → Cloud Storage → Settings → Interoperability.
    * `secret_access_key` — Chave secreta do GCS.

    **Coleções nomeadas**

    Você também pode usar coleções nomeadas.
    Por exemplo:

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **Sintaxe**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **Argumentos**

    * `connection_string` — string de conexão do Azure
    * `storage_account_url` — URL da conta de armazenamento do Azure (por exemplo, https://account.blob.core.windows.net)
    * `container_name` — nome do contêiner do Azure
    * `blobpath` — caminho para a tabela Delta Lake dentro do contêiner
    * `account_name` — nome da conta de armazenamento do Azure
    * `account_key` — chave da conta de armazenamento do Azure
  </TabItem>
</Tabs>

<div id="insert-data">
  ## Gravar dados usando uma tabela DeltaLake
</div>

Depois de criar uma tabela usando o motor de tabela DeltaLake, você pode inserir dados nela com:

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
A gravação com o motor de tabela é compatível apenas por meio do delta kernel.
Gravações no Azure ainda não são compatíveis, mas funcionam com S3 e GCS.

As gravações no Delta Lake são um recurso Beta e devem ser habilitadas com `SET allow_delta_lake_writes = 1` (disponível a partir da versão 26.7; em versões anteriores, use `SET allow_experimental_delta_lake_writes = 1`).
:::

<div id="data-cache">
  ### Cache de dados
</div>

O motor de tabela `DeltaLake` e a função de tabela oferecem suporte a cache de dados, assim como os armazenamentos `S3`, `AzureBlobStorage` e `HDFS`. Consulte [&quot;motor de tabela S3&quot;](../../../engines/table-engines/integrations/s3.md#data-cache) para mais detalhes.

<div id="see-also">
  ## Veja também
</div>

* [função de tabela DeltaLake](../../../sql-reference/table-functions/deltalake.md)