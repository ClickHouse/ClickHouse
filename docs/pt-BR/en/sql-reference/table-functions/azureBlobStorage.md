---
description: 'Fornece uma interface semelhante a uma tabela para selecionar e inserir arquivos no Azure Blob
  Storage. Semelhante à função s3.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # Função de tabela azureBlobStorage
</div>

Fornece uma interface semelhante a uma tabela para selecionar/inserir arquivos no [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). Esta função de tabela é semelhante à [função s3](../../sql-reference/table-functions/s3.md).

<div id="syntax">
  ## Sintaxe
</div>

<Tabs>
  <TabItem value="connection_string" label="String de conexão" default>
    As credenciais estão embutidas na string de conexão, portanto `account_name`/`account_key` separados não são necessários:

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="URL da conta de armazenamento">
    Requer `account_name` e `account_key` como argumentos separados:

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="Coleção nomeada">
    Veja [Named Collections](#named-collections) abaixo para a lista completa de chaves compatíveis:

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## Argumentos
</div>

| Argumento                        | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | Uma string de conexão que inclui credenciais embutidas (nome da conta + chave da conta ou SAS token). Ao usar esse formato, `account_name` e `account_key` **não** devem ser passados separadamente. Consulte [Configurar uma string de conexão](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account).                 |
| `storage_account_url`            | A URL de endpoint da conta de armazenamento, por exemplo `https://myaccount.blob.core.windows.net/`. Ao usar esse formato, você **deve** também passar `account_name` e `account_key`.                                                                                                                                                                                                                                                                                                                                    |
| `container_name`                 | Nome do contêiner.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `blobpath`                       | Caminho do arquivo. Suporta os seguintes curingas no modo somente leitura: `*`, `**`, `?`, `{abc,def}` e `{N..M}`, em que `N`, `M` — números, `'abc'`, `'def'` — strings.                                                                                                                                                                                                                                                                                                                                                 |
| `account_name`                   | Nome da conta de armazenamento. **Obrigatório** ao usar `storage_account_url` sem SAS; **não** deve ser passado ao usar `connection_string`.                                                                                                                                                                                                                                                                                                                                                                              |
| `account_key`                    | Chave da conta de armazenamento. **Obrigatória** ao usar `storage_account_url` sem SAS; **não** deve ser passada ao usar `connection_string`.                                                                                                                                                                                                                                                                                                                                                                             |
| `format`                         | O [format](/pt-BR/sql-reference/formats) do arquivo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `compression`                    | Valores compatíveis: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Por padrão, a compressão será detectada automaticamente pela extensão do arquivo (o mesmo que definir `auto`).                                                                                                                                                                                                                                                                                                                                |
| `structure`                      | Estrutura da tabela. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `partition_strategy`             | Opcional. Valores compatíveis: `WILDCARD` ou `HIVE`. `WILDCARD` exige um `{_partition_id}` no caminho, que é substituído pela chave de partição. `HIVE` não permite curingas, assume que o caminho é a raiz da tabela e gera diretórios particionados no estilo Hive, com IDs Snowflake como nomes de arquivo e o formato do arquivo como extensão. O padrão é a configuração `file_like_engine_default_partition_strategy` (`WILDCARD` em configurações de `compatibility` anteriores à `26.6`; `HIVE`, caso contrário). |
| `partition_columns_in_data_file` | Opcional. Usado apenas com a estratégia de partição `HIVE`. Informa ao ClickHouse se deve esperar que as colunas de partição sejam gravadas no arquivo de dados. O padrão é `false`.                                                                                                                                                                                                                                                                                                                                      |
| `extra_credentials`              | Use `client_id` e `tenant_id` para autenticação. Se `extra_credentials` forem fornecidas, elas terão prioridade sobre `account_name` e `account_key`.                                                                                                                                                                                                                                                                                                                                                                     |

<div id="named-collections">
  ## Coleções nomeadas
</div>

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections). Nesse caso, as seguintes chaves são aceitas:

| Key                   | Required | Description                                                                                                          |
| --------------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| `container`           | Sim      | Nome do contêiner. Corresponde ao argumento posicional `container_name`.                                             |
| `blob_path`           | Sim      | Caminho do arquivo (com curingas opcionais). Corresponde ao argumento posicional `blobpath`.                         |
| `connection_string`   | Não*     | String de conexão com credenciais incorporadas. *É necessário fornecer `connection_string` ou `storage_account_url`. |
| `storage_account_url` | Não*     | URL do endpoint da conta de armazenamento. *É necessário fornecer `connection_string` ou `storage_account_url`.      |
| `account_name`        | Não      | Obrigatório ao usar `storage_account_url`                                                                            |
| `account_key`         | Não      | Obrigatório ao usar `storage_account_url`                                                                            |
| `format`              | Não      | Formato do arquivo.                                                                                                  |
| `compression`         | Não      | Tipo de compressão.                                                                                                  |
| `structure`           | Não      | Estrutura da tabela.                                                                                                 |
| `client_id`           | Não      | ID do cliente para autenticação.                                                                                     |
| `tenant_id`           | Não      | ID do tenant para autenticação.                                                                                      |

:::note
Os nomes das chaves da coleção nomeada diferem dos nomes dos argumentos posicionais da função: `container` (não `container_name`) e `blob_path` (não `blobpath`).
:::

**Exemplo:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

Você também pode substituir valores de coleção nomeada em tempo de consulta:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para leitura ou gravação de dados no arquivo especificado.

<div id="examples">
  ## Exemplos
</div>

<div id="reading-with-storage-account-url">
  ### Leitura com `storage_account_url`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### Leitura com `connection_string`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### Escrita com partições
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

Em seguida, leia novamente uma partição específica:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor será `NULL`.
* `_time` — Hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor será `NULL`.

<div id="partitioned-write">
  ## Escrita particionada
</div>

<div id="partition-strategy">
  ### Estratégia de partição
</div>

Compatível apenas com consultas `INSERT`.

`WILDCARD`: substitui o curinga `{_partition_id}` no caminho do arquivo pela chave de partição correspondente. É selecionado por padrão apenas em configurações de `compatibility` anteriores à `26.6`; caso contrário, o padrão é `HIVE` (consulte a configuração `file_like_engine_default_partition_strategy`).

`HIVE` implementa o particionamento no estilo Hive para leituras &amp; gravações. Ele gera arquivos no seguinte formato: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Exemplo de estratégia de partição `HIVE`**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## configuração use_hive_partitioning
</div>

Esta é uma indicação para o ClickHouse interpretar arquivos particionados no estilo Hive durante a leitura. Ela não tem efeito na escrita. Para leituras e escritas simétricas, use o argumento `partition_strategy`.

Quando `use_hive_partitioning` é definido como 1, o ClickHouse detecta o particionamento no estilo Hive no caminho (`/name=value/`) e permite usar colunas de partição como colunas virtuais na consulta. Essas colunas virtuais terão os mesmos nomes do caminho particionado.

**Exemplo**

Use a coluna virtual criada com particionamento no estilo Hive

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Usando Assinaturas de Acesso Compartilhado (SAS)
</div>

Uma Assinatura de Acesso Compartilhado (SAS) é um URI que concede acesso restrito a um contêiner ou arquivo no Azure Storage. Use-a para fornecer acesso temporário a recursos da conta de armazenamento sem compartilhar a chave da conta de armazenamento. Mais detalhes [aqui](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

A função `azureBlobStorage` oferece suporte a Assinaturas de Acesso Compartilhado (SAS).

Um [token SAS de Blob](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) contém todas as informações necessárias para autenticar a solicitação, incluindo o blob de destino, as permissões e o período de validade. Para construir uma URL de blob, acrescente o token SAS ao endpoint do serviço de blob. Por exemplo, se o endpoint for `https://clickhousedocstest.blob.core.windows.net/`, a solicitação passa a ser:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

Como alternativa, os usuários podem usar a [URL SAS do Blob](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) gerada:

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## Relacionados
</div>

* [Motor de Tabela AzureBlobStorage](/pt-BR/engines/table-engines/integrations/azureBlobStorage.md)