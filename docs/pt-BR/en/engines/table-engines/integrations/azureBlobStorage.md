---
description: 'Este motor oferece integração com o ecossistema do Azure Blob Storage.'
sidebar_label: 'Azure Blob Storage'
sidebar_position: 10
slug: /engines/table-engines/integrations/azureBlobStorage
title: 'Motor de tabela AzureBlobStorage'
doc_type: 'reference'
---

Este motor oferece integração com o ecossistema do [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

<div id="create-table">
  ## CREATE TABLE
</div>

```sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
    [PARTITION BY expr]
    [SETTINGS ...]
```

<div id="engine-parameters">
  ### Parâmetros do mecanismo
</div>

* `endpoint` — URL do endpoint do AzureBlobStorage com contêiner e prefixo. Opcionalmente, pode conter `account_name` se o método de autenticação usado exigir isso. (`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) ou esses parâmetros podem ser fornecidos separadamente usando `storage_account_url`, `account_name` e `container`. Para especificar o prefixo, deve-se usar `endpoint`.
* `endpoint_contains_account_name` - Este sinalizador é usado para especificar se `endpoint` contém `account_name`, já que isso só é necessário para certos métodos de autenticação. (Padrão: true)
* `connection_string|storage_account_url` — `connection_string` inclui o nome e a chave da conta ([Criar string de conexão](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) ou você também pode fornecer aqui a URL da conta de armazenamento, além do nome e da chave da conta como parâmetros separados (consulte os parâmetros `account_name` e `account_key`)
* `container_name` - Nome do contêiner
* `blobpath` - caminho do arquivo. Suporta os seguintes curingas no modo `readonly`: `*`, `**`, `?`, `{abc,def}` e `{N..M}`, em que `N`, `M` — números, `'abc'`, `'def'` — strings.
* `account_name` - se `storage_account_url` for usado, o nome da conta pode ser especificado aqui
* `account_key` - se `storage_account_url` for usado, a chave da conta pode ser especificada aqui
* `format` — O [formato](/pt-BR/interfaces/formats.md) do arquivo.
* `compression` — Valores compatíveis: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Por padrão, a compressão será detectada automaticamente pela extensão do arquivo. (o mesmo que definir como `auto`).
* `partition_strategy` – Opções: `wildcard` ou `hive`. `wildcard` exige um `{_partition_id}` no caminho, que é substituído pela chave de partição. `hive` não permite curingas, assume que o caminho é a raiz da tabela e gera diretórios particionados no estilo Hive com IDs Snowflake como nomes de arquivo e o formato do arquivo como extensão. O padrão é a configuração `file_like_engine_default_partition_strategy` (`wildcard` em configurações de `compatibility` anteriores à `26.6`, `hive` caso contrário).
* `partition_columns_in_data_file` - Usado apenas com a estratégia de partição `hive`. Informa ao ClickHouse se deve esperar que as colunas de partição sejam gravadas no arquivo de dados. O padrão é `false`.
* `extra_credentials` - Use `client_id` e `tenant_id` para autenticação. Se `extra_credentials` forem fornecidas, elas terão prioridade sobre `account_name` e `account_key`.

**Exemplo**

Os usuários podem usar o emulador Azurite para desenvolvimento local do Azure Storage. Mais detalhes [aqui](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Se estiver usando uma instância local do Azurite, talvez seja necessário substituir `http://localhost:10000` por `http://azurite1:10000` nos comandos abaixo, em que presumimos que o Azurite está disponível no host `azurite1`.

```sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV');

INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM test_table;
```

```text
┌─key──┬─data──┐
│  1   │   a   │
│  2   │   b   │
│  3   │   c   │
└──────┴───────┘
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho for desconhecido, o valor é `NULL`.
* `_time` — Data e hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor é `NULL`.

<div id="authentication">
  ## Autenticação
</div>

Atualmente, há 3 maneiras de se autenticar:

* `Managed Identity` - Pode ser usada fornecendo um `endpoint`, `connection_string` ou `storage_account_url`.
* `SAS Token` - Pode ser usado fornecendo um `endpoint`, `connection_string` ou `storage_account_url`. Ele é identificado pela presença de &#39;?&#39; na URL. Consulte [azureBlobStorage](/pt-BR/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) para ver exemplos.
* `Workload Identity` - Pode ser usada fornecendo um `endpoint` ou `storage_account_url`. Se o parâmetro `use_workload_identity` estiver definido na config, a [workload identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications) será usada para autenticação.

<div id="data-cache">
  ### Cache de dados
</div>

O motor de tabela `Azure` oferece suporte ao cache de dados em disco local.
Veja as opções de configuração e o uso do cache do sistema de arquivos nesta [seção](/pt-BR/operations/storing-data.md/#using-local-cache).
O cache é feito com base no caminho e no ETag do objeto de armazenamento, portanto o ClickHouse não lerá uma versão desatualizada do cache.

Para habilitar o cache, use as configurações `filesystem_cache_name = '<name>'` e `enable_filesystem_cache = 1`.

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. adicione a seção a seguir ao arquivo de configuração do ClickHouse:

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. reutilize a configuração do cache (e, portanto, o armazenamento em cache) da seção `storage_configuration` do ClickHouse, [descrita aqui](/pt-BR/operations/storing-data.md/#using-local-cache)

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — Opcional. Na maioria dos casos, você não precisa de uma chave de partição e, quando precisa, em geral ela não deve ser mais granular do que mensal. O particionamento não acelera as consultas (ao contrário da expressão ORDER BY). Nunca use um particionamento granular demais. Não particione seus dados por identificadores ou nomes de clientes (em vez disso, faça do identificador ou nome do cliente a primeira coluna da expressão ORDER BY).

Para particionar por mês, use a expressão `toYYYYMM(date_column)`, em que `date_column` é uma coluna com uma data do tipo [Date](/pt-BR/sql-reference/data-types/date.md). Os nomes das partições aqui têm o formato `"YYYYMM"`.

<div id="partition-strategy">
  #### Estratégia de partição
</div>

`wildcard`: Substitui o curinga `{_partition_id}` no caminho do arquivo pela chave de partição real. Não há suporte para leitura. É selecionada por padrão apenas em configurações de `compatibility` anteriores à `26.6`; caso contrário, o padrão é `hive` (consulte a configuração `file_like_engine_default_partition_strategy`).

`hive` implementa o particionamento no estilo Hive para leituras &amp; gravações. A leitura é implementada com um padrão glob recursivo. A gravação gera arquivos no seguinte formato: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

Nota: Ao usar a estratégia de partição `hive`, a configuração `use_hive_partitioning` não tem efeito.

Exemplo de estratégia de partição `hive`:

```sql
arthur :) create table azure_table (year UInt16, country String, counter UInt8) ENGINE=AzureBlobStorage(account_name='devstoreaccount1', account_key='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='hive_partitioned', format='Parquet', compression='auto', partition_strategy='hive') PARTITION BY (year, country);

arthur :) insert into azure_table values (2020, 'Russia', 1), (2021, 'Brazil', 2);

arthur :) select _path, * from azure_table;

   ┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
1. │ cont/hive_partitioned/year=2020/country=Russia/7351305360873664512.parquet │ 2020 │ Russia  │       1 │
2. │ cont/hive_partitioned/year=2021/country=Brazil/7351305360894636032.parquet │ 2021 │ Brazil  │       2 │
   └────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

<div id="see-also">
  ## Veja também
</div>

[Função de tabela do Azure Blob Storage](/pt-BR/sql-reference/table-functions/azureBlobStorage)