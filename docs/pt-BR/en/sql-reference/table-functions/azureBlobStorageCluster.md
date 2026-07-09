---
description: 'Permite processar arquivos do Azure Blob Storage em paralelo usando vários
  nós em um cluster especificado.'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

Permite processar arquivos do [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) em paralelo usando vários nós em um cluster especificado. No nó iniciador, cria uma conexão com todos os nós do cluster, expande os asteriscos no caminho de arquivo do S3 e atribui dinamicamente cada arquivo. No nó worker, solicita ao iniciador a próxima tarefa a ser processada e a processa. Isso se repete até que todas as tarefas sejam concluídas.
Esta função de tabela é semelhante à função [s3Cluster](../../sql-reference/table-functions/s3Cluster.md).

<div id="syntax">
  ## Sintaxe
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento           | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `connection_string` | storage&#95;account&#95;url&#96; — connection&#95;string inclui o nome da conta e a chave ([Criar string de conexão](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) ou você também pode fornecer aqui a URL da conta de armazenamento e o nome e a chave da conta como parâmetros separados (consulte os parâmetros account&#95;name e account&#95;key) |
| `container_name`    | Nome do contêiner                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `blobpath`          | caminho do arquivo. Suporta os seguintes curingas no modo readonly: `*`, `**`, `?`, `{abc,def}` e `{N..M}`, em que `N`, `M` — números, `'abc'`, `'def'` — strings.                                                                                                                                                                                                                                                                                                                                                                                                |
| `account_name`      | se storage&#95;account&#95;url for usada, o nome da conta poderá ser especificado aqui                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `account_key`       | se storage&#95;account&#95;url for usada, a chave da conta poderá ser especificada aqui                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `format`            | O [formato](/pt-BR/sql-reference/formats) do arquivo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `compression`       | Valores compatíveis: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Por padrão, a compressão será detectada automaticamente pela extensão do arquivo. (o mesmo que definir como `auto`).                                                                                                                                                                                                                                                                                                                                                                  |
| `structure`         | Estrutura da tabela. Formato `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para leitura ou gravação de dados no arquivo especificado.

<div id="examples">
  ## Exemplos
</div>

Assim como no mecanismo de tabela [AzureBlobStorage](/pt-BR/engines/table-engines/integrations/azureBlobStorage), os usuários podem usar o emulador Azurite para desenvolvimento local com o Azure Storage. Mais detalhes [aqui](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Abaixo, assumimos que o Azurite está disponível no hostname `azurite1`.

Selecione a contagem do arquivo `test_cluster_*.csv` usando todos os nós do cluster `cluster_simple`:

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Usando Assinaturas de Acesso Compartilhado (SAS)
</div>

Consulte [azureBlobStorage](/pt-BR/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) para exemplos.

<div id="related">
  ## Relacionados
</div>

* [motor AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md)
* [função de tabela azureBlobStorage](../../sql-reference/table-functions/azureBlobStorage.md)