---
description: 'Permite acessar todos os shards (configurados na seção `remote_servers`)
  de um cluster sem criar uma tabela Distributed.'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'reference'
---

Permite acessar todos os shards (configurados na seção `remote_servers`) de um cluster sem criar uma tabela [Distributed](../../engines/table-engines/special/distributed.md). Apenas uma réplica de cada shard é consultada.

Função `clusterAllReplicas` — igual a `cluster`, mas consulta todas as réplicas. Cada réplica em um cluster é usada como um shard/conexão separado.

:::note
Todos os clusters disponíveis estão listados na tabela [system.clusters](../../operations/system-tables/clusters.md).
:::

<div id="syntax">
  ## Sintaxe
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumentos                  | Tipo                                                                                                                                                               |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name`              | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais; `default` é usado se não for especificado. |
| `db.table` or `db`, `table` | Nome de um banco de dados e de uma tabela.                                                                                                                         |
| `sharding_key`              | Uma chave de sharding. Opcional. Deve ser especificada se o cluster tiver mais de um shard.                                                                        |

<div id="returned_value">
  ## Valor retornado
</div>

O conjunto de dados dos clusters.

<div id="using_macros">
  ## Usando macros
</div>

`cluster_name` pode conter macros — substituições em `{}`. O valor substituído é obtido na seção [macros](../../operations/server-configuration-parameters/settings.md#macros) do arquivo de configuração do servidor.

Exemplo:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## Uso e recomendações
</div>

Usar as funções de tabela `cluster` e `clusterAllReplicas` é menos eficiente do que criar uma tabela `Distributed`, porque, nesse caso, a conexão com o servidor é restabelecida a cada requisição. Ao processar um grande número de consultas, sempre crie a tabela `Distributed` com antecedência e não use as funções de tabela `cluster` e `clusterAllReplicas`.

As funções de tabela `cluster` e `clusterAllReplicas` podem ser úteis nos seguintes casos:

* Acessar um cluster específico para comparação de dados, depuração e testes.
* Consultas a vários clusters e réplicas do ClickHouse para fins de pesquisa.
* Requisições distribuídas pouco frequentes feitas manualmente.

Configurações de conexão como `host`, `port`, `user`, `password`, `compression` e `secure` são obtidas da seção de configuração `<remote_servers>`. Veja os detalhes em [motor Distributed](../../engines/table-engines/special/distributed.md).

<div id="related">
  ## Relacionado
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)