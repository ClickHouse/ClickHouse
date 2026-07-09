---
description: 'Fornece uma interface semelhante a uma tabela, somente leitura, para tabelas Apache Hudi no Amazon
  S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

Fornece uma interface semelhante a uma tabela, somente leitura, para tabelas Apache [Hudi](https://hudi.apache.org/) no Amazon S3.

<div id="syntax">
  ## Sintaxe
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                                    | Descrição                                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | URL do bucket com o caminho para uma tabela Hudi existente no S3.                                                                                                                                                                                                                                                                                                                                                  |
| `aws_access_key_id`, `aws_secret_access_key` | Credenciais de longo prazo para o usuário da conta [AWS](https://aws.amazon.com/). Você pode usá-las para autenticar suas requisições. Esses parâmetros são opcionais. Se as credenciais não forem especificadas, elas serão obtidas da configuração do ClickHouse. Para mais informações, consulte [Using S3 for Data Storage](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | O [formato](/pt-BR/interfaces/formats) do arquivo.                                                                                                                                                                                                                                                                                                                                                                      |
| `structure`                                  | Estrutura da tabela. Formato `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                       |
| `compression`                                | O parâmetro é opcional. Valores suportados: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Por padrão, a compressão será detectada automaticamente com base na extensão do arquivo.                                                                                                                                                                                                                       |
| `extra_credentials`                          | O parâmetro é opcional. Usado para passar um `role_arn` para acesso baseado em papéis no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.                                                                                                                                                                                                                |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para ler dados da tabela Hudi especificada no S3.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor é `NULL`.
* `_time` — Horário da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se o horário for desconhecido, o valor é `NULL`.
* `_etag` — O etag do arquivo. Tipo: `LowCardinality(String)`. Se o etag for desconhecido, o valor é `NULL`.

<div id="related">
  ## Relacionados
</div>

* [motor Hudi](/pt-BR/engines/table-engines/integrations/hudi.md)
* [função de tabela de cluster do Hudi](/pt-BR/sql-reference/table-functions/hudiCluster.md)