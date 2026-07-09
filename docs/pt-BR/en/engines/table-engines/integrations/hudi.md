---
description: 'Este motor fornece uma integração de somente leitura com tabelas Apache Hudi
  existentes no Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Motor de tabela Hudi'
doc_type: 'referência'
---

Este motor fornece uma integração de somente leitura com tabelas Apache [Hudi](https://hudi.apache.org/) existentes no Amazon S3.

<div id="create-table">
  ## Criar tabela
</div>

Observe que a tabela Hudi já deve existir no S3; este comando não recebe parâmetros DDL para criar uma nova tabela.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Parâmetros do motor**

* `url` — URL do bucket com o caminho para uma tabela Hudi existente.
* `aws_access_key_id`, `aws_secret_access_key` - Credenciais de longo prazo para o usuário da conta [AWS](https://aws.amazon.com/). Você pode usá-las para autenticar suas solicitações. O parâmetro é opcional. Se as credenciais não forem especificadas, elas serão obtidas do arquivo de configuração.
* `extra_credentials` - Opcional. Usado para passar um `role_arn` para acesso baseado em função no ClickHouse Cloud. Consulte [S3 seguro](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

Os parâmetros do motor podem ser especificados usando [Coleções nomeadas](/pt-BR/operations/named-collections.md).

**Exemplo**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Usando coleções nomeadas:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## Veja também
</div>

* [função de tabela Hudi](/pt-BR/sql-reference/table-functions/hudi.md)