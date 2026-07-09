---
description: 'Documentação da cláusula INTO OUTFILE'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'Cláusula INTO OUTFILE'
doc_type: 'reference'
---

A cláusula `INTO OUTFILE` redireciona o resultado de uma consulta `SELECT` para um arquivo no lado do **cliente**.

Há suporte a arquivos comprimidos. O tipo de compressão é detectado pela extensão do nome do arquivo (o modo `'auto'` é usado por padrão). Como alternativa, ele pode ser especificado explicitamente em uma cláusula `COMPRESSION`. O nível de compressão para um determinado tipo de compressão pode ser especificado em uma cláusula `LEVEL`.

**Sintaxe**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` e `type` são literais de string. Os tipos de compressão suportados são: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level` é um literal numérico. Há suporte a inteiros positivos nos seguintes intervalos: `1-12` para o tipo `lz4`, `1-22` para o tipo `zstd` e `1-9` para os demais tipos de compressão.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

* Esta funcionalidade está disponível no [cliente de linha de comando](../../../interfaces/client.md) e no [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Portanto, uma consulta enviada pela [interface HTTP](/pt-BR/interfaces/http) falhará.
* A consulta falhará se já existir um arquivo com o mesmo nome.
* O [formato de saída](../../../interfaces/formats.md) padrão é `TabSeparated` (como no modo em lote do cliente de linha de comando). Use a cláusula [FORMAT](format.md) para alterá-lo.
* Se `AND STDOUT` for mencionado na consulta, a saída gravada no arquivo também será exibida na saída padrão. Se for usado com compressão, o texto em claro será exibido na saída padrão.
* Se `APPEND` for mencionado na consulta, a saída será anexada a um arquivo existente. Se a compressão for usada, `APPEND` não poderá ser usado.
* Ao gravar em um arquivo que já existe, `APPEND` ou `TRUNCATE` deve ser usado.

**Exemplo**

Execute a consulta a seguir usando o [cliente de linha de comando](../../../interfaces/client.md):

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```