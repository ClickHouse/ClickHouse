---
description: 'Documentação do formato RawBLOB'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## Descrição
</div>

Os formatos `RawBLOB` leem todos os dados de entrada em um único valor. Só é possível analisar uma tabela com um único campo do tipo [`String`](/pt-BR/sql-reference/data-types/string.md) ou semelhante.
O resultado é gerado em formato binário, sem delimitadores nem escape de caracteres. Se mais de um valor for gerado, o formato será ambíguo e será impossível ler os dados de volta.

<div id="raw-formats-comparison">
  ### Comparação de formatos raw
</div>

Abaixo está uma comparação entre os formatos `RawBLOB` e [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:

* os dados são gerados em formato binário, sem escape;
* não há delimitadores entre os valores;
* não há quebra de linha ao final de cada valor.

`TabSeparatedRaw`:

* os dados são gerados sem escape;
* as linhas contêm valores separados por tabulações;
* há uma quebra de linha após o último valor de cada linha.

A seguir, uma comparação entre os formatos `RawBLOB` e [RowBinary](./RowBinary/RowBinary.md).

`RawBLOB`:

* Campos do tipo String são gerados sem prefixo de comprimento.

`RowBinary`:

* Campos do tipo String são representados pelo comprimento no formato varint (sem sinal [LEB128] (https://en.wikipedia.org/wiki/LEB128)), seguido pelos bytes da string.

Quando dados vazios são passados para a entrada `RawBLOB`, o ClickHouse lança uma exceção:

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## Exemplo de uso
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## Configurações de formato
</div>
