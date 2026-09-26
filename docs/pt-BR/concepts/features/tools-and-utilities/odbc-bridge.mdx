---
description: 'Documentação do Odbc Bridge'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

Servidor HTTP simples que funciona como proxy para o driver ODBC. A principal motivação
foram possíveis segfaults ou outras falhas nas implementações de ODBC, que podem
derrubar todo o processo do clickhouse-server.

Esta ferramenta funciona via HTTP, e não por pipes, memória compartilhada ou TCP, porque:

* É mais simples de implementar
* É mais simples de depurar
* O jdbc-bridge pode ser implementado da mesma forma

<div id="usage">
  ## Uso
</div>

`clickhouse-server` usa esta ferramenta na table function odbc e no StorageODBC.
No entanto, ela também pode ser usada como uma ferramenta independente a partir da linha de comando, com os seguintes
parâmetros na URL da requisição POST:

* `connection_string` -- string de conexão ODBC.
* `sample_block` -- descrição das colunas no formato ClickHouse NamesAndTypesList, nome entre backticks,
  tipo como String. Nome e tipo são separados por espaço, e as linhas por
  quebra de linha.
* `max_block_size` -- parâmetro opcional que define o tamanho máximo de um único bloco.
  A consulta é enviada no corpo do POST. A resposta é retornada no formato RowBinary.

<div id="example">
  ## Exemplo:
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```