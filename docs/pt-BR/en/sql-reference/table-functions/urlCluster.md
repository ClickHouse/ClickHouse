---
description: 'Permite processar arquivos de URL em paralelo a partir de vários nós em um
  cluster especificado.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

Permite processar arquivos de URL em paralelo a partir de vários nós em um cluster especificado. No iniciador, cria uma conexão com todos os nós do cluster, expande o asterisco no caminho do arquivo da URL e distribui cada arquivo dinamicamente. No nó worker, consulta o iniciador sobre a próxima tarefa a ser processada e a processa. Isso se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento      | Descrição                                                                                                                                                           |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nome de um cluster usado para compor um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                             |
| `URL`          | Endereço do servidor HTTP ou HTTPS que aceita solicitações `GET`. Tipo: [String](../../sql-reference/data-types/string.md).                                         |
| `format`       | [Formato](/pt-BR/sql-reference/formats) dos dados. Tipo: [String](../../sql-reference/data-types/string.md).                                                              |
| `structure`    | Estrutura da tabela no formato `'UserID UInt64, Name String'`. Determina os nomes e os tipos das colunas. Tipo: [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com o formato e a estrutura especificados e com dados da `URL` especificada.

<div id="examples">
  ## Exemplos
</div>

Obtendo as 3 primeiras linhas de uma tabela com colunas dos tipos `String` e [UInt32](../../sql-reference/data-types/int-uint.md) a partir de um servidor HTTP que responde no formato [CSV](/pt-BR/interfaces/formats/CSV).

1. Crie um servidor HTTP básico usando as ferramentas padrão do Python 3 e inicie-o:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## Globs na URL
</div>

Os padrões em `{ }` são usados para gerar um conjunto de shards ou para especificar endereços de failover. Consulte os tipos de padrão compatíveis e os exemplos na descrição da função [remote](remote.md#globs-in-addresses).
O caractere `|` dentro dos padrões é usado para especificar endereços de failover. Eles são percorridos na mesma ordem em que aparecem no padrão. O número de endereços gerados é limitado pela configuração [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).

<div id="related">
  ## Relacionados
</div>

* [motor HDFS](/pt-BR/engines/table-engines/integrations/hdfs)
* [função de tabela URL](/pt-BR/engines/table-engines/special/url)