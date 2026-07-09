---
description: 'Consulta dados de/para um servidor HTTP/HTTPS remoto. Este motor é semelhante
  ao motor File.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'motor de tabela URL'
doc_type: 'referência'
---

Consulta dados de/para um servidor HTTP/HTTPS remoto. Este motor é semelhante ao motor [File](../../../engines/table-engines/special/file.md).

Sintaxe: `URL(URL [,Format] [,CompressionMethod])`

* O parâmetro `URL` deve estar em conformidade com a estrutura de um Uniform Resource Locator. Para uma URL `http`/`https` (o backend padrão), ele deve apontar para um servidor que use HTTP ou HTTPS, e a obtenção de uma resposta do servidor não deve exigir cabeçalhos adicionais. Já uma URL com um esquema não HTTP reconhecido (`file://`, `s3://`, `az://`, `hdfs://`, …) é delegada ao motor correspondente — veja [Despacho por esquema de URL](#scheme-dispatch) abaixo.

* O `Format` deve ser um formato que o ClickHouse possa usar em consultas `SELECT` e, se necessário, em comandos `INSERT`. Para a lista completa de formatos compatíveis, veja [Formats](/pt-BR/interfaces/formats#formats-overview).

  Se esse argumento não for especificado, o ClickHouse detectará automaticamente o formato pelo sufixo do parâmetro `URL`. Se o sufixo do parâmetro `URL` não corresponder a nenhum dos formatos compatíveis, a criação da tabela falhará. Por exemplo, para a expressão de engine `URL('http://localhost/test.json')`, o formato `JSON` é aplicado.

* `CompressionMethod` indica se o body HTTP deve ser comprimido. Se a compressão estiver habilitada, os packets HTTP enviados pelo motor URL conterão o header &#39;Content-Encoding&#39; para indicar qual método de compressão está sendo usado.

Para habilitar a compressão, primeiro certifique-se de que o endpoint HTTP remoto indicado pelo parâmetro `URL` oferece suporte ao algoritmo de compressão correspondente.

O `CompressionMethod` compatível deve ser um dos seguintes:

* gzip ou gz
* deflate
* brotli ou br
* lzma ou xz
* zstd ou zst
* lz4
* bz2
* snappy
* none
* auto

Se `CompressionMethod` não for especificado, o padrão será `auto`. Isso significa que o ClickHouse detecta automaticamente o método de compressão pelo sufixo do parâmetro `URL`. Se o sufixo corresponder a qualquer um dos métodos de compressão listados acima, a compressão correspondente será aplicada; caso contrário, nenhuma compressão será habilitada.

Por exemplo, para a expressão de engine `URL('http://localhost/test.gzip')`, o método de compressão `gzip` é aplicado, mas para `URL('http://localhost/test.fr')`, nenhuma compressão é habilitada porque o sufixo `fr` não corresponde a nenhum dos métodos de compressão acima.

<div id="scheme-dispatch">
  ## Encaminhamento por esquema de URL
</div>

O motor `URL` é um wrapper unificado sobre os outros motores de arquivo e de armazenamento de objetos: ele encaminha para o backend correto com base no esquema da URL. `http`/`https` (e qualquer esquema não reconhecido) são atendidos pelo próprio motor `URL`; `file://` é atendido pelo motor [File](../../../engines/table-engines/special/file.md); `s3://`, `gs://`, `gcs://`, `oss://` pelo motor [S3](/pt-BR/engines/table-engines/integrations/s3); `az://`, `azure://`, `abfss://`, `abfs://` pelo motor [AzureBlobStorage](/pt-BR/engines/table-engines/integrations/azureBlobStorage); e `hdfs://` pelo motor [HDFS](/pt-BR/engines/table-engines/integrations/hdfs).

Só são encaminhados os esquemas S3 que o mapeador de URI do S3 resolve para um endpoint concreto sem configuração adicional (`s3`, além de `gs`/`gcs`/`oss`). Outros esquemas de provedores compatíveis com S3 (`cos`, `obs`, `eos`, …) são específicos de região e não têm mapeamento de endpoint padrão; por isso, passar essa URL para o motor `URL` faz com que ela seja tratada como um esquema não reconhecido e reportada como um erro. Para esses backends, use diretamente o motor [S3](/pt-BR/engines/table-engines/integrations/s3) (com `url_scheme_mappers` configurado).

A configuração [url&#95;base](/pt-BR/operations/settings/settings.md#url_base) é aplicada antes do encaminhamento por esquema, portanto uma referência relativa é primeiro resolvida em relação à base e depois encaminhada para o motor correspondente.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## Uso
</div>

As consultas `INSERT` e `SELECT` são transformadas em requisições `POST` e `GET`,
respectivamente. Para processar requisições `POST`, o servidor remoto precisa oferecer suporte à
[Chunked transfer encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

Você pode limitar o número máximo de redirecionamentos HTTP GET usando a configuração [max&#95;http&#95;get&#95;redirects](/pt-BR/operations/settings/settings#max_http_get_redirects).

<div id="wildcards-with-http-index-pages">
  ## Caracteres curinga com páginas de índice HTTP
</div>

Quando [allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/pt-BR/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) está habilitada, o motor de tabela `URL` pode expandir caracteres curinga ao buscar páginas de índice HTTP e extrair links delas.
Esse é o mesmo mecanismo da função de tabela [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages).

A expansão é limitada por [max&#95;http&#95;index&#95;page&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md#max_http_index_page_size) para cada página de índice buscada e por [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/pt-BR/operations/settings/settings.md#url_wildcard_max_directories_to_read) para o percurso recursivo de diretórios.

<div id="example">
  ## Exemplo
</div>

**1.** Crie uma tabela `url_engine_table` no servidor:

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Crie um servidor HTTP básico usando as ferramentas padrão do Python 3 e
inicie-o:

```python3
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

```bash
$ python3 server.py
```

**3.** Solicite dados:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## Detalhes da implementação
</div>

* Leituras e gravações podem ocorrer em paralelo
* Não há suporte a:
  * operações `ALTER` e `SELECT...SAMPLE`.
  * índices.
  * replicação.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho para a `URL`. Tipo: `LowCardinality(String)`.
* `_file` — Nome do recurso da `URL`. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do recurso em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho for desconhecido, o valor é `NULL`.
* `_time` — Hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor é `NULL`.
* `_headers` - Cabeçalhos da resposta HTTP. Tipo: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="resolving-relative-urls">
  ## Resolução de URLs relativas
</div>

A configuração [url&#95;base](/pt-BR/operations/settings/settings.md#url_base) permite usar uma URL relativa no motor `URL`. Quando `url_base` está definido, a URL passada para o motor é resolvida com base nele, de acordo com a [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). Para uma descrição completa das regras de resolução, consulte a [documentação da função de tabela url](../../../sql-reference/table-functions/url.md#resolving-relative-urls).

**Exemplo**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## Configurações de armazenamento
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/pt-BR/operations/settings/settings.md#engine_url_skip_empty_files) - permite ignorar arquivos vazios durante a leitura. Desativado por padrão.
* [enable&#95;url&#95;encoding](/pt-BR/operations/settings/settings.md#enable_url_encoding) - permite ativar/desativar a decodificação/codificação do caminho na URI. Ativado por padrão.
* [url&#95;base](/pt-BR/operations/settings/settings.md#url_base) - URL base para resolver URLs relativas passadas ao motor.