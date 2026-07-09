---
description: 'O ClickHouse permite enviar ao servidor, junto com uma consulta `SELECT`,
  os dados necessários para processá-la. Esses dados são colocados em uma tabela temporária e
  podem ser usados na consulta (por exemplo, em operadores `IN`).'
sidebar_label: 'Dados externos para processamento de consultas'
sidebar_position: 130
slug: /engines/table-engines/special/external-data
title: 'Dados externos para processamento de consultas'
doc_type: 'reference'
---

O ClickHouse permite enviar ao servidor, junto com uma consulta `SELECT`, os dados necessários para processá-la. Esses dados são colocados em uma tabela temporária (consulte a seção &quot;Tabelas temporárias&quot;) e podem ser usados na consulta (por exemplo, em operadores `IN`).

Por exemplo, se você tiver um arquivo de texto com identificadores importantes de usuários, poderá enviá-lo ao servidor junto com uma consulta que filtra com base nessa lista.

Se você precisar executar mais de uma consulta com um grande volume de dados externos, não use este recurso. É melhor carregar os dados no banco de dados com antecedência.

Os dados externos podem ser enviados usando o cliente de linha de comando (no modo não interativo) ou a interface HTTP.

No cliente de linha de comando, você pode especificar uma seção de parâmetros no formato

```bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

Você pode ter várias seções como esta, de acordo com o número de tabelas que estão sendo transmitidas.

**–external** – Marca o início de uma cláusula.
**–file** – Caminho para o arquivo com o dump da tabela, ou -, que se refere a stdin.
Só é possível recuperar uma única tabela a partir de stdin.

Os seguintes parâmetros são opcionais: **–name**– Nome da tabela. Se omitido, &#95;data será usado.
**–format** – Formato dos dados no arquivo. Se omitido, TabSeparated será usado.

Um dos seguintes parâmetros é obrigatório:**–types** – Uma lista de tipos de coluna separados por vírgula. Por exemplo: `UInt64,String`. As colunas serão nomeadas como &#95;1, &#95;2, ...
**–structure**– A estrutura da tabela no formato`UserID UInt64`, `URL String`. Define os nomes e os tipos das colunas.

Os arquivos especificados em &#39;file&#39; serão processados no formato especificado em &#39;format&#39;, usando os tipos de dados especificados em &#39;types&#39; ou &#39;structure&#39;. A tabela será enviada ao servidor e ficará acessível nele como uma tabela temporária com o nome especificado em &#39;name&#39;.

Exemplos:

```bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Ao usar a interface HTTP, os dados externos são enviados no formato multipart/form-data. Cada tabela é transmitida como um arquivo separado. O nome da tabela é obtido a partir do nome do arquivo. Os parâmetros `name_format`, `name_types` e `name_structure` são passados para `query_string`, em que `name` é o nome da tabela à qual esses parâmetros correspondem. O significado desses parâmetros é o mesmo de quando se usa o cliente de linha de comando.

Exemplo:

```bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

No processamento distribuído de consultas, as tabelas temporárias são enviadas a todos os servidores remotos.