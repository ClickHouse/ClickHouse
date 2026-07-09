---
description: 'Documentação do utilitário cliente do ClickHouse Keeper'
sidebar_label: 'clickhouse-keeper-client'
slug: /operations/utilities/clickhouse-keeper-client
title: 'utilitário clickhouse-keeper-client'
doc_type: 'reference'
---

Um aplicativo cliente para interagir com o clickhouse-keeper por meio de seu protocolo nativo.

<div id="clickhouse-keeper-client">
  ## Opções
</div>

* `-q QUERY`, `--query=QUERY` — consulta a ser executada. Se este parâmetro não for informado, o `clickhouse-keeper-client` será iniciado em modo interativo.
* `-h HOST`, `--host=HOST` — host do servidor. Valor padrão: `localhost`.
* `-p N`, `--port=N` — porta do servidor. Valor padrão: 9181
* `-c FILE_PATH`, `--config-file=FILE_PATH` — define o caminho do arquivo de configuração para obter a connection string. Valor padrão: `config.xml`.
* `--password=PASSWORD` — senha para autenticação. Também pode ser definida pela variável de ambiente `CLICKHOUSE_KEEPER_PASSWORD` ou no arquivo de configuração XML em `<zookeeper><password>`.
* `--identity=IDENTITY` — identidade para o scheme de autenticação `digest`. Também pode ser definida pela variável de ambiente `CLICKHOUSE_KEEPER_IDENTITY` ou no arquivo de configuração XML em `<zookeeper><identity>`.
* `--connection-timeout=TIMEOUT` — define o connection timeout em segundos. Valor padrão: 10s.
* `--session-timeout=TIMEOUT` — define o timeout da sessão em segundos. Valor padrão: 10s.
* `--operation-timeout=TIMEOUT` — define o timeout da operação em segundos. Valor padrão: 10s.
* `--history-file=FILE_PATH` — define o caminho do arquivo de histórico. Valor padrão: `~/.keeper-client-history`.
* `--log-level=LEVEL` — define o nível de log. Valor padrão: `information`.
* `--no-confirmation` — se definido, não exigirá confirmação para vários comandos. Valor padrão: `false` no modo interativo e `true` para consulta
* `--help` — mostra a mensagem de ajuda.

<div id="clickhouse-keeper-client-env">
  ## Variáveis de ambiente
</div>

* `CLICKHOUSE_KEEPER_PASSWORD` — Usada como senha padrão caso `--password` não seja fornecido na linha de comando.
* `CLICKHOUSE_KEEPER_IDENTITY` — Usada como identidade padrão caso `--identity` não seja fornecido na linha de comando.

<div id="clickhouse-keeper-client-auth">
  ## Autenticação
</div>

Ao se conectar a um servidor Keeper que exige autenticação, a senha é resolvida na seguinte ordem de prioridade (a primeira correspondência encontrada prevalece):

1. argumento de linha de comando `--password`
2. variável de ambiente `CLICKHOUSE_KEEPER_PASSWORD`
3. `<zookeeper><password>` no arquivo de configuração XML especificado por `--config-file`

A mesma prioridade se aplica a `--identity` / `CLICKHOUSE_KEEPER_IDENTITY` / `<zookeeper><identity>`.

Exemplo de arquivo de configuração XML com configurações de autenticação:

```xml
<clickhouse>
    <zookeeper>
        <password>secret</password>
        <node index="1">
            <host>localhost</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

<div id="clickhouse-keeper-client-example">
  ## Exemplo
</div>

```bash
./clickhouse-keeper-client -h localhost -p 9181 --connection-timeout 30 --session-timeout 30 --operation-timeout 30
Connected to ZooKeeper at [::1]:9181 with session_id 137
/ :) ls
keeper foo bar
/ :) cd 'keeper'
/keeper :) ls
api_version
/keeper :) cd 'api_version'
/keeper/api_version :) ls

/keeper/api_version :) cd 'xyz'
Path /keeper/api_version/xyz does not exist
/keeper/api_version :) cd ../../
/ :) ls
keeper foo bar
/ :) get 'keeper/api_version'
2
```

<div id="clickhouse-keeper-client-commands">
  ## Comandos
</div>

* `ls '[path]' [watch_id]` -- Lista os nós do caminho especificado (padrão: cwd). Opcionalmente define um watch de filhos identificado por `watch_id`
* `cd '[path]'` -- Altera o caminho de trabalho (padrão `.`)
* `cp '<src>' '<dest>'`  -- Copia o nó &#39;src&#39; para o caminho &#39;dest&#39;
* `cpr '<src>' '<dest>'`  -- Copia a subárvore do nó &#39;src&#39; para o caminho &#39;dest&#39;
* `mv '<src>' '<dest>'`  -- Move o nó &#39;src&#39; para o caminho &#39;dest&#39;
* `mvr '<src>' '<dest>'`  -- Move a subárvore do nó &#39;src&#39; para o caminho &#39;dest&#39;
* `exists '<path>' [watch_id]` -- Retorna `1` se o nó existir, `0` caso contrário. Opcionalmente define um watch identificado por `watch_id`
* `set '<path>' <value> [version]` -- Atualiza o valor do nó. Só atualiza se a versão corresponder (padrão: -1)
* `create '<path>' <value> [mode]` -- Cria um novo nó com o valor definido
* `touch '<path>'` -- Cria um novo nó com uma string vazia como valor. Não gera exceção se o nó já existir
* `get '<path>' [watch_id]` -- Retorna o valor do nó. Opcionalmente define um watch de dados identificado por `watch_id`
* `watch <watch_id> [timeout_seconds]` -- Aguarda o evento de watch identificado por `watch_id` e imprime o tipo de evento e o caminho. Se `timeout_seconds` for especificado, retorna um erro após o tempo limite informado
* `rm '<path>' [version]` -- Remove o nó somente se a versão corresponder (padrão: -1)
* `rmr '<path>' [limit]` -- Exclui recursivamente o caminho se o tamanho da subárvore for menor que o limite. Confirmação obrigatória (limite padrão = 100)
* `flwc <command>` -- Executa um comando de quatro letras
* `help` -- Imprime esta mensagem
* `get_direct_children_number '[path]'` -- Obtém o número de nós filhos diretos em um caminho específico
* `get_all_children_number '[path]'` -- Obtém o número total de nós filhos em um caminho específico
* `get_stat '[path]'` -- Retorna as estatísticas do nó (padrão `.`)
* `find_super_nodes <threshold> '[path]'` -- Encontra nós com número de filhos maior que um determinado limite para o caminho especificado (padrão `.`)
* `delete_stale_backups` -- Exclui nós do ClickHouse usados para backups que agora estão inativos
* `find_big_family [path] [n]` -- Retorna os n nós com a maior família na subárvore (caminho padrão = `.` e n = 10)
* `sync '<path>'` -- Sincroniza o nó entre processos e o líder
* `reconfig <add|remove|set> "<arg>" [version]` -- Reconfigura o Keeper cluster. Veja /docs/en/guides/sre/keeper/clickhouse-keeper#reconfiguration