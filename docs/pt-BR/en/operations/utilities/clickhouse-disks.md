---
description: 'Documentação do Clickhouse-disks'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
doc_type: 'reference'
---

Um utilitário que fornece operações semelhantes às de um sistema de arquivos para os disks do ClickHouse. Ele pode funcionar tanto no modo interativo quanto no modo não interativo.

<div id="program-wide-options">
  ## Opções gerais do programa
</div>

* `--config-file, -C` -- caminho para o arquivo de configuração do ClickHouse; o padrão é `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- Registra o progresso dos comandos executados em `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- Que [tipo](../server-configuration-parameters/settings#logger) de eventos registrar; o padrão é `none`.
* `--disk` -- qual disk usar nos comandos `mkdir, move, read, write, remove`. O padrão é `default`.
* `--query, -q` -- consulta única que pode ser executada sem iniciar o modo interativo
* `--help, -h` -- imprime todas as opções e comandos com descrição

<div id="lazy-initialization">
  ## Inicialização lazy
</div>

Todos os disks disponíveis na configuração são inicializados de forma lazy. Isso significa que o objeto correspondente a um disk é inicializado apenas quando esse disk é usado em algum comando. Isso é feito para tornar o utilitário mais robusto e evitar mexer em disks definidos na configuração, mas não usados pelo usuário, que podem falhar durante a inicialização. No entanto, deve haver um disk que seja inicializado na execução do clickhouse-disks. Esse disk é especificado com o parâmetro `--disk` pela linha de comando (o valor padrão é `default`).

<div id="default-disks">
  ## Discos padrão
</div>

Após a inicialização, há dois disks que não estão especificados na configuração, mas estão disponíveis para inicialização.

1. **`local` Disk**: Este disk foi projetado para reproduzir o sistema de arquivos local a partir do qual o utilitário `clickhouse-disks` foi iniciado. Seu caminho inicial é o diretório a partir do qual o `clickhouse-disks` foi iniciado, e ele é montado no diretório raiz do sistema de arquivos.

2. **`default` Disk**: Este disk é montado no sistema de arquivos local no diretório especificado pelo parâmetro `clickhouse/path` na configuração (o valor padrão é `/var/lib/clickhouse`). Seu caminho inicial é definido como `/.`

<div id="clickhouse-disks-state">
  ## Estado do clickhouse-disks
</div>

Para cada disco adicionado, o utilitário armazena o diretório atual (assim como em um sistema de arquivos comum). O usuário pode alterar o diretório atual e alternar entre os discos.

O estado é refletido em um prompt &quot;`disk_name`:`path_name`&quot;

<div id="commands">
  ## Comandos
</div>

Neste arquivo de documentação, todos os argumentos posicionais obrigatórios são indicados como `<parameter>`, e os argumentos nomeados são indicados como `[--parameter value]`. Todos os parâmetros posicionais também podem ser informados como parâmetros nomeados com o nome correspondente.

* `cd (change-dir, change_dir) [--disk disk] <path>`
  Altera o diretório para o `path` `path` no disk `disk` (o valor padrão é o disk atual). Não ocorre troca de disk.
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  Copia recursivamente os dados de `path-from` no disk `disk_1` (o valor padrão é o disk atual (parâmetro `disk` no modo não interativo))
  para `path-to` no disk `disk_2` (o valor padrão é o disk atual (parâmetro `disk` no modo não interativo)).
* `current_disk_with_path (current, current_disk, current_path)`
  Imprime o estado atual no formato:
  `Disk: "current_disk" Path: "current path on current disk"`
* `du [--human-readable] [<path>]`
  Imprime o tamanho total em bytes do arquivo ou diretório em `path` no disk atual. Para um diretório, o tamanho de todos os arquivos que ele contém é somado recursivamente. Se `path` não for especificado, o diretório atual será usado. Com `--human-readable` (`-h`), o tamanho é impresso em um formato legível (por exemplo, `1.23 GiB`).
* `help [<command>]`
  Imprime a mensagem de ajuda sobre o comando `command`. Se `command` não for especificado, imprime informações sobre todos os comandos.
* `move (mv) <path-from> <path-to>`.
  Move um arquivo ou diretório de `path-from` para `path-to` dentro do disk atual.
* `remove (rm, delete) <path>`.
  Remove `path` recursivamente no disk atual.
* `link (ln) <path-from> <path-to>`.
  Cria um link físico de `path-from` para `path-to` no disk atual.
* `list (ls) [--recursive] <path>`
  Lista os arquivos em `path` no disk atual. Não é recursivo por padrão.
* `list-disks (list_disks, ls-disks, ls_disks)`.
  Lista os nomes dos disks.
* `mkdir [--recursive] <path>` no disk atual.
  Cria um diretório. Não é recursivo por padrão.
* `read (r) <path-from> [--path-to path]`
  Lê um arquivo de `path-from` para `path` (`stdout` se não for fornecido).
* `read-bitmap <path-from> [--values]`
  Inspeciona um sidecar `delete-bitmap` (`.rbm`) em `path-from`. Imprime o magic e a versão, a validade do CRC, a cardinalidade (número de linhas excluídas) e o intervalo de linhas. Com `--values`, também despeja todos os bits definidos (os offsets das linhas excluídas) em ordem crescente.
* `switch-disk [--path path] <disk>`
  Alterna para o disk `disk` no `path` `path` (se `path` não for especificado, o valor padrão será o `path` anterior no disk `disk`).
* `write (w) [--path-from path] <path-to>`.
  Escreve um arquivo de `path` (`stdin` se `path` não for fornecido; a entrada deve terminar com Ctrl+D) para `path-to`.
* `wc <path> [--bytes] [--lines] [--words]`
  Conta bytes, linhas e palavras no arquivo em `path` no disk atual (como o `wc` do Unix). Sem nenhuma flag, as três contagens são impressas na ordem: linhas, palavras e depois bytes. Use `--bytes` (`-c`), `--lines` (`-l`) e `--words` (`-w`) para selecionar contagens específicas.
* `sed <expression> <path>`
  Aplica a `expression` do `sed` ao arquivo em `path` no disk atual, no próprio arquivo. Requer que `sed` esteja instalado no host. Apenas uma única `expression` `sed` sem opções é suportada (por exemplo, `'s/foo/bar/g'`, `'/foo/d'`), não múltiplas `expression` (`-e ... -e ...`) nem opções combinadas com um endereço (por exemplo, `-n` com `4,10p`).
* `read-checksums <path>`
  Lê um arquivo `checksums.txt` de uma data part `MergeTree` em um disk atual e o imprime em `stdout` como uma tabela legível, separada por tabulações, com as colunas `name`, `file_size`, `file_hash`, `uncompressed_size` e `uncompressed_hash`. As duas últimas colunas estão presentes apenas para arquivos comprimidos.