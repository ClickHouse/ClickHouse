---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: 'Fonte de dicionário Arquivo executável'
sidebar_position: 3
sidebar_label: 'Arquivo executável'
description: 'Configure um arquivo executável como uma fonte de dicionário no ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

O uso de arquivos executáveis depende de [como o dicionário é armazenado na memória](../layouts/). Se o dicionário for armazenado usando `cache` e `complex_key_cache`, o ClickHouse solicita as chaves necessárias enviando uma requisição para o STDIN do arquivo executável. Caso contrário, o ClickHouse inicia o arquivo executável e trata sua saída como dados do dicionário.

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

Campos de configuração:

| Configuração                  | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `command`                     | O caminho absoluto para o arquivo executável ou o nome do arquivo (se o diretório do comando estiver no `PATH`).                                                                                                                                                                                                                                                                                                                                             |
| `format`                      | O formato do arquivo. Todos os formatos descritos em [Formats](/pt-BR/sql-reference/formats) são suportados.                                                                                                                                                                                                                                                                                                                                                       |
| `command_termination_timeout` | O script executável deve conter um loop principal de leitura e escrita. Depois que o dicionário for destruído, o pipe é fechado, e o arquivo executável terá `command_termination_timeout` segundos para encerrar antes que o ClickHouse envie um sinal SIGTERM ao processo filho. Especificado em segundos. O valor padrão é `10`. Opcional.                                                                                                                |
| `command_read_timeout`        | Tempo limite para ler dados do stdout do comando, em milissegundos. Valor padrão: `10000`. Opcional.                                                                                                                                                                                                                                                                                                                                                         |
| `command_write_timeout`       | Tempo limite para gravar dados no stdin do comando, em milissegundos. Valor padrão: `10000`. Opcional.                                                                                                                                                                                                                                                                                                                                                       |
| `implicit_key`                | O arquivo de origem executável pode retornar apenas valores, e a correspondência com as chaves solicitadas é determinada implicitamente pela ordem das linhas no resultado. O valor padrão é `false`.                                                                                                                                                                                                                                                        |
| `execute_direct`              | Se `execute_direct` = `1`, `command` será procurado dentro da pasta user&#95;scripts especificada por [user&#95;scripts&#95;path](/pt-BR/operations/server-configuration-parameters/settings#user_scripts_path). Argumentos adicionais do script podem ser especificados usando espaço em branco como separador. Exemplo: `script_name arg1 arg2`. Se `execute_direct` = `0`, `command` é passado como argumento para `bin/sh -c`. O valor padrão é `0`. Opcional. |
| `send_chunk_header`           | Controla se a contagem de linhas deve ser enviada antes de enviar um fragmento de dados para o processo. O valor padrão é `false`. Opcional.                                                                                                                                                                                                                                                                                                                 |

Essa fonte de dicionário pode ser configurada somente via configuração XML. A criação de dicionários com fonte executável via DDL está desabilitada; caso contrário, o usuário do banco de dados poderia executar binários arbitrários no nó do ClickHouse.