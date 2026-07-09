---
description: 'Documentação sobre Funções Definidas pelo Usuário (UDFs)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'Funções Definidas pelo Usuário (UDFs)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # UDFs Funções definidas pelo usuário
</div>

O ClickHouse oferece suporte a vários tipos de funções definidas pelo usuário (UDFs):

* [UDFs executáveis](#executable-user-defined-functions) iniciam um programa externo ou script (Python, Bash etc.) e transmitem blocos de dados para ele por STDIN / STDOUT. Use-as para integrar código ou ferramentas existentes sem recompilar o ClickHouse. Elas têm maior sobrecarga por chamada em comparação com opções no processo e são mais indicadas para lógicas mais pesadas ou quando é necessário um ambiente de execução diferente.
* [UDFs SQL](#sql-user-defined-functions) são definidas com `CREATE FUNCTION` puramente em SQL. Elas são embutidas/expandidas no plano de consulta (sem fronteira de processo), o que as torna leves e ideais para reutilizar lógica de expressão ou simplificar colunas calculadas complexas.
* [UDFs WebAssembly experimentais](#webassembly-user-defined-functions) executam código compilado em WebAssembly dentro de um sandbox no processo do servidor. Elas oferecem menor sobrecarga por chamada do que executáveis externos, com melhor isolamento do que extensões nativas, o que as torna adequadas para algoritmos personalizados escritos em linguagens que podem ter WASM como destino (por exemplo, C/C++/Rust).
* [UDFs executáveis baseadas em driver experimentais](#driver-based-executable-user-defined-functions) permitem que um &quot;driver&quot; fornecido pelo operador transforme um trecho de código fornecido em `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'` em uma UDF executável no momento da criação da função (por exemplo, compilando-o). Elas se baseiam em UDFs executáveis e exigem configuração de driver no lado do servidor.

<div id="executable-user-defined-functions">
  ## Funções Definidas pelo Usuário Executáveis
</div>

<BetaBadge />

:::note
No ClickHouse Cloud, as UDFs executáveis estão em beta público e são criadas pela interface do Cloud Console. Consulte [Funções definidas pelo usuário no Cloud](/pt-BR/cloud/features/user-defined-functions) para ver o fluxo de trabalho específico do Cloud.
:::

O ClickHouse pode chamar qualquer programa executável externo ou script para processar dados.

A configuração das funções definidas pelo usuário executáveis pode ser localizada em um ou mais arquivos XML.
O caminho para a configuração é especificado no parâmetro [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config).

A configuração de uma função contém as seguintes definições:

| Parâmetro                     | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                          | Obrigatório | Valor padrão              |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ------------------------- |
| `name`                        | Nome da função                                                                                                                                                                                                                                                                                                                                                                                                                                     | Sim         | -                         |
| `command`                     | Nome do script a ser executado ou comando, se `execute_direct` for false                                                                                                                                                                                                                                                                                                                                                                           | Sim         | -                         |
| `argument`                    | Descrição do argumento com o `type` e, opcionalmente, o `name` de um argumento. Cada argumento é descrito em uma configuração separada. Especificar o nome é necessário se os nomes dos argumentos fizerem parte da serialização do formato de função definida pelo usuário, como [Native](/pt-BR/interfaces/formats/Native) ou [JSONEachRow](/pt-BR/interfaces/formats/JSONEachRow)                                                                           | Sim         | `c` + argument&#95;number |
| `format`                      | Um [format](../../interfaces/formats.md) no qual os argumentos são passados para o comando. Espera-se que a saída do comando também use o mesmo formato                                                                                                                                                                                                                                                                                            | Sim         | -                         |
| `return_type`                 | O tipo do valor retornado                                                                                                                                                                                                                                                                                                                                                                                                                          | Sim         | -                         |
| `return_name`                 | Nome do valor retornado. Especificar o nome de retorno é necessário se ele fizer parte da serialização do formato de função definida pelo usuário, como [Native](/pt-BR/interfaces/formats/Native) ou [JSONEachRow](/pt-BR/interfaces/formats/JSONEachRow)                                                                                                                                                                                                     | Opcional    | `result`                  |
| `type`                        | Um tipo executável. Se `type` estiver definido como `executable`, um único comando será iniciado. Se estiver definido como `executable_pool`, um pool de comandos será criado                                                                                                                                                                                                                                                                      | Sim         | -                         |
| `max_command_execution_time`  | Tempo máximo de execução, em segundos, para processar um bloco de dados. Essa configuração é válida apenas para comandos `executable_pool`                                                                                                                                                                                                                                                                                                         | Opcional    | `10`                      |
| `command_termination_timeout` | Tempo, em segundos, durante o qual um comando deve ser encerrado após seu pipe ser fechado. Depois desse período, `SIGTERM` é enviado ao processo que executa o comando                                                                                                                                                                                                                                                                            | Opcional    | `10`                      |
| `command_read_timeout`        | Timeout para leitura de dados do stdout do comando, em milissegundos                                                                                                                                                                                                                                                                                                                                                                               | Opcional    | `10000`                   |
| `command_write_timeout`       | Timeout para gravação de dados no stdin do comando, em milissegundos                                                                                                                                                                                                                                                                                                                                                                               | Opcional    | `10000`                   |
| `pool_size`                   | Tamanho do pool de comandos                                                                                                                                                                                                                                                                                                                                                                                                                        | Opcional    | `16`                      |
| `send_chunk_header`           | Controla se a contagem de linhas deve ser enviada antes de enviar um fragmento de dados para o processo                                                                                                                                                                                                                                                                                                                                            | Opcional    | `false`                   |
| `execute_direct`              | Se `execute_direct` = `1`, `command` será procurado na pasta user&#95;scripts especificada por [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Argumentos adicionais do script podem ser especificados usando espaço em branco como separador. Exemplo: `script_name arg1 arg2`. Se `execute_direct` = `0`, `command` é passado como argumento para `bin/sh -c`                       | Opcional    | `1`                       |
| `lifetime`                    | O intervalo de recarga de uma função, em segundos. Se estiver definido como `0`, a função não será recarregada                                                                                                                                                                                                                                                                                                                                     | Opcional    | `0`                       |
| `deterministic`               | Indica se a função é determinística (retorna o mesmo resultado para a mesma entrada)                                                                                                                                                                                                                                                                                                                                                               | Opcional    | `false`                   |
| `stderr_reaction`             | Como tratar a saída stderr do comando. Valores: `none` (ignorar), `log` (registrar todo o stderr imediatamente), `log_first` (registrar os primeiros 4 KiB após a saída), `log_last` (registrar os últimos 4 KiB após a saída), `throw` (lançar uma exceção imediatamente em caso de qualquer saída em stderr). Ao usar `log_first` ou `log_last` com um código de saída diferente de zero, o conteúdo de stderr é incluído na mensagem de exceção | Opcional    | `log_last`                |
| `check_exit_code`             | Se true, o ClickHouse verificará o código de saída do comando. Um código de saída diferente de zero gera uma exceção                                                                                                                                                                                                                                                                                                                               | Opcional    | `true`                    |

O comando deve ler os argumentos de `STDIN` e enviar o resultado para `STDOUT`. O comando deve processar os argumentos de forma iterativa. Ou seja, após processar um fragmento de argumentos, ele deve aguardar o próximo fragmento.

## Funções executáveis definidas pelo usuário

<div id="examples">
  ## Exemplos
</div>

<div id="udf-inline">
  ### UDF de script embutido
</div>

Crie `test_function_sum` manualmente, definindo `execute_direct` como `0` usando configuração em XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Arquivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` com as configurações padrão de caminho).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum</name>
            <return_type>UInt64</return_type>
            <argument>
                <type>UInt64</type>
                <name>lhs</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>rhs</name>
            </argument>
            <format>TabSeparated</format>
            <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
            <execute_direct>0</execute_direct>
            <deterministic>true</deterministic>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Arquivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` com as configurações padrão de caminho).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum
      return_type: UInt64
      argument:
        - type: UInt64
          name: lhs
        - type: UInt64
          name: rhs
      format: TabSeparated
      command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
      execute_direct: 0
      deterministic: true
    ```
  </TabItem>
</Tabs>

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### UDF a partir de script Python
</div>

Neste exemplo, criamos uma UDF que lê um valor de `STDIN` e o retorna como string.

Crie `test_function` usando uma configuração XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Arquivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` com o caminho padrão configurado).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_function.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Arquivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` com o caminho padrão configurado).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_python
      return_type: String
      argument:
        - type: UInt64
          name: value
      format: TabSeparated
      command: test_function.py
    ```
  </TabItem>
</Tabs>

<br />

Crie um arquivo de script `test_function.py` dentro da pasta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function.py` com o caminho padrão configurado).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

<div id="udf-stdin">
  ### Leia dois valores de `STDIN` e retorne a soma deles como um objeto JSON
</div>

Crie `test_function_sum_json` com argumentos nomeados e formato [JSONEachRow](/pt-BR/interfaces/formats/JSONEachRow) usando uma configuração XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Arquivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` com o caminho padrão configurado).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum_json</name>
            <return_type>UInt64</return_type>
            <return_name>result_name</return_name>
            <argument>
                <type>UInt64</type>
                <name>argument_1</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>argument_2</name>
            </argument>
            <format>JSONEachRow</format>
            <command>test_function_sum_json.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Arquivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` com o caminho padrão configurado).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum_json
      return_type: UInt64
      return_name: result_name
      argument:
        - type: UInt64
          name: argument_1
        - type: UInt64
          name: argument_2
      format: JSONEachRow
      command: test_function_sum_json.py
    ```
  </TabItem>
</Tabs>

<br />

Crie o arquivo de script `test_function_sum_json.py` na pasta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` com o caminho padrão configurado).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

<div id="udf-parameters-in-command">
  ### Use parâmetros na configuração `command`
</div>

Funções executáveis definidas pelo usuário podem receber parâmetros constantes configurados na configuração `command` (isso funciona apenas para funções definidas pelo usuário do tipo `executable`).
Também é necessário usar a opção `execute_direct` para evitar vulnerabilidades de expansão de argumentos pelo shell.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Arquivo `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` com a configuração de caminho padrão).

    ```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
    <functions>
        <function>
            <type>executable</type>
            <execute_direct>true</execute_direct>
            <name>test_function_parameter_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
            </argument>
            <format>TabSeparated</format>
            <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Arquivo `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` com a configuração de caminho padrão).

    ```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
    functions:
      type: executable
      execute_direct: true
      name: test_function_parameter_python
      return_type: String
      argument:
        - type: UInt64
      format: TabSeparated
      command: test_function_parameter_python.py {test_parameter:UInt64}
    ```
  </TabItem>
</Tabs>

<br />

Crie o arquivo de script `test_function_parameter_python.py` dentro da pasta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` com a configuração de caminho padrão).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

<div id="udf-shell-script">
  ### UDF a partir de script de shell
</div>

Neste exemplo, criamos um script de shell que multiplica cada valor por 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Arquivo `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` com o caminho padrão).

    ```xml title="/etc/clickhouse-server/test_function_shell.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_shell</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt8</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_shell.sh</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Arquivo `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` com o caminho padrão).

    ```yml title="/etc/clickhouse-server/test_function_shell.yaml"
    functions:
      type: executable
      name: test_shell
      return_type: String
      argument:
        - type: UInt8
          name: value
      format: TabSeparated
      command: test_shell.sh
    ```
  </TabItem>
</Tabs>

<br />

Crie o arquivo de script `test_shell.sh` dentro da pasta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_shell.sh` com o caminho padrão).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```

<div id="error-handling">
  ## Tratamento de erros
</div>

Algumas funções podem lançar uma exceção se os dados forem inválidos.
Nesse caso, a consulta é cancelada e uma mensagem de erro é retornada ao cliente.
No processamento distribuído, quando ocorre uma exceção em um dos servidores, os outros servidores também tentam interromper a consulta.

<div id="evaluation-of-argument-expressions">
  ## Avaliação de expressões de argumentos
</div>

Em quase todas as linguagens de programação, para determinados operadores, um dos argumentos pode não ser avaliado.
Normalmente, esses operadores são `&&`, `||` e `?:`.
No ClickHouse, os argumentos de funções (operadores) são sempre avaliados.
Isso ocorre porque partes inteiras das colunas são avaliadas de uma só vez, em vez de cada linha ser calculada separadamente.

<div id="performing-functions-for-distributed-query-processing">
  ## Execução de funções no processamento distribuído de consultas
</div>

No processamento distribuído de consultas, o maior número possível de etapas do processamento da consulta é executado em servidores remotos, e as etapas restantes (a mesclagem dos resultados intermediários e tudo o que vem depois) são executadas no servidor solicitante.

Isso significa que as funções podem ser executadas em servidores diferentes.
Por exemplo, na consulta `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

* se uma `distributed_table` tiver pelo menos dois shards, as funções &#39;g&#39; e &#39;h&#39; serão executadas em servidores remotos, e a função &#39;f&#39; será executada no servidor solicitante.
* se uma `distributed_table` tiver apenas um shard, todas as funções &#39;f&#39;, &#39;g&#39; e &#39;h&#39; serão executadas no servidor desse shard.

O resultado de uma função normalmente não depende do servidor em que ela é executada. No entanto, às vezes isso é importante.
Por exemplo, funções que usam dicionários utilizam o dicionário existente no servidor em que estão sendo executadas.
Outro exemplo é a função `hostName`, que retorna o nome do servidor em que está sendo executada, para permitir o `GROUP BY` por servidores em uma consulta `SELECT`.

Se uma função em uma consulta for executada no servidor solicitante, mas você precisar executá-la em servidores remotos, poderá envolvê-la em uma função de agregação &#39;any&#39; ou adicioná-la a uma chave em `GROUP BY`.

<div id="sql-user-defined-functions">
  ## Funções definidas pelo usuário em SQL
</div>

Funções personalizadas com base em expressões lambda podem ser criadas usando a instrução [CREATE FUNCTION](../statements/create/function.md). Para excluir essas funções, use a instrução [DROP FUNCTION](../statements/drop.md#drop-function).

<div id="webassembly-user-defined-functions">
  ## Funções Definidas pelo Usuário em WebAssembly
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

As Funções Definidas pelo Usuário em WebAssembly (WASM UDFs) permitem executar código personalizado compilado em WebAssembly no processo do servidor ClickHouse.

<div id="quick-start">
  ### Quick Start
</div>

Ative o suporte experimental a WebAssembly na configuração do ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

Insira seu módulo WASM compilado na tabela do sistema:

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Crie uma função usando seu módulo WASM:

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

Use a função nas consultas:

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### Mais informações
</div>

Consulte a documentação sobre [Funções Definidas pelo Usuário em WebAssembly](wasm_udf.md) para mais informações.

<div id="driver-based-executable-user-defined-functions">
  ## Funções executáveis definidas pelo usuário baseadas em driver
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
Este é um recurso experimental que pode mudar de maneiras incompatíveis com versões anteriores em lançamentos futuros. Ative-o com a configuração no servidor [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers).
:::

Um *driver* é um adaptador fornecido pelo operador que transforma um trecho de código do usuário em uma [UDF executável](#executable-user-defined-functions). Quando uma função é criada com `ENGINE = DriverName(...)`, o ClickHouse executa o `create_command` do driver, passando a assinatura da função e o corpo do código; o driver compila ou processa esse corpo de outra forma e gera uma configuração de UDF executável, que o ClickHouse então armazena e carrega.

Isso permite que administradores ofereçam aos usuários uma forma segura e restrita de definir funções em qualquer linguagem (por exemplo, C compilado dentro de um contêiner em sandbox) sem dar a eles acesso aos arquivos de configuração nem ao sistema de arquivos do servidor. O conjunto de drivers disponíveis é inteiramente controlado pelo operador.

<div id="enabling-drivers">
  ### Habilitando drivers
</div>

As UDFs executáveis baseadas em driver vêm desativadas por padrão. Para habilitá-las:

1. Defina a chave experimental na configuração do servidor:

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. Aponte [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) para um ou mais arquivos de configuração de driver (com suporte a `glob`) e, opcionalmente, defina [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), o diretório onde as configurações geradas das UDFs executáveis são armazenadas:

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

O registro de drivers é carregado na inicialização do servidor e atualizado com `SYSTEM RELOAD CONFIG`, portanto os drivers podem ser adicionados, alterados ou removidos sem reiniciar o servidor.

<div id="driver-configuration">
  ### Configuração do driver
</div>

Um driver é descrito por um arquivo XML (ou YAML) com um elemento `<driver>` no nível superior. Os campos a seguir são compatíveis:

| Campo              | Descrição                                                                                                                                                                           | Obrigatório |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`             | O nome do driver, conforme usado em `CREATE FUNCTION ... ENGINE = <name>(...)`.                                                                                                     | Sim         |
| `create_command`   | Caminho para o programa invocado para criar uma UDF a partir de um trecho de código. Caminhos relativos são resolvidos em relação ao arquivo de configuração do driver.             | Sim         |
| `drop_command`     | Caminho para o programa invocado quando uma função baseada neste driver é removida.                                                                                                 | Não         |
| `engine_arguments` | Declara os argumentos permitidos em `ENGINE = DriverName(...)`. Cada elemento filho é um nome de argumento; um elemento filho `<required>true</required>` o marca como obrigatório. | Não         |
| `env`              | Variáveis de ambiente exportadas ao invocar os comandos do driver.                                                                                                                  | Não         |

Exemplo de configuração do driver:

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### Contrato de invocação do driver
</div>

Quando `CREATE FUNCTION` é executado, `create_command` é invocado com as variáveis `env` configuradas e os seguintes argumentos:

* `--name <function_name>`
* `--return <return_type>` (se houver uma cláusula `RETURNS`)
* `--args <signature>` (se houver uma cláusula `ARGUMENTS`), em que a assinatura é a lista de argumentos declarada, por exemplo `x UInt8, y DateTime`
* `--<key> <value>` para cada argumento de engine declarado fornecido em `ENGINE = DriverName(key = value)`

O corpo do código do usuário (o texto após `AS`) é enviado para a entrada padrão do comando. O comando deve imprimir a configuração de uma UDF executável na saída padrão. O formato é detectado automaticamente: a saída que começa com `<` é tratada como XML; caso contrário, como YAML. O nome da função definido na configuração gerada deve corresponder ao nome que está sendo criado. Se `create_command` terminar com um status diferente de zero, a instrução falhará com uma exceção que inclui o código de saída e a saída de erro padrão do driver.

`drop_command`, quando presente, é invocado da mesma forma (sem um corpo de código no stdin) quando a função é removida.

<div id="creating-a-function-with-a-driver">
  ### Criando uma função
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

O ClickHouse executa o `create_command` do driver, grava a configuração gerada em [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), e o carregador existente de UDFs executáveis a identifica. A função pode então ser chamada como qualquer outra função.

<div id="dropping-a-function-with-a-driver">
  ### Removendo uma função
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

`DROP FUNCTION` invoca o `drop_command` do driver (se presente), remove a configuração dinâmica gerada e o diretório de trabalho de cada função, recarrega o carregador de UDFs executáveis e remove a consulta persistida.

<div id="driver-persistence-and-restart">
  ### Persistência e reinicialização
</div>

A consulta original é persistida como uma instrução `ATTACH FUNCTION ...` no diretório de objetos SQL definidos pelo usuário, para que a função sobreviva à reinicialização do servidor. Na inicialização, as configurações geradas em [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) são carregadas diretamente, sem executar o driver novamente. Se uma instrução `ATTACH FUNCTION` persistida não tiver uma configuração gerada correspondente (por exemplo, se o diretório dinâmico tiver sido perdido), o driver será executado novamente para recriá-la.

<div id="driver-limitations">
  ### Limitações
</div>

* A funcionalidade é experimental e fica condicionada a `allow_experimental_executable_udf_drivers`.
* Funções baseadas em driver não são compatíveis com armazenamento replicado de função definida pelo usuário (`ON CLUSTER` e `<user_defined_zookeeper_path>`), porque apenas a consulta de origem é replicada, não os artefatos gerados.
* O `RESTORE` de uma função baseada em driver incluída em backup preserva a consulta, mas não executa o driver novamente; a configuração gerada é materializada posteriormente pela recuperação após reinicialização.

<div id="example-c-drivers">
  ### Exemplo de drivers em C
</div>

A árvore de código-fonte inclui drivers de prova de conceito em `programs/server/user_defined_executable_function_drivers_config.d/` que compilam e executam um corpo de função em C. Eles são exemplos e **não vêm instalados nos pacotes**:

* `DockerC` - compila e executa o código dentro de contêineres Docker em sandbox (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`, além de limites de memória/CPU/PID), gerando uma UDF `executable_pool`.
* `GVisorC` - uma variante que executa o binary compilado no runtime `runsc` do [gVisor](https://gvisor.dev/).
* `UnsafeC` - compila e executa o código diretamente no host, sem sandbox. Como o nome indica, não oferece isolamento e se destina apenas a ambientes confiáveis e testes.

Esses drivers de exemplo servem como ponto de partida; revise e reforce o sandboxing no seu ambiente antes de expô-los a usuários não confiáveis.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* [Funções definidas pelo usuário no ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)