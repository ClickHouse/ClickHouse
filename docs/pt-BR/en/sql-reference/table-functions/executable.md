---
description: 'A função de tabela `executable` cria uma tabela com base na saída de uma função definida pelo usuário (UDF) que você define em um script que envia linhas para o **stdout**.'
keywords: ['udf', 'função definida pelo usuário', 'clickhouse', 'executable', 'tabela', 'função']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'referência'
---

A função de tabela `executable` cria uma tabela com base na saída de uma função definida pelo usuário (UDF) que você define em um script que envia linhas para o **stdout**. O script executável é armazenado no diretório `users_scripts` e pode ler dados de qualquer fonte. Certifique-se de que seu servidor ClickHouse tenha todos os pacotes necessários para executar o script. Por exemplo, se for um script Python, garanta que o servidor tenha os pacotes Python necessários instalados.

Opcionalmente, você pode incluir uma ou mais consultas de entrada que transmitem seus resultados para o **stdin**, para que o script possa lê-los.

:::note
Uma vantagem importante da `executable` função de tabela e do `Executable` motor de tabela em relação às funções UDF comuns é que funções UDF comuns não podem alterar o número de linhas. Por exemplo, se a entrada tiver 100 linhas, o resultado deverá retornar 100 linhas. Ao usar a `executable` função de tabela ou o `Executable` motor de tabela, seu script pode fazer qualquer transformação de dados que você quiser, incluindo agregações complexas.
:::

<div id="syntax">
  ## Sintaxe
</div>

A função de tabela `executable` exige três parâmetros e aceita uma lista opcional de consultas de entrada:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: o nome do arquivo do script, salvo na pasta `user_scripts` (a pasta padrão da configuração `user_scripts_path`)
* `format`: o formato da tabela gerada
* `structure`: o esquema da tabela gerada
* `input_query`: uma consulta opcional (ou uma coleção ou consultas) cujos resultados são passados ao script via **stdin**

:::note
Se você pretende invocar o mesmo script repetidamente com as mesmas consultas de entrada, considere usar o [motor de tabela `Executable`](../../engines/table-engines/special/executable.md).
:::

O script Python a seguir se chama `generate_random.py` e é salvo na pasta `user_scripts`. Ele lê um número `i` e imprime `i` strings aleatórias, cada uma precedida por um número e separada por um caractere de tabulação:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Vamos executar o script para que ele gere 10 strings aleatórias:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

A resposta fica assim:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## Configurações
</div>

* `send_chunk_header` - controla se a contagem de linhas deve ser enviada antes de enviar um fragmento de dados para o processo. O valor padrão é `false`.
* `pool_size` — Tamanho do pool. Se `pool_size` for definido como 0, não haverá restrições de tamanho para o pool. O valor padrão é `16`.
* `max_command_execution_time` — Tempo máximo de execução do comando do script executável para processar um bloco de dados. Especificado em segundos. O valor padrão é 10.
* `command_termination_timeout` — o script executável deve conter o loop principal de leitura e escrita. Após a função de tabela ser destruída, o pipe é fechado, e o executável terá `command_termination_timeout` segundos para encerrar antes que o ClickHouse envie o sinal SIGTERM ao processo filho. Especificado em segundos. O valor padrão é 10.
* `command_read_timeout` - timeout para leitura de dados do stdout do comando em milissegundos. O valor padrão é 10000.
* `command_write_timeout` - timeout para gravação de dados no stdin do comando em milissegundos. O valor padrão é 10000.

<div id="passing-query-results-to-a-script">
  ## Passando resultados de consulta para um script
</div>

Não deixe de conferir o exemplo no motor de tabela `Executable` sobre [como passar resultados de consulta para um script](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). Veja a seguir como executar o mesmo script desse exemplo usando a função de tabela `executable`:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```