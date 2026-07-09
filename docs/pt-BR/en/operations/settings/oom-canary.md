---
description: 'Um processo filho de sacrifício que atrai o canário de OOM do Linux antes
  do servidor ClickHouse, dando ao servidor a chance de reduzir a carga e sobreviver.'
sidebar_label: 'canário de OOM'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'canário de OOM'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
O canário de OOM é experimental e vem desativado por padrão. Seu comportamento pode mudar
entre versões do ClickHouse até que a validação em produção seja concluída.
:::

<div id="overview">
  ## Visão geral
</div>

Quando um host ou cgroup de memória esgota a memória, o OOM killer
(out-of-memory) do Linux encerra um processo com `SIGKILL` — geralmente o processo que mais consome memória, que
em um host dedicado é o próprio `clickhouse-server`. Assim, o servidor inteiro é perdido
em vez de ter a chance de se recuperar.

O canário de OOM muda quem morre primeiro. Ele executa um pequeno processo filho
*de sacrifício* que se torna o alvo mais provável do OOM, para que o kernel o mate
em vez do servidor. O servidor então detecta essa morte, confirma que foi um evento
de OOM e reduz a pressão de memória para conseguir sobreviver.

O canário não aumenta nenhum limite de memória e não substitui
limites configurados corretamente (consulte [memory overcommit](/pt-BR/operations/settings/memory-overcommit) e
`max_server_memory_usage`). É uma última linha de defesa que troca uma pequena
quantidade fixa de memória por uma chance de sobreviver a um pico de uso de memória.

<div id="how-it-works">
  ## Como funciona
</div>

O canário é um processo `clickhouse oom-canary` separado. Ele ajusta seu próprio
`oom_score_adj` para o valor máximo (`1000`), para que o kernel o escolha primeiro; depois,
aloca, acessa e aplica `mlock` a `oom_canary_size` bytes (100 MB por padrão), para que
seu conjunto residente seja efetivamente real. Ele é encerrado automaticamente se o servidor for encerrado.

No servidor, uma thread de monitoramento observa o canário (via `pidfd`) e reage quando
ele morre:

* Encerrado por `SIGKILL` **com** evidência de OOM no cgroup → executa a resposta de OOM e, em seguida,
  relança um novo canário.
* Encerrado **sem** evidência de OOM (por exemplo, um `kill -9` manual) ou finalizado
  por uma falha transitória → apenas relança, sem resposta.
* Falha permanente na configuração inicial ou desligamento do servidor → o canário se desativa.

A evidência de OOM vem apenas do contador `oom_kill` em `memory.events.local` do cgroup v2.
Ela é deliberadamente local ao cgroup: contadores hierárquicos ou de todo o host podem
ser incrementados por processos não relacionados e disparariam respostas falsas.

Em um OOM confirmado, a resposta executa estas etapas independentes: registrar uma mensagem
`FATAL`, purgar as arenas do alocador (jemalloc), tentar cancelar todas as
consultas em execução, cancelar todos os merges e mutações e enfileirar um evento em
[`system.crash_log`](/pt-BR/operations/system-tables/crash_log). Os logs do sistema não são
descarregados de forma síncrona, porque forçar E/S sob pressão de memória pode piorar
a situação.

<div id="requirements">
  ## Requisitos
</div>

* **Linux ≥ 5.3.** O monitor controla o canário por meio de `pidfd_open`; em kernels mais antigos,
  o canário se desativa na inicialização. Em plataformas não Linux, isso é um no-op.
* **cgroup v2 com `memory.events.local`** para a resposta a OOM. Sem isso, o
  canário ainda é reiniciado após um `SIGKILL`, mas não consegue confirmar um OOM, então a
  resposta nunca é executada (um aviso é registrado na inicialização).
* **capability `mlock` (opcional).** Bloquear a memória do canário requer
  `CAP_IPC_LOCK` ou um `RLIMIT_MEMLOCK` suficiente; se isso falhar, o canário registra um
  aviso, e sua memória pode ser movida para swap, reduzindo sua eficácia como alvo de OOM.

:::warning memory.oom.group
Se `memory.oom.group` do cgroup v2 estiver habilitado para o cgroup do servidor, o kernel
encerra o cgroup inteiro como uma única unidade em um OOM — o servidor morre junto com o
canário, e a resposta nunca é executada. O canário não consegue proteger o servidor nesse
modo; um aviso é registrado na inicialização.
:::

<div id="configuration">
  ## Configuração
</div>

O canário é controlado pelas [configurações do servidor](/pt-BR/operations/server-configuration-parameters/settings),
definidas como elementos de nível superior da configuração do servidor e aplicadas após a reinicialização.

| Setting                              | Default              | Description                                                                                                                                                                                                                                        |
| ------------------------------------ | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | Habilita o canário de OOM.                                                                                                                                                                                                                         |
| `oom_canary_size`                    | `104857600` (100 MB) | Quantidade de bytes que o canário aloca e acessa. Valores maiores o tornam um alvo de OOM mais provável.                                                                                                                                           |
| `oom_canary_relaunch`                | `true`               | Reinicia o canário após sua finalização (a menos que tenha ocorrido uma falha permanente de Setup ou desligamento), respeitando os limites abaixo.                                                                                                 |
| `oom_canary_max_rapid_relaunches`    | `10`                 | Número máximo de reinicializações *rápidas* consecutivas antes que a reinicialização automática seja desativada, para evitar ciclos excessivos. É redefinido quando um canário permanece em execução por mais de `oom_canary_max_backoff_seconds`. |
| `oom_canary_initial_backoff_seconds` | `1`                  | Atraso inicial entre reinicializações; dobra a cada vez até atingir o máximo.                                                                                                                                                                      |
| `oom_canary_max_backoff_seconds`     | `60`                 | Atraso máximo entre reinicializações.                                                                                                                                                                                                              |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## Observabilidade
</div>

Uma condição de OOM confirmada gera uma linha em
[`system.crash_log`](/pt-BR/operations/system-tables/crash_log) com `signal = 9` e uma
`signal_description` mencionando `canário de OOM`:

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

O ciclo de vida do canário e cada etapa da resposta a OOM também são registrados no log do servidor.