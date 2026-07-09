---
description: 'Você pode monitorar o uso dos recursos de hardware e também as métricas
  do servidor ClickHouse.'
keywords: ['monitoramento', 'observabilidade', 'dashboard avançado', 'dashboard', 'dashboard
    de observabilidade']
sidebar_label: 'Monitoramento'
sidebar_position: 45
slug: /operations/monitoring
title: 'Monitoramento'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # Monitoramento
</div>

:::note
Os dados de monitoramento descritos neste guia podem ser acessados no ClickHouse Cloud. Além de serem exibidos no dashboard integrado descrito abaixo, as métricas de desempenho básicas e avançadas também podem ser visualizadas diretamente no console principal do serviço.
:::

Você pode monitorar:

* A utilização de recursos de hardware.
* As métricas do servidor ClickHouse.

<div id="built-in-advanced-observability-dashboard">
  ## Dashboard avançado de observabilidade integrado
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="Captura de tela 2023-11-12 às 6 08 58 PM" size="md" />

O ClickHouse vem com um dashboard avançado de observabilidade integrado, que pode ser acessado em `$HOST:$PORT/dashboard` (requer usuário e senha) e exibe as seguintes métricas:

* Consultas por segundo
* Uso de CPU (núcleos)
* Consultas em execução
* Merges em execução
* Bytes selecionados por segundo
* Espera de E/S
* Espera de CPU
* Uso de CPU do SO (userspace)
* Uso de CPU do SO (kernel)
* Leitura de disco
* Leitura do sistema de arquivos
* Memória (monitorada)
* Linhas inseridas por segundo
* Total de partes do MergeTree
* Máximo de partes por partição

<div id="resource-utilization">
  ## Utilização de recursos
</div>

O ClickHouse também monitora, por conta própria, o estado dos recursos de hardware, como:

* Carga e temperatura dos processadores.
* Utilização do sistema de armazenamento, da RAM e da rede.

Esses dados são coletados na tabela `system.asynchronous_metric_log`.

<div id="clickhouse-server-metrics">
  ## Métricas do servidor ClickHouse
</div>

O servidor ClickHouse tem mecanismos embutidos para monitorar seu próprio estado.

Para acompanhar os eventos do servidor, use os logs do servidor. Consulte a seção [logger](../operations/server-configuration-parameters/settings.md#logger) do arquivo de configuração.

O ClickHouse coleta:

* Diferentes métricas sobre como o servidor usa os recursos computacionais.
* Estatísticas gerais sobre o processamento de consultas.

Você pode encontrar as métricas nas tabelas [system.metrics](/pt-BR/operations/system-tables/metrics), [system.events](/pt-BR/operations/system-tables/events) e [system.asynchronous&#95;metrics](/pt-BR/operations/system-tables/asynchronous_metrics).

Você pode configurar o ClickHouse para exportar métricas para o [Graphite](https://github.com/graphite-project). Consulte a [seção Graphite](../operations/server-configuration-parameters/settings.md#graphite) no arquivo de configuração do servidor ClickHouse. Antes de configurar a exportação de métricas, você deve configurar o Graphite seguindo o [guia oficial](https://graphite.readthedocs.io/en/latest/install.html).

Você pode configurar o ClickHouse para exportar métricas para o [Prometheus](https://prometheus.io). Consulte a [seção Prometheus](../operations/server-configuration-parameters/settings.md#prometheus) no arquivo de configuração do servidor ClickHouse. Antes de configurar a exportação de métricas, você deve configurar o Prometheus seguindo o [guia oficial](https://prometheus.io/docs/prometheus/latest/installation/).

Além disso, você pode monitorar a disponibilidade do servidor por meio da API HTTP. Envie a requisição `HTTP GET` para `/ping`. Se o servidor estiver disponível, ele responderá com `200 OK`.

Para monitorar servidores em uma configuração em cluster, você deve definir o parâmetro [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) e usar o recurso HTTP `/replicas_status`. Uma requisição para `/replicas_status` retorna `200 OK` se a réplica estiver disponível e não estiver atrasada em relação às outras réplicas. Se uma réplica estiver atrasada, ela retorna `503 HTTP_SERVICE_UNAVAILABLE` com informações sobre a defasagem.