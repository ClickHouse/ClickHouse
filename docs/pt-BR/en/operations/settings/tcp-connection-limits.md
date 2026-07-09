---
description: 'Limites de conexões TCP.'
sidebar_label: 'Limites de conexões TCP'
slug: /operations/settings/tcp-connection-limits
title: 'Limites de conexões TCP'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

Uma conexão TCP do ClickHouse (ou seja, uma conexão feita por meio do [cliente de linha de comando](https://clickhouse.com/docs/interfaces/client))
pode ser desconectada automaticamente após um determinado número de consultas ou um certo tempo de duração.
Após a desconexão, não há reconexão automática (a menos que ela seja acionada por outra ação,
como o envio de outra consulta no cliente de linha de comando).

Os limites de conexão são ativados definindo as configurações do servidor
`tcp_close_connection_after_queries_num` (para o limite de consultas)
ou `tcp_close_connection_after_queries_seconds` (para o limite de duração) com um valor maior que 0.
Se ambos os limites estiverem ativados, a conexão será fechada quando qualquer um deles for atingido primeiro.

Ao atingir um limite e ser desconectado, o cliente recebe uma
exceção `TCP_CONNECTION_LIMIT_REACHED`, e **a consulta que causa a desconexão nunca é processada**.

<div id="query-limits">
  ## Limites de consulta
</div>

Supondo que `tcp_close_connection_after_queries_num` esteja definido como N, a conexão permite
N consultas bem-sucedidas. Então, na consulta N + 1, o cliente se desconecta.

Toda consulta processada conta para o limite de consultas. Portanto, ao conectar um cliente de linha de comando,
pode haver uma consulta automática inicial de avisos do sistema que conta para esse limite.

Quando uma conexão TCP fica ociosa (isto é, não processa consultas por algum período,
especificado pela configuração de sessão `poll_interval`), o número de consultas contabilizadas até então é redefinido para 0.
Isso significa que o número total de consultas em uma única conexão pode exceder
`tcp_close_connection_after_queries_num` se houver ociosidade.

<div id="duration-limits">
  ## Limites de duração
</div>

A duração da conexão é medida a partir do momento em que o cliente se conecta.
O cliente é desconectado na primeira consulta após decorridos `tcp_close_connection_after_queries_seconds` segundos.