---
description: 'Tabela do sistema que mostra os watches do ZooKeeper atualmente ativos registrados neste servidor ClickHouse.'
keywords: ['tabela do sistema', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'referência'
---

<div id="description">
  ## Descrição
</div>

Mostra os [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) ativos no momento, registrados por este servidor ClickHouse em nós do ZooKeeper (incluindo ZooKeepers auxiliares). Cada linha representa um watch.

<div id="columns">
  ## Colunas
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — Nome da conexão com o ZooKeeper (`default` para a conexão principal ou o nome auxiliar).
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Momento em que o watch foi criado.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Momento em que o watch foi criado, com precisão de microssegundos.
* `path` ([String](../../sql-reference/data-types/string.md)) — Caminho do ZooKeeper que está sendo observado.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — ID da sessão da conexão que registrou o watch.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID da requisição que criou o watch.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — Tipo da requisição que criou o watch.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Tipo de watch. Valores possíveis:
  * `Children` — observa alterações na lista de nós filhos (definido por operações `List`).
  * `Exists` — observa a criação ou exclusão de nós.
  * `Data` — observa alterações nos dados do nó (definido por operações `Get`).

Exemplo:

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**Veja também**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [Guia do ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)