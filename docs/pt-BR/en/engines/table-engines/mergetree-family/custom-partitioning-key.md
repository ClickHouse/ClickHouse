---
description: 'Aprenda a adicionar uma chave de particionamento personalizada a tabelas MergeTree.'
sidebar_label: 'Chave de Particionamento Personalizada'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: 'Chave de Particionamento Personalizada'
doc_type: 'guide'
---

:::note
Na maioria dos casos, você não precisa de uma chave de particionamento e, na maioria dos demais, também não precisa de uma chave de particionamento mais granular do que mensal, a menos que esteja lidando com um caso de uso de observabilidade em que o particionamento diário é comum.

Você nunca deve usar um particionamento granular demais. Não particione seus dados por identificadores ou nomes de clientes. Em vez disso, use o identificador ou nome do cliente como a primeira coluna na expressão `ORDER BY`.
:::

O particionamento está disponível para as [tabelas da família MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), incluindo [tabelas replicadas](../../../engines/table-engines/mergetree-family/replication.md) e [visões materializadas](/pt-BR/sql-reference/statements/create/view#materialized-view).

Uma partição é um agrupamento lógico de registros em uma tabela com base em um critério especificado. Você pode definir uma partição com base em um critério arbitrário, como mês, dia ou tipo de evento. Cada partição é armazenada separadamente para simplificar as operações com esses dados. Ao acessar os dados, o ClickHouse usa o menor subconjunto possível de partições. As partições melhoram o desempenho de consultas que contêm uma chave de particionamento, porque o ClickHouse filtra essa partição antes de selecionar as partes e os grânulos dentro dela.

A partição é especificada na cláusula `PARTITION BY expr` ao [criar uma tabela](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). A chave de particionamento pode ser qualquer expressão das colunas da tabela. Por exemplo, para especificar o particionamento por mês, use a expressão `toYYYYMM(date_column)`:

```sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

A chave de particionamento também pode ser uma tupla de expressões (assim como a [chave primária](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)). Por exemplo:

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

Neste exemplo, definimos o particionamento com base nos tipos de evento que ocorreram durante a semana atual.

Por padrão, não há suporte para chave de particionamento de ponto flutuante. Para usá-la, habilite a configuração [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key).

Ao inserir novos dados em uma tabela, esses dados são armazenados como uma parte separada (fragmento), ordenada pela chave primária. De 10 a 15 minutos após a inserção, as partes da mesma partição são mescladas em uma única parte.

:::info
A mesclagem só funciona para partes de dados que têm o mesmo valor da expressão de particionamento. Isso significa que **você não deve criar partições granulares demais** (mais de cerca de mil partições). Caso contrário, a consulta `SELECT` tem desempenho ruim devido a um número excessivamente grande de arquivos no sistema de arquivos e descritores de arquivo abertos.
:::

Use a tabela [system.parts](../../../operations/system-tables/parts.md) para visualizar as partes de tabela e as partições. Por exemplo, suponha que temos uma tabela `visits` com particionamento por mês. Vamos executar a consulta `SELECT` na tabela `system.parts`:

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

A coluna `partition` contém os nomes das partições. Há duas partições neste exemplo: `201901` e `201902`. Você pode usar o valor dessa coluna para especificar o nome da partição em consultas [ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md).

A coluna `name` contém os nomes das partes de dados da partição. Você pode usar essa coluna para especificar o nome da parte na consulta [ALTER ATTACH PART](/pt-BR/sql-reference/statements/alter/partition#attach-partitionpart).

Vamos detalhar o nome da parte: `201901_1_9_2_11`:

* `201901` é o nome da partição.
* `1` é o número mínimo do bloco de dados.
* `9` é o número máximo do bloco de dados.
* `2` é o nível do fragmento (a profundidade da árvore de mesclagem da qual ele foi formado).
* `11` é a versão da mutação (se uma parte sofreu mutação)

:::info
As partes de tabelas do tipo antigo têm o nome: `20190117_20190123_2_2_0` (data mínima - data máxima - número mínimo do bloco - número máximo do bloco - nível).
:::

A coluna `active` mostra o status da parte. `1` é ativa; `0` é inativa. As partes inativas são, por exemplo, partes de origem que permanecem após a mesclagem em uma parte maior. As partes de dados corrompidas também são indicadas como inativas.

Como você pode ver no exemplo, há várias partes separadas da mesma partição (por exemplo, `201901_1_3_1` e `201901_1_9_2`). Isso significa que essas partes ainda não foram mescladas. O ClickHouse mescla periodicamente as partes de dados inseridas, aproximadamente 15 minutos após a inserção. Além disso, você pode executar uma mesclagem não agendada usando a consulta [OPTIMIZE](../../../sql-reference/statements/optimize.md). Exemplo:

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

As partes inativas serão excluídas cerca de 10 minutos após a mesclagem.

Outra maneira de visualizar um conjunto de partes e partições é acessar o diretório da tabela: `/var/lib/clickhouse/data/<database>/<table>/`. Por exemplo:

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

As pastas &#39;201901&#95;1&#95;1&#95;0&#39;, &#39;201901&#95;1&#95;7&#95;1&#39; e assim por diante são os diretórios das partes. Cada parte corresponde a uma partição e contém dados apenas de um determinado mês (a tabela neste exemplo tem particionamento por mês).

O diretório `detached` contém partes que foram desanexadas da tabela usando a consulta [DETACH](/pt-BR/sql-reference/statements/detach). As partes corrompidas também são movidas para esse diretório, em vez de serem excluídas. O servidor não usa as partes do diretório `detached`. Você pode adicionar, excluir ou modificar os dados nesse diretório a qualquer momento – o servidor não saberá disso até que você execute a consulta [ATTACH](/pt-BR/sql-reference/statements/alter/partition#attach-partitionpart).

Observe que, com o servidor em execução, você não pode alterar manualmente o conjunto de partes nem seus dados no sistema de arquivos, pois o servidor não saberá disso. Para tabelas não replicadas, você pode fazer isso quando o servidor estiver parado, mas isso não é recomendado. Para tabelas replicadas, o conjunto de partes não pode ser alterado em hipótese alguma.

O ClickHouse permite realizar operações com as partições: excluí-las, copiá-las de uma tabela para outra ou criar um backup. Veja a lista de todas as operações na seção [Manipulations With Partitions and Parts](/pt-BR/sql-reference/statements/alter/partition).

<div id="group-by-optimisation-using-partition-key">
  ## Otimização de Agrupar por usando a chave de particionamento
</div>

Para algumas combinações da chave de particionamento da tabela e da chave de agrupar por da consulta, pode ser possível executar a agregação de cada partição de forma independente.
Assim, não será necessário mesclar os dados parcialmente agregados de todas as threads de execução no final,
pois temos a garantia de que cada valor da chave de agrupar por não pode aparecer nos conjuntos de trabalho de duas threads diferentes.

O exemplo típico é:

```sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
O desempenho dessa consulta depende fortemente do layout da tabela. Por isso, a otimização não é habilitada por padrão.
:::

Os principais fatores para um bom desempenho são:

* o número de partições envolvidas na consulta deve ser suficientemente grande (mais de `max_threads / 2`), caso contrário a consulta não aproveitará totalmente a máquina
* as partições não devem ser muito pequenas, para que o processamento em lote não se degrade para processamento linha a linha
* as partições devem ter tamanhos semelhantes, para que todas as threads façam aproximadamente a mesma quantidade de trabalho

:::info
Recomenda-se aplicar alguma função de hash às colunas da cláusula `partition by` para distribuir os dados uniformemente entre as partições.
:::

As configurações relevantes são:

* `allow_aggregate_partitions_independently` - controla se o uso da otimização está habilitado
* `force_aggregate_partitions_independently` - força seu uso quando ele é aplicável do ponto de vista da correção, mas seria desabilitado pela lógica interna que estima seu benefício
* `max_number_of_partitions_for_independent_aggregation` - limite rígido para o número máximo de partições que a tabela pode ter