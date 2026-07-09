---
description: 'Um motor de tabela que armazena séries temporais, ou seja, um conjunto de valores associados
  a timestamps e tags (ou labels).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'Motor de tabela TimeSeries'
doc_type: 'referência'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # Mecanismo de tabela TimeSeries
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Um motor de tabela para armazenar séries temporais, ou seja, um conjunto de valores associados a timestamps e tags (ou labels):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
Este é um recurso experimental que pode mudar de maneira incompatível com versões anteriores em lançamentos futuros.
Habilite o uso do motor de tabela TimeSeries
com a configuração [allow&#95;experimental&#95;time&#95;series&#95;table](/pt-BR/operations/settings/settings#allow_experimental_time_series_table).
Execute o comando `set allow_experimental_time_series_table = 1`.
:::

<div id="syntax">
  ## Sintaxe
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
A palavra-chave `SAMPLES` tem um alias `DATA`, mantido para compatibilidade com versões anteriores.
:::

<div id="usage">
  ## Uso
</div>

É mais fácil começar deixando tudo com os valores padrão (é possível criar uma tabela `TimeSeries` sem especificar uma lista de colunas):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Esta tabela pode ser usada com os seguintes protocolos (é necessário atribuir uma porta na configuração do servidor):

* [prometheus remote-write](/pt-BR/interfaces/prometheus#remote-write)
* [prometheus remote-read](/pt-BR/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### Colunas externas
</div>

As colunas de uma tabela TimeSeries são geradas automaticamente. Essas são as colunas externas: elas não armazenam dados, apenas fornecem a interface para SELECT/INSERT. Os dados reais são armazenados nas [tabelas de destino](#target-tables). Aqui está a lista das colunas externas:

| Nome            | Tipo                                              | Descrição                                                                                                                                                                                                                                             |
| --------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | O nome da métrica                                                                                                                                                                                                                                     |
| `tags`          | `Map(String, String)`                             | map de tags (labels) da série temporal                                                                                                                                                                                                                |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` por padrão | Array de pares (timestamp, valor) de uma série temporal. Os tipos de elemento do timestamp e do escalar da tupla podem ser derivados da declaração `INNER COLUMNS` das samples (consulte [Especificando colunas externas](#specifying-outer-columns)) |
| `metric_family` | `String`                                          | O nome da família de métricas (para metadata de métricas)                                                                                                                                                                                             |
| `type`          | `String`                                          | O tipo da métrica (por exemplo, &quot;counter&quot;, &quot;gauge&quot;)                                                                                                                                                                               |
| `unit`          | `String`                                          | A unidade da métrica                                                                                                                                                                                                                                  |
| `help`          | `String`                                          | A descrição da métrica                                                                                                                                                                                                                                |

Exemplo:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` pode ficar vazio durante a inserção, o que significa que o nome da métrica é especificado em `tags`, sob `__name__`, por exemplo:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

Para inserir os metadados das métricas, insira nas colunas `metric_family`, `type`, `unit` e `help`:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### Especificando colunas externas
</div>

A coluna externa `time_series` pode ser listada explicitamente em uma instrução `CREATE TABLE` para substituir seu tipo padrão `Array(Tuple(DateTime64(3), Float64))`. O ClickHouse extrai os tipos de timestamp e escalares da tupla e os propaga para a tabela interna de samples:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

Isso equivale a declarar diretamente, na cláusula `INNER COLUMNS` de samples, os tipos de coluna de timestamp e de valor:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

Se ambas as formas forem usadas na mesma instrução `CREATE TABLE`, os tipos declarados devem corresponder.

<div id="target-tables">
  ## Tabelas de destino
</div>

Uma tabela `TimeSeries` não tem dados próprios; tudo é armazenado em suas tabelas de destino.
Isso é semelhante ao funcionamento de uma [visão materializada](../../../sql-reference/statements/create/view#materialized-view),
com a diferença de que uma visão materializada tem uma tabela de destino,
enquanto uma tabela `TimeSeries` tem três tabelas de destino chamadas [samples](#samples-table), [tags](#tags-table) e [metrics](#metrics-table).

As tabelas de destino podem ser especificadas explicitamente na consulta `CREATE TABLE`
ou o motor de tabela `TimeSeries` pode gerar automaticamente tabelas de destino internas.

As linhas inseridas em uma tabela `TimeSeries` são transformadas, divididas em blocos e inseridas nessas três tabelas de destino.

As tabelas de destino são as seguintes:

<div id="samples-table">
  ### Tabela *samples*
</div>

A tabela *samples* contém séries temporais associadas a um identificador.

A tabela *samples* deve ter as colunas:

| Nome        | Obrigatória? | Tipo padrão     | Tipos possíveis        | Descrição                                             |
| ----------- | ------------ | --------------- | ---------------------- | ----------------------------------------------------- |
| `id`        | [x]          | `UUID`          | qualquer               | Identifica uma combinação de nomes de métricas e tags |
| `timestamp` | [x]          | `DateTime64(3)` | `DateTime64(X)`        | Um ponto no tempo                                     |
| `value`     | [x]          | `Float64`       | `Float32` ou `Float64` | Um valor associado ao `timestamp`                     |

<div id="tags-table">
  ### Tabela de tags
</div>

A tabela *tags* contém identificadores calculados para cada combinação de nome de métrica e tags.

A tabela *tags* deve ter as colunas:

| Nome                 | Obrigatória? | Tipo padrão                           | Tipos possíveis                                                                                                         | Descrição                                                                                                                                                                                        |
| -------------------- | ------------ | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`                 | [x]          | `UUID`                                | qualquer tipo (deve corresponder ao tipo de `id` na tabela [samples](#samples-table))                                   | Um `id` identifica uma combinação de nome de métrica e tags. A expressão DEFAULT especifica como calcular esse identificador                                                                     |
| `metric_name`        | [x]          | `LowCardinality(String)`              | `String` ou `LowCardinality(String)`                                                                                    | O nome de uma métrica                                                                                                                                                                            |
| `<tag_value_column>` | [ ]          | `String`                              | `String` ou `LowCardinality(String)` ou `LowCardinality(Nullable(String))`                                              | O valor de uma tag específica; o nome da tag e o nome da coluna correspondente são especificados na configuração [tags&#95;to&#95;columns](#settings)                                            |
| `tags`               | [x]          | `Map(LowCardinality(String), String)` | `Map(String, String)` ou `Map(LowCardinality(String), String)` ou `Map(LowCardinality(String), LowCardinality(String))` | Mapa de tags que exclui a tag `__name__`, que contém o nome de uma métrica, e as tags com nomes listados na configuração [tags&#95;to&#95;columns](#settings)                                    |
| `all_tags`           | [ ]          | `Map(String, String)`                 | `Map(String, String)` ou `Map(LowCardinality(String), String)` ou `Map(LowCardinality(String), LowCardinality(String))` | Coluna efêmera; cada linha é um mapa de todas as tags, excluindo apenas a tag `__name__`, que contém o nome de uma métrica. O único propósito dessa coluna é ser usada durante o cálculo de `id` |
| `min_time`           | [ ]          | `Nullable(DateTime64(3))`             | `DateTime64(X)` ou `Nullable(DateTime64(X))`                                                                            | timestamp mínimo da série temporal com esse `id`. A coluna é criada se [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) for `true`                                                   |
| `max_time`           | [ ]          | `Nullable(DateTime64(3))`             | `DateTime64(X)` ou `Nullable(DateTime64(X))`                                                                            | timestamp máximo da série temporal com esse `id`. A coluna é criada se [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) for `true`                                                   |

<div id="metrics-table">
  ### Tabela de métricas
</div>

A tabela *metrics* contém algumas informações sobre as métricas coletadas, os tipos dessas métricas e suas descrições.

A tabela *metrics* deve ter as colunas:

| Nome                 | Obrigatório? | Tipo padrão              | Tipos possíveis                      | Descrição                                                                                                                                                                           |
| -------------------- | ------------ | ------------------------ | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x]          | `String`                 | `String` ou `LowCardinality(String)` | O nome de uma família de métricas                                                                                                                                                   |
| `type`               | [x]          | `LowCardinality(String)` | `String` ou `LowCardinality(String)` | O tipo de uma família de métricas, sendo um de &quot;counter&quot;, &quot;gauge&quot;, &quot;summary&quot;, &quot;stateset&quot;, &quot;histogram&quot;, &quot;gaugehistogram&quot; |
| `unit`               | [x]          | `LowCardinality(String)` | `String` ou `LowCardinality(String)` | A unidade usada em uma métrica                                                                                                                                                      |
| `help`               | [x]          | `String`                 | `String` ou `LowCardinality(String)` | A descrição de uma métrica                                                                                                                                                          |

<div id="creation">
  ## Criação
</div>

Há várias maneiras de criar uma tabela com o motor de tabela `TimeSeries`.
A instrução mais simples

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

na verdade, criará a tabela a seguir (você pode verificar isso executando `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

Assim, as colunas foram geradas automaticamente, e também há três tabelas de destino internas com suas próprias definições de colunas
armazenadas nas cláusulas `INNER COLUMNS`.

As tabelas de destino internas têm nomes como `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
e cada tabela de destino tem seu próprio conjunto de colunas:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## Criando uma tabela AS a partir de uma tabela existente
</div>

A instrução `CREATE TABLE new_table AS existing_table` copia de `existing_table`:

* `SETTINGS`
* `INNER COLUMNS` para cada tipo
* `INNER ENGINE` para cada tipo

A instrução não é permitida se `existing_table` tiver destinos externos.
A lista externa de colunas é regenerada, e não copiada.

<div id="adjusting-column-types">
  ## Ajustando os tipos das colunas
</div>

Você pode ajustar os tipos das colunas nas tabelas de destino internas usando a cláusula `INNER COLUMNS`. Por exemplo, para armazenar timestamps em microssegundos e os valores como `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

A mesma cláusula pode ser usada para especificar codecs e outros atributos da coluna:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## A coluna `id`
</div>

A coluna `id` contém identificadores; cada identificador é calculado com base na combinação entre o nome de uma métrica e tags.
O tipo e a expressão `DEFAULT` usados para gerar os identificadores podem ser personalizados por meio da cláusula `TAGS INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

O tipo da coluna `id` deve ser um dos seguintes: `UUID`, `UInt64`, `UInt128` ou `FixedString(16)`. Se nenhuma expressão `DEFAULT` for fornecida, o ClickHouse a escolherá automaticamente com base no tipo de `id`. Os tipos de `id` declarados nas tabelas internas de samples e tags devem ser iguais.

A configuração `id_generator` oferece a mesma personalização sem usar a cláusula `INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

Se essa configuração estiver definida, ela será usada para gerar `id`, mesmo que o `DEFAULT` da coluna contenha uma expressão diferente.

<div id="tags-and-all-tags">
  ## As colunas `tags` e `all_tags`
</div>

Há duas colunas que contêm mapas com tags — `tags` e `all_tags`. Neste exemplo, elas significam a mesma coisa; no entanto, podem ser diferentes
se a configuração `tags_to_columns` for usada. Essa configuração permite especificar que uma determinada tag seja armazenada em uma coluna separada, em vez de ficar
em um mapa dentro da coluna `tags`:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

Esta instrução adicionará as colunas `instance` e `job` à [tabela de destino](#tags-table) interna `tags`.
Nesse caso, a coluna `tags` não conterá as tags `instance` e `job`,
mas a coluna `all_tags` as conterá. A coluna `all_tags` é temporária e existe apenas para ser usada na expressão DEFAULT
da coluna `id`.

<div id="inner-table-engines">
  ## Motores de tabela das tabelas de destino internas
</div>

Por padrão, as tabelas de destino internas usam os seguintes motores de tabela:

* a tabela [samples](#samples-table) usa [MergeTree](../mergetree-family/mergetree);
* a tabela [tags](#tags-table) usa [AggregatingMergeTree](../mergetree-family/aggregatingmergetree), porque os mesmos dados geralmente são inseridos várias vezes nessa tabela, então precisamos de uma forma
  de remover duplicatas, e também porque é necessário fazer agregação para as colunas `min_time` e `max_time`;
* a tabela [metrics](#metrics-table) usa [ReplacingMergeTree](../mergetree-family/replacingmergetree), porque os mesmos dados geralmente são inseridos várias vezes nessa tabela, então precisamos de uma forma
  de remover duplicatas.

Outros motores de tabela também podem ser usados nas tabelas de destino internas, se isso for especificado:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

A tabela [tags](#tags-table) mantém as colunas de tag (e os Maps `tags`/`all_tags`) fora da sua chave de ordenação,
o que `AggregatingMergeTree` rejeita por padrão (veja [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)).
Isso é seguro neste caso porque essas colunas dependem funcionalmente de `id`, que faz parte da chave de ordenação, de modo que todas as
linhas que uma mesclagem em segundo plano consolida compartilham os mesmos valores. Quando a tabela interna de tags é gerada ou seu
engine é especificado inline como acima, `TimeSeries` define `allow_dimensions_outside_sorting_key = 1` nela automaticamente;
no caso de uma tabela [externa](#external-target-tables) de tags agregadas criada manualmente, você mesmo precisa definir isso.

<div id="external-target-tables">
  ## Tabelas de destino externas
</div>

É possível fazer com que uma tabela `TimeSeries` use uma tabela criada manualmente:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

Os tipos de coluna das tabelas externas (`id`, `timestamp`, `value` e os `<tag_value_column>` listados em [`tags_to_columns`](#settings)) devem corresponder ao que a tabela `TimeSeries` geraria internamente (consulte [Samples table](#samples-table), [Tags table](#tags-table) e [Metrics table](#metrics-table) para as restrições de tipo). Incompatibilidades de tipo são reportadas no momento do `CREATE`.

A expressão geradora de `id` para um destino externo de tags é resolvida no momento do `INSERT` na seguinte ordem: a configuração [`id_generator`](#settings) (se definida), depois o `DEFAULT` declarado na coluna `id` da tabela externa (se houver) e, por fim, o gerador canônico derivado do tipo de `id`. Portanto, a configuração sobrescreve qualquer `DEFAULT` declarado na tabela externa — consulte [The `id` column](#id-column) para mais detalhes.

<div id="altering-settings">
  ## Alterando configurações
</div>

Duas configurações podem ser alteradas após `CREATE`:

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

Observe que alterar `id_generator` quando já há dados na tabela de tags pode gerar IDs diferentes para a mesma combinação de métrica+tag — as linhas antigas mantêm seus IDs antigos, e as novas usam o novo gerador.

As outras configurações não podem ser alteradas com `ALTER ... MODIFY SETTING` porque são incorporadas ao esquema das tabelas internas no momento do `CREATE`.

<div id="settings">
  ## Configurações
</div>

Aqui está uma lista de configurações que podem ser especificadas ao definir uma tabela `TimeSeries`:

| Nome                                 | Tipo       | Padrão                  | Descrição                                                                                                                                                                                                                                                                        |
| ------------------------------------ | ---------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expression | depende do tipo de `id` | Expressão que calcula o identificador (fingerprint) de uma série temporal a partir de suas tags. Se não for definida, será usada a expressão padrão da coluna `id`. Se a expressão padrão da coluna `id` também não estiver definida, a expressão será escolhida automaticamente |
| `tags_to_columns`                    | Map        | {}                      | Map que especifica quais tags devem ser colocadas em colunas separadas na tabela [tags](#tags-table). Sintaxe: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                                                                      |
| `use_all_tags_column_to_generate_id` | Bool       | true                    | Ao gerar uma expressão para calcular o identificador de uma série temporal, esta opção habilita o uso da coluna `all_tags` nesse cálculo                                                                                                                                         |
| `store_min_time_and_max_time`        | Bool       | true                    | Se definido como true, a tabela armazenará `min_time` e `max_time` para cada série temporal                                                                                                                                                                                      |
| `aggregate_min_time_and_max_time`    | Bool       | true                    | Ao criar uma tabela `tags` interna de destino, esta opção habilita o uso de `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` em vez de apenas `Nullable(DateTime64(3))` como tipo da coluna `min_time`, e o mesmo vale para a coluna `max_time`                           |
| `filter_by_min_time_and_max_time`    | Bool       | true                    | Se definido como true, a tabela usará as colunas `min_time` e `max_time` para filtrar séries temporais                                                                                                                                                                           |

<div id="functions">
  # Funções
</div>

Aqui está uma lista de funções que têm uma tabela `TimeSeries` como argumento:

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)