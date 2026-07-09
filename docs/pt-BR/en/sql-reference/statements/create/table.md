---
description: 'Documentação sobre tabela'
keywords: ['compressão', 'codec', 'esquema', 'DDL']
sidebar_label: 'TABELA'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'referência'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Cria uma nova tabela. Esta consulta pode ter várias formas de sintaxe, dependendo do caso de uso.

Por padrão, as tabelas são criadas apenas no servidor atual. As consultas de DDL distribuído são implementadas por meio da cláusula `ON CLUSTER`, que é [descrita separadamente](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## Variantes de sintaxe
</div>

<div id="with-explicit-schema">
  ### Com Esquema Explícito
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Cria uma tabela chamada `table_name` no banco de dados `db` ou no banco de dados atual, se `db` não estiver definido, com a estrutura especificada entre colchetes e o motor `engine`.
A estrutura da tabela é uma lista de descrições de colunas, índices secundários, projeções e restrições. Se a [chave primária](#primary-key) for compatível com o motor, ela será indicada como parâmetro do motor de tabela.

Uma descrição de coluna é `name type` no caso mais simples. Exemplo: `RegionID UInt32`.

Também é possível definir expressões para valores padrão (veja abaixo).

Se necessário, a chave primária pode ser especificada com uma ou mais expressões-chave.

É possível adicionar comentários às colunas e à tabela.

<div id="with-a-schema-similar-to-other-table">
  ### Com o esquema de uma tabela existente
</div>

O ClickHouse permite copiar o esquema e os dados de uma tabela existente.

Para replicar o esquema de uma tabela existente:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

Isso cria uma tabela com a mesma estrutura de outra tabela.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### Com o Esquema e os Dados de uma Tabela Existente
</div>

Para replicar o esquema e os dados de uma tabela existente:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

Isso cria uma tabela com o mesmo esquema e os mesmos dados de uma tabela existente. Após a criação da nova tabela, todas as partições de `db.table` são anexadas a ela. Em outras palavras, os dados de `db.table` são clonados para `db2.table_clone` no momento da criação. Esta consulta é equivalente à seguinte:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

Em ambos os casos, você pode especificar um motor diferente para a tabela. Se nenhum motor for especificado, será usado o mesmo motor da tabela original (`db.table`).

<div id="from-a-table-function">
  ### A partir de uma função de tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Cria uma tabela com o mesmo resultado da [função de tabela](/pt-BR/sql-reference/table-functions) especificada. A tabela criada também funcionará da mesma forma que a função de tabela correspondente especificada.

<div id="from-select-query">
  ### Da consulta SELECT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Cria uma tabela com uma estrutura igual à do resultado da consulta `SELECT`, com o motor `engine`, e a preenche com dados de `SELECT`. Também é possível especificar explicitamente a descrição das colunas.

Se a tabela já existir e `IF NOT EXISTS` for especificado, a consulta não fará nada.

Pode haver outras cláusulas após a cláusula `ENGINE` na consulta. Veja a documentação detalhada sobre como criar tabelas nas descrições dos [motores de tabela](/pt-BR/engines/table-engines).

**Exemplo**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## Modificadores NULL ou NOT NULL
</div>

Os modificadores `NULL` e `NOT NULL` após o tipo de dado na definição da coluna permitem ou não que ele seja [Nullable](/pt-BR/sql-reference/data-types/nullable).

Se o tipo não for `Nullable` e `NULL` for especificado, ele será tratado como `Nullable`; se `NOT NULL` for especificado, não será. Por exemplo, `INT NULL` é o mesmo que `Nullable(INT)`. Se o tipo for `Nullable` e forem especificados os modificadores `NULL` ou `NOT NULL`, uma exceção será gerada.

Veja também a configuração [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable).

<div id="default_values">
  ## Valores padrão
</div>

A descrição da coluna pode especificar uma expressão de valor padrão na forma `DEFAULT expr`, `MATERIALIZED expr` ou `ALIAS expr`. Exemplo: `URLDomain String DEFAULT domain(URL)`.

A expressão `expr` é opcional. Se for omitida, o tipo da coluna deverá ser especificado explicitamente, e o valor padrão será `0` para colunas numéricas, `''` (a string vazia) para colunas String, `[]` (o Array vazio) para colunas Array, `1970-01-01` para colunas de data ou `NULL` para colunas Nullable.

O tipo de uma coluna com valor padrão pode ser omitido; nesse caso, ele é inferido a partir do tipo de `expr`. Por exemplo, o tipo da coluna `EventDate DEFAULT toDate(EventTime)` será Date.

Se forem especificados tanto um tipo de dado quanto uma expressão de valor padrão, será inserida uma função implícita de conversão de tipo para converter a expressão para o tipo especificado. Exemplo: `Hits UInt32 DEFAULT 0` é representado internamente como `Hits UInt32 DEFAULT toUInt32(0)`.

Uma expressão de valor padrão `expr` pode referenciar colunas e constantes arbitrárias da tabela. O ClickHouse verifica se alterações na estrutura da tabela não introduzem loops no cálculo da expressão. Para INSERT, ele verifica se as expressões podem ser resolvidas — isto é, se todas as colunas a partir das quais podem ser calculadas foram fornecidas.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

Valor padrão comum. Se o valor dessa coluna não for especificado em uma consulta INSERT, ele será calculado a partir de `expr`.

Exemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

Expressão materializada. Os valores dessas colunas são calculados automaticamente de acordo com a expressão materializada especificada quando as linhas são inseridas. Os valores não podem ser especificados explicitamente durante `INSERT`s.

Além disso, as colunas com valor padrão desse tipo não são incluídas no resultado de `SELECT *`. Isso serve para preservar a propriedade de que o resultado de um `SELECT *` sempre pode ser inserido de volta na tabela usando `INSERT`. Esse comportamento pode ser desativado com a configuração `asterisk_include_materialized_columns`.

Exemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

Coluna efêmera. Colunas desse tipo não são armazenadas na tabela, e não é possível executar `SELECT` nelas. A única finalidade das colunas efêmeras é construir, a partir delas, expressões de valor padrão para outras colunas.

Uma inserção sem colunas explicitamente especificadas ignorará colunas desse tipo. Isso preserva a invariante de que o resultado de um `SELECT *` sempre pode ser inserido de volta na tabela usando `INSERT`.

Exemplo:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

Colunas calculadas (sinônimo). Colunas desse tipo não são armazenadas na tabela e não é possível fazer INSERT de valores nelas.

Quando consultas SELECT fazem referência explícita a colunas desse tipo, o valor é calculado no momento da consulta a partir de `expr`. Por padrão, `SELECT *` exclui colunas ALIAS. Esse comportamento pode ser desativado com a configuração `asterisk_include_alias_columns`.

Ao usar a consulta ALTER para adicionar novas colunas, os dados antigos dessas colunas não são gravados. Em vez disso, ao ler dados antigos que não têm valores para as novas colunas, as expressões são calculadas dinamicamente por padrão. No entanto, se a execução das expressões exigir outras colunas que não estejam indicadas na consulta, essas colunas também serão lidas, mas apenas para os blocos de dados que precisarem disso.

Se você adicionar uma nova coluna a uma tabela, mas depois alterar sua expressão padrão, os valores usados para os dados antigos mudarão (para dados cujos valores não foram armazenados em disco). Observe que, ao executar mesclagens em segundo plano, os dados de colunas ausentes em uma das partes em mesclagem são gravados na parte mesclada.

Não é possível definir valores padrão para elementos em estruturas de dados aninhadas.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## Chave primária
</div>

Você pode definir uma [chave primária](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries) ao criar uma tabela. A chave primária pode ser especificada de duas formas:

* Na lista de colunas

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* Fora da lista de colunas

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
Não é possível combinar as duas formas em uma única consulta.
:::

<div id="constraints">
  ## Restrições
</div>

Além das descrições das colunas, também é possível definir restrições:

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` pode ser qualquer expressão booleana. Se houver restrições definidas para a tabela, cada uma delas será verificada para cada linha na consulta `INSERT`. Se alguma restrição não for atendida — o servidor gerará uma exceção com o nome da restrição e a expressão verificada.

Adicionar uma grande quantidade de restrições pode afetar negativamente o desempenho de consultas `INSERT` grandes.

As restrições existentes em todas as tabelas podem ser inspecionadas por meio da tabela [`system.constraints`](/pt-BR/operations/system-tables/constraints).

<div id="assume">
  ### ASSUME
</div>

A cláusula `ASSUME` é usada para definir uma `CONSTRAINT` em uma tabela que é considerada verdadeira. Essa restrição pode então ser usada pelo otimizador para melhorar o desempenho das consultas SQL.

Considere este exemplo em que `ASSUME CONSTRAINT` é usado na criação da tabela `users_a`:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

Aqui, `ASSUME CONSTRAINT` é usado para afirmar que a função `length(name)` sempre corresponde ao valor da coluna `name_len`. Isso significa que, sempre que `length(name)` for chamada em uma consulta, o ClickHouse poderá substituí-la por `name_len`, o que deve ser mais rápido, pois evita chamar a função `length()`.

Então, ao executar a consulta `SELECT name FROM users_a WHERE length(name) < 5;`, o ClickHouse pode otimizá-la para `SELECT name FROM users_a WHERE name_len < 5`; devido a `ASSUME CONSTRAINT`. Isso pode fazer a consulta ser executada mais rapidamente, pois evita calcular o comprimento de `name` para cada linha.

`ASSUME CONSTRAINT` **não impõe a restrição**; ele apenas informa ao otimizador que a restrição é verdadeira. Se a restrição não for realmente verdadeira, os resultados das consultas poderão estar incorretos. Portanto, você só deve usar `ASSUME CONSTRAINT` se tiver certeza de que a restrição é verdadeira.

<div id="ttl-expression">
  ## Expressão TTL
</div>

Define o tempo de retenção dos valores. Pode ser especificada apenas para tabelas da família MergeTree. Para uma descrição detalhada, consulte [TTL para colunas e tabelas](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

<div id="column_compression_codec">
  ## Codecs de compressão de colunas
</div>

Por padrão, o ClickHouse aplica compressão `lz4` na versão autogerenciada e `zstd` no ClickHouse Cloud.

Para a família de engines `MergeTree`, você pode alterar o método de compressão padrão na seção [compression](/pt-BR/operations/server-configuration-parameters/settings#compression) da configuração do servidor.

Você também pode definir o método de compressão para cada coluna individual na consulta `CREATE TABLE`.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

O codec `Default` pode ser especificado para se referir à compressão padrão, que pode depender de diferentes configurações (e das propriedades dos dados) em tempo de execução.
Exemplo: `value UInt64 CODEC(Default)` — equivale a não especificar um codec.

Você também pode remover o CODEC atual da coluna e usar a compressão padrão de `config.xml`:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Os codecs podem ser combinados em um pipeline, por exemplo, `CODEC(Delta, Default)`.

:::tip
Não é possível descompactar arquivos de banco de dados do ClickHouse com utilitários externos, como `lz4`. Em vez disso, use o utilitário especial [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).
:::

A compressão é compatível com os seguintes motores de tabela:

* Família [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md). Oferece suporte a codecs de compressão de coluna e à seleção do método de compressão padrão nas configurações de [compressão](/pt-BR/operations/server-configuration-parameters/settings#compression).
* Família [Log](../../../engines/table-engines/log-family/index.md). Usa o método de compressão `lz4` por padrão e oferece suporte a codecs de compressão de coluna.
* [Set](../../../engines/table-engines/special/set.md). Compatível apenas com a compressão padrão.
* [Join](../../../engines/table-engines/special/join.md). Compatível apenas com a compressão padrão.

O ClickHouse oferece suporte a codecs de uso geral e codecs especializados.

<div id="general-purpose-codecs">
  ### Codecs de uso geral
</div>

<div id="none">
  #### NONE
</div>

`NONE` — Sem compressão.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — [Algoritmo de compressão de dados](https://github.com/lz4/lz4) sem perda usado por padrão. Aplica a compressão rápida LZ4.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — algoritmo LZ4 HC (alta compressão) com nível configurável. Nível padrão: 9. Definir `level <= 0` aplica o nível padrão. Níveis possíveis: [1, 12]. Faixa de níveis recomendada: [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [algoritmo de compressão ZSTD](https://en.wikipedia.org/wiki/Zstandard) com `level` configurável. Níveis possíveis: [1, 22]. Nível padrão: 1.

Níveis mais altos de compressão são úteis em cenários assimétricos, como compactar uma vez e descompactar repetidamente. Níveis mais altos significam melhor compressão e maior uso de CPU.

<div id="zstd_qat">
  #### Obsoleto: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### Obsoleto: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### Codecs especializados
</div>

Esses codecs foram projetados para tornar a compressão mais eficaz ao explorar características específicas dos dados. Alguns deles não comprimem os dados diretamente; em vez disso, fazem um pré-processamento dos dados para que uma segunda etapa de compressão, com um codec de uso geral, possa alcançar uma taxa de compressão mais alta.

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — Método de compressão em que os valores brutos são substituídos pela diferença entre dois valores vizinhos, exceto o primeiro valor, que permanece inalterado. `delta_bytes` é o tamanho máximo dos valores brutos; o valor padrão é `sizeof(type)`. Especificar `delta_bytes` como argumento está descontinuado, e o suporte será removido em um lançamento futuro. Delta é um codec de preparação de dados, ou seja, não pode ser usado de forma independente.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — Calcula o delta dos deltas e o grava em formato binário compacto. O `bytes_size` tem significado semelhante ao de `delta_bytes` no codec [Delta](#delta). Especificar `bytes_size` como argumento está obsoleto, e o suporte será removido em um futuro lançamento. As melhores taxas de compressão são obtidas em sequências monotônicas com passo constante, como dados de séries temporais. Pode ser usado com qualquer tipo numérico. Implementa o algoritmo usado no Gorilla TSDB, estendendo-o para oferecer suporte a tipos de 64 bits. Usa 1 bit extra para deltas de 32 bits: prefixos de 5 bits em vez de prefixos de 4 bits. Para mais informações, consulte Compressing Time Stamps em [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta é um codec de preparação de dados, ou seja, não pode ser usado isoladamente.

<div id="gcd">
  #### GCD
</div>

`GCD()` - - Calcula o máximo divisor comum (GCD) dos valores da coluna e, em seguida, divide cada valor pelo GCD. Pode ser usado com colunas de inteiros, decimais e data/hora. O codec é especialmente adequado para colunas com valores que variam (aumentam ou diminuem) em múltiplos do GCD, por exemplo, 24, 28, 16, 24, 8, 24 (GCD = 4). GCD é um codec de preparação de dados, ou seja, não pode ser usado isoladamente.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — Calcula o XOR entre o valor de ponto flutuante atual e o anterior e o grava em forma binária compacta. Quanto menor for a diferença entre valores consecutivos, ou seja, quanto mais lentamente os valores da série mudarem, melhor será a taxa de compressão. Implementa o algoritmo usado no Gorilla TSDB, estendendo-o para oferecer suporte a tipos de 64 bits. Valores possíveis de `bytes_size`: 1, 2, 4, 8; o valor padrão é `sizeof(type)` se for igual a 1, 2, 4 ou 8. Em todos os outros casos, é 1. Para mais informações, consulte a seção 4.1 em [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — Compressão adaptativa sem perdas para dados de ponto flutuante. Suporta `Float32` e `Float64`. Para mais detalhes, consulte [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

O codec aceita um argumento opcional de variante:

* `ALP()` ou `ALP(AUTO)` (padrão) — Usa STD e recorre a RD com base no tamanho comprimido estimado.
* `ALP(STD)` — Variante padrão do ALP. Representa cada valor como um inteiro escalonado exato usando potências decimais e, em seguida, comprime os inteiros resultantes com Frame-of-Reference e empacotamento de bits. Valores não representáveis são armazenados como exceções brutas. Funciona melhor para números provenientes de valores decimais (por exemplo, medições, preços).
* `ALP(RD)` — Variante Real Doubles. Reinterpreta o padrão de bits de cada valor e o divide em uma parte alta (sinal + expoente + bits superiores da mantissa) e uma parte baixa. As partes altas são codificadas com dicionário (até 8 entradas), e as partes baixas são empacotadas em bits. Funciona melhor quando muitos valores compartilham os mesmos bits altos.

:::note
Este codec é experimental e requer `SET allow_experimental_codecs = 1` para ser usado.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - Prevê repetidamente o próximo valor de ponto flutuante na sequência usando o melhor de dois preditores, depois aplica XOR entre o valor real e o valor previsto e comprime o resultado com zeros à esquerda. Semelhante ao Gorilla, ele é eficiente para armazenar uma série de valores de ponto flutuante que mudam lentamente. Para valores de 64 bits (double), o FPC é mais rápido que o Gorilla; para valores de 32 bits, o desempenho pode variar. Valores possíveis de `level`: 1-28, o valor padrão é 12. Valores possíveis de `float_size`: 4, 8, o valor padrão é `sizeof(type)` se o tipo for Float. Em todos os outros casos, é 4. Para uma descrição detalhada do algoritmo, consulte [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

<div id="t64">
  #### T64
</div>

`T64` — Abordagem de compressão que elimina os bits altos não utilizados dos valores em tipos de dados inteiros (incluindo `Enum`, `Date` e `DateTime`). Em cada passo do algoritmo, o codec pega um bloco de 64 valores, coloca-os em uma matriz de bits 64x64, transpõe essa matriz, elimina os bits não utilizados dos valores e retorna o restante como uma sequência. Bits não utilizados são os bits que não diferem entre os valores máximo e valor mínimo em toda a data part para a qual a compressão é usada.

Os codecs `DoubleDelta` e `Gorilla` são usados no Gorilla TSDB como componentes do seu algoritmo de compressão. A abordagem Gorilla é eficaz em cenários em que há uma sequência de valores que mudam lentamente com seus timestamps. Os timestamps são comprimidos com eficiência pelo codec `DoubleDelta`, e os valores são comprimidos com eficiência pelo codec `Gorilla`. Por exemplo, para obter uma tabela armazenada com eficiência, você pode criá-la com a seguinte configuração:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### Codecs de Criptografia
</div>

Esses codecs não comprimem os dados de fato; em vez disso, criptografam os dados em disco. Eles só estão disponíveis quando uma chave de criptografia é especificada nas configurações de [encryption](/pt-BR/operations/server-configuration-parameters/settings#encryption). Observe que a criptografia só faz sentido no fim dos pipelines de codecs, porque dados criptografados geralmente não podem ser comprimidos de forma significativa.

Codecs de criptografia:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — Criptografa os dados com AES-128 em modo GCM-SIV, conforme a [RFC 8452](https://tools.ietf.org/html/rfc8452).

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — Criptografa os dados com AES-256 no modo GCM-SIV.

Esses codecs usam um nonce fixo e, portanto, a criptografia é determinística. Isso os torna compatíveis com motores com deduplicação, como o [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md), mas há uma fraqueza: quando o mesmo bloco de dados é criptografado duas vezes, o ciphertext resultante será exatamente o mesmo, de modo que um invasor com acesso de leitura ao disco poderá perceber essa equivalência (embora apenas a equivalência, sem acessar o conteúdo).

:::note
A maioria dos motores, incluindo a família &quot;*MergeTree&quot;, cria arquivos de índice no disco sem aplicar codecs. Isso significa que o plaintext aparecerá no disco se uma coluna criptografada for indexada.
:::

:::note
Se você executar uma consulta SELECT que mencione um valor específico em uma coluna criptografada (como em sua cláusula WHERE), o valor poderá aparecer em [system.query&#95;log](../../../operations/system-tables/query_log.md). Talvez seja melhor desativar o logging.
:::

**Exemplo**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
Se for necessário aplicar compressão, ela deverá ser especificada explicitamente. Caso contrário, apenas a criptografia será aplicada aos dados.
:::

**Exemplo**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## Tabelas Temporárias
</div>

:::note
Observe que tabelas temporárias não são replicadas. Como resultado, não há garantia de que os dados inseridos em uma tabela temporária estarão disponíveis em outras réplicas. O principal caso de uso em que tabelas temporárias podem ser úteis é na consulta ou no `join` com pequenos conjuntos de dados externos durante uma única sessão.
:::

O ClickHouse oferece suporte a tabelas temporárias, que têm as seguintes características:

* As tabelas temporárias desaparecem quando a sessão termina, inclusive se a conexão for perdida.
* Uma tabela temporária usa o mecanismo de tabela Memory quando nenhum motor é especificado, e pode usar qualquer mecanismo de tabela, exceto os motores Replicated e `KeeperMap`.
* O DB não pode ser especificado para uma tabela temporária. Ela é criada fora dos bancos de dados.
* É impossível criar uma tabela temporária com uma consulta DDL distribuída em todos os servidores do cluster (usando `ON CLUSTER`): essa tabela existe apenas na sessão atual.
* Se uma tabela temporária tiver o mesmo nome que outra e uma consulta especificar o nome da tabela sem especificar o DB, a tabela temporária será usada.
* Para o processamento de consultas distribuídas, as tabelas temporárias com motor Memory usadas em uma consulta são passadas para servidores remotos.

Para criar uma tabela temporária, use a seguinte sintaxe:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

Na maioria dos casos, as tabelas temporárias não são criadas manualmente, mas sim ao usar dados externos em uma consulta ou `(GLOBAL) IN` distribuído. Para mais informações, consulte as seções apropriadas.

É possível usar tabelas com [ENGINE = Memory](../../../engines/table-engines/special/memory.md) em vez de tabelas temporárias.

<div id="replace-table">
  ## REPLACE TABLE
</div>

A instrução `REPLACE` permite atualizar uma tabela [atomicamente](/pt-BR/concepts/glossary#atomicity).

:::note
Esta instrução é compatível com os motores de banco de dados [`Atomic`](../../../engines/database-engines/atomic.md) e [`Replicated`](../../../engines/database-engines/replicated.md),
que são os motores de banco de dados padrão do ClickHouse e do ClickHouse Cloud, respectivamente.
:::

Em geral, se você precisar excluir parte dos dados de uma tabela,
pode criar uma nova tabela e preenchê-la usando uma instrução `SELECT` que não retorne os dados indesejados,
depois excluir a tabela antiga e renomear a nova.
Essa abordagem é demonstrada no exemplo abaixo:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

Em vez da abordagem acima, também é possível usar `REPLACE` (desde que você esteja usando os motores de banco de dados padrão) para obter o mesmo resultado:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### Sintaxe
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
Todas as sintaxes da instrução `CREATE` também funcionam para esta instrução. Executar `REPLACE` em uma tabela que não existe causará um erro.
:::

<div id="examples">
  ### Exemplos:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="Local" default>
    Considere a tabela a seguir:

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    Podemos usar a instrução `REPLACE` para remover todos os dados:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    Ou podemos usar a instrução `REPLACE` para alterar a estrutura da tabela:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    Considere a tabela a seguir no ClickHouse Cloud:

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    Podemos usar a instrução `REPLACE` para remover todos os dados:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    Ou podemos usar a instrução `REPLACE` para alterar a estrutura da tabela:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## Cláusula COMMENT
</div>

Você pode adicionar um comentário à tabela durante sua criação.

**Sintaxe**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
A cláusula `COMMENT` deve ser especificada **depois** de quaisquer cláusulas específicas de armazenamento, como `PARTITION BY`, `ORDER BY` e `SETTINGS` específicos de armazenamento.

Depois da cláusula `COMMENT`, apenas `SETTINGS` específicos de consulta (como `max_threads` etc.) serão processados, não configurações relacionadas ao armazenamento.

Isso significa que a ordem correta das cláusulas é:

* `ENGINE`
* cláusulas de armazenamento
* `COMMENT`
* SETTINGS de consulta (se houver)
  :::

**Exemplo**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Otimizando o ClickHouse com esquemas e codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog: [Trabalhando com dados de séries temporais no ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)