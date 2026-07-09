---
description: 'Documentação sobre índices hipotéticos (what-if)'
sidebar_label: 'ÍNDICE HIPOTÉTICO'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: 'Índices Hipotéticos'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # Índices hipotéticos
</div>

Índices hipotéticos são skip indexes virtuais, com escopo de sessão, que você pode anexar a uma tabela da família `MergeTree` sem realmente criá-los nem armazená-los. Eles existem apenas na sessão atual e são usados por [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif) para estimar como um skip index real afetaria uma consulta — normalmente a taxa de descarte (fração de marcas que poderiam ser descartadas) e um custo aproximado em marcas e bytes.

Use índices hipotéticos para avaliar índices candidatos antes de arcar com o custo de materializá-los em disco.

<div id="create-hypothetical-index">
  ## CREATE HYPOTHETICAL INDEX
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

A sintaxe segue `ALTER TABLE ... ADD INDEX`, mas nenhum índice é construído nem gravado — apenas a descrição do índice é armazenada na sessão atual.

* `name` — nome do índice; deve ser único em `(database, table)` para esta sessão.
* `expression` — a coluna ou expressão a ser indexada.
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. `text` e `vector_similarity` não têm suporte e são rejeitados no `CREATE`, porque a validação real de `ALTER TABLE ... ADD INDEX` depende de configurações no nível da tabela que o armazenamento restrito à sessão não consegue replicar.
* `GRANULARITY value` — número de grânulos de dados por grânulo de índice. O padrão é 1.

A tabela de destino deve ser uma tabela da família `MergeTree` em um banco de dados `Atomic` (ela deve ter um UUID). Tabelas sem UUID — por exemplo, em um banco de dados `Ordinary` legado ou em `MergeTree` com sintaxe antiga — são rejeitadas, porque o armazenamento de sessão identifica os índices hipotéticos pelo UUID da tabela.

**Exemplo**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## Avaliando um índice hipotético com EXPLAIN WHATIF
</div>

Definir um índice hipotético, por si só, não produz efeito algum — para ver como ele afetaria uma consulta, execute [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif) em um `SELECT` representativo. O estimador informa a aplicabilidade de cada índice candidato, as marcas que seriam lidas, a taxa de descarte resultante e como a estimativa foi produzida (`empirical`, `statistical` ou `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

Resultado:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` é uma estimativa baseada no tamanho médio das linhas da tabela, portanto o valor exato varia conforme o armazenamento e a compressão.

Para ignorar a varredura empírica em memória e estimar a partir das [estatísticas de coluna](/pt-BR/engines/table-engines/mergetree-family/mergetree#column-statistics), primeiro defina essas estatísticas nas colunas relevantes (elas ficam desativadas por padrão), aguarde a conclusão da mutação de materialização e, em seguida, desative o caminho empírico:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

Consulte a referência do [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif) para conferir o esquema de saída completo e as configurações.

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

Remove um índice hipotético da sessão atual.

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

Remove todos os índices hipotéticos definidos na sessão atual, independentemente da tabela.

<div id="scope-and-lifetime">
  ## Escopo e ciclo de vida
</div>

* Índices hipotéticos existem apenas na **sessão atual** — ficam invisíveis para outras sessões e são descartados quando a sessão termina.
* Definir ou remover um deles não cria nenhum índice nem afeta consultas comuns na tabela. O `EXPLAIN WHATIF` empírico de fato lê dados da tabela para criar o índice candidato em memória, e essa varredura conta para os limites de leitura e as quotas da sessão.
* Inspecione os índices hipotéticos da sessão atual por meio de [`system.hypothetical_indexes`](/pt-BR/operations/system-tables/hypothetical_indexes).

<div id="limitations">
  ## Limitações
</div>

Os candidatos `text` e `vector_similarity` são rejeitados no momento de `CREATE HYPOTHETICAL INDEX`, porque sua validação real depende de configurações no nível da tabela que o armazenamento restrito à sessão não consegue replicar.

`EXPLAIN WHATIF` informa `status: not_applicable` para consultas com `FINAL` (a poda do skip index interage com `PrimaryKeyExpand`) e retorna erros com `NOT_IMPLEMENTED` quando a consulta é atendida por uma projeção (um índice da tabela pai não é materializado nas partes da projeção).

A `skip_ratio` empírica é um **limite superior**: ela contabiliza cada grânulo sobrevivente de forma independente e não modela a coalescência de lacunas de seek (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`), nem a combinação de um candidato com um skip index existente sob um predicado disjuntivo (`OR`). Portanto, um índice materializado real pode ler um pouco mais ou fazer a poda em casos que a estimativa não prevê.

<div id="required-privileges">
  ## Privilégios necessários
</div>

`CREATE HYPOTHETICAL INDEX` requer `SELECT` nas colunas referenciadas pela expressão do índice — `SELECT` no nível da coluna (por exemplo, `GRANT SELECT(b)`) é suficiente — porque o `EXPLAIN WHATIF` empírico lê essas colunas.

`DROP HYPOTHETICAL INDEX` e `DROP ALL HYPOTHETICAL INDEXES` não exigem nenhum privilégio adicional; apenas removem entradas do armazenamento local da sessão.

<div id="see-also">
  ## Veja também
</div>

* [`EXPLAIN WHATIF`](/pt-BR/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/pt-BR/operations/system-tables/hypothetical_indexes)
* [Data skipping indexes](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)