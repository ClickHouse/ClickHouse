---
description: 'O motor `Merge` (não deve ser confundido com `MergeTree`) não armazena
  dados por si só, mas permite ler simultaneamente de um número qualquer de outras tabelas.'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Mecanismo de tabela Merge'
doc_type: 'referência'
---

O motor `Merge` (não deve ser confundido com `MergeTree`) não armazena dados por si só, mas permite ler simultaneamente de um número qualquer de outras tabelas.

A leitura é paralelizada automaticamente. Não há suporte para gravação na tabela. Ao ler, são usados os índices das tabelas que estão sendo efetivamente lidas, se existirem.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## Parâmetros do motor
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — Valores possíveis:

* nome do banco de dados,
  * expressão constante que retorna uma string com o nome de um banco de dados, por exemplo, `currentDatabase()`,
  * `REGEXP(expression)`, em que `expression` é uma expressão regular para corresponder a nomes de bancos de dados.

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — Uma expressão regular para corresponder aos nomes das tabelas no DB especificado ou nos DBs especificados.

Expressões regulares — [re2](https://github.com/google/re2) (suporta um subconjunto de PCRE), diferencia maiúsculas de minúsculas.
Veja as observações sobre escape de símbolos em expressões regulares na seção &quot;match&quot;.

<div id="usage">
  ## Uso
</div>

Ao selecionar tabelas para leitura, a própria tabela `Merge` não é selecionada, mesmo que corresponda à regex. Isso é para evitar loops.
É possível criar duas tabelas `Merge` que tentem ler indefinidamente os dados uma da outra, mas isso não é uma boa ideia.

A forma mais comum de usar a motor `Merge` é trabalhar com um grande número de tabelas `TinyLog` como se fossem uma única tabela.

<div id="examples">
  ## Exemplos
</div>

**Exemplo 1**

Considere dois bancos de dados, `ABC_corporate_site` e `ABC_store`. A tabela `all_visitors` conterá IDs das tabelas `visitors` de ambos os bancos de dados.

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**Exemplo 2**

Digamos que você tenha uma tabela antiga, `WatchLog_old`, e decida alterar o particionamento sem mover os dados para uma nova tabela, `WatchLog_new`, mas precise visualizar os dados de ambas as tabelas.

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_table` — O nome da tabela da qual os dados foram lidos. Tipo: [String](../../../sql-reference/data-types/string.md).

  Se você aplicar um filtro em `_table` (por exemplo, `WHERE _table='xyz'`), somente as tabelas que atenderem à condição de filtro serão lidas.

* `_database` — Contém o nome do banco de dados do qual os dados foram lidos. Tipo: [String](../../../sql-reference/data-types/string.md).

**Veja também**

* [Colunas virtuais](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* função de tabela [merge](../../../sql-reference/table-functions/merge.md)