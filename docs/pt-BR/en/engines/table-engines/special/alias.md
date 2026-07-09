---
description: 'O motor de tabela Alias cria um proxy transparente para outra tabela. Todas as operações são encaminhadas para a tabela de destino, enquanto o alias em si não armazena dados.'
sidebar_label: 'Alias'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: 'Motor de tabela Alias'
doc_type: 'reference'
---

<div id="alias-table-engine">
  # Motor de tabela Alias
</div>

O mecanismo `Alias` cria um proxy para outra tabela. Todas as operações de leitura e escrita são encaminhadas à tabela de destino, enquanto o próprio alias não armazena dados e apenas mantém uma referência à tabela de destino.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

Ou com o nome do banco de dados explícito:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
A tabela `Alias` não oferece suporte à definição explícita de colunas. As colunas são herdadas automaticamente da tabela de destino. Isso garante que o alias sempre corresponda ao esquema da tabela de destino.
:::

<div id="engine-parameters">
  ## Parâmetros do mecanismo
</div>

* **`target_db (optional)`** — Nome do banco de dados que contém a tabela de destino.
* **`target_table`** — Nome da tabela de destino.

:::note
Quando `target_db` é omitido e `target_table` não está totalmente qualificado (por exemplo, `Alias('my_table')`), o destino é resolvido no mesmo banco de dados do próprio alias, e não no banco de dados atual da sessão.
:::

<div id="supported-operations">
  ## Operações suportadas
</div>

O mecanismo de tabela `Alias` oferece suporte às principais operações. 

<div id="operations-on-target">
  ### Operações na tabela de destino
</div>

Estas operações são encaminhadas para a tabela de destino:

| Operação                     | Suporte | Descrição                                                       |
| ---------------------------- | ------- | --------------------------------------------------------------- |
| `SELECT`                     | ✅       | Ler dados da tabela de destino                                  |
| `INSERT`                     | ✅       | Gravar dados na tabela de destino                               |
| `INSERT SELECT`              | ✅       | Inserção em lote na tabela de destino                           |
| `ALTER TABLE ADD COLUMN`     | ✅       | Adicionar colunas à tabela de destino                           |
| `ALTER TABLE MODIFY SETTING` | ✅       | Modificar as configurações da tabela de destino                 |
| `ALTER TABLE PARTITION`      | ✅       | Operações de partição (DETACH/ATTACH/DROP) na tabela de destino |
| `ALTER TABLE UPDATE`         | ✅       | Atualizar linhas na tabela de destino (mutação)                 |
| `ALTER TABLE DELETE`         | ✅       | Excluir linhas da tabela de destino (mutação)                   |
| `OPTIMIZE TABLE`             | ✅       | Otimizar a tabela de destino (mesclar partes)                   |
| `TRUNCATE TABLE`             | ✅       | Truncar a tabela de destino                                     |

<div id="operations-on-alias">
  ### Operações no próprio alias
</div>

Essas operações afetam apenas o alias, **não** a tabela de destino:

| Operação       | Suporte | Descrição                                                         |
| -------------- | ------- | ----------------------------------------------------------------- |
| `DROP TABLE`   | ✅       | Remove apenas o alias; a tabela de destino permanece inalterada   |
| `RENAME TABLE` | ✅       | Renomeia apenas o alias; a tabela de destino permanece inalterada |

<div id="usage-examples">
  ## Exemplos de uso
</div>

<div id="basic-alias-creation">
  ### Criação básica de alias
</div>

Crie um alias simples no mesmo banco de dados:

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

<div id="cross-database-alias">
  ### Alias entre bancos de dados
</div>

Crie um alias que aponte para uma tabela em outro banco de dados:

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

<div id="write-operations">
  ### Operações de escrita por meio do alias
</div>

Todas as operações de escrita são encaminhadas à tabela de destino:

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

<div id="schema-modification">
  ### Modificação do esquema
</div>

As operações `ALTER` modificam o esquema da tabela de destino:

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

<div id="data-mutations">
  ### Mutações de dados
</div>

As operações UPDATE e DELETE são suportadas:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

<div id="partition-operations">
  ### Operações de partição
</div>

Para tabelas particionadas, as operações de partição são repassadas:

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

<div id="table-optimization">
  ### Otimização da tabela
</div>

Otimize as operações de mesclagem de partes na tabela de destino:

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

<div id="alias-management">
  ### Gerenciamento de alias
</div>

Os aliases podem ser renomeados ou removidos independentemente:

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```