---
description: 'O motor Memory armazena dados na RAM, em formato não comprimido. Os dados
  são armazenados exatamente na mesma forma em que são recebidos. Em outras palavras,
  a leitura dessa tabela não tem custo algum.'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Motor de tabela Memory'
doc_type: 'reference'
---

:::note
Ao usar o motor de tabela Memory no ClickHouse Cloud, os dados não são replicados em todos os nós (intencionalmente). Para garantir que todas as consultas sejam roteadas para o mesmo nó e que o motor de tabela Memory funcione como esperado, você pode fazer uma das seguintes opções:

* Executar todas as operações na mesma sessão
* Usar um cliente que utilize TCP ou a interface nativa (o que habilita suporte a conexões persistentes), como o [clickhouse-client](/pt-BR/interfaces/client)
  :::

O motor Memory armazena dados na RAM, em formato não comprimido. Os dados são armazenados exatamente na mesma forma em que são recebidos. Em outras palavras, a leitura dessa tabela não tem custo algum.
O acesso simultâneo aos dados é sincronizado. Os bloqueios são curtos: operações de leitura e escrita não bloqueiam umas às outras.
Índices não são suportados. A leitura é paralelizada.

O desempenho máximo (mais de 10 GB/s) é alcançado em consultas simples, porque não há leitura de disco, descompressão nem desserialização de dados. (Vale observar que, em muitos casos, o desempenho do motor MergeTree é quase tão alto quanto.)
Ao reiniciar o servidor, os dados desaparecem da tabela e ela fica vazia.
Normalmente, o uso desse motor de tabela não se justifica. No entanto, ele pode ser usado para testes e para tarefas em que a velocidade máxima é necessária com um número relativamente pequeno de linhas (até aproximadamente 100.000.000).

O motor Memory é usado pelo sistema para tabelas temporárias com dados externos da consulta (consulte a seção &quot;Dados externos para o processamento de uma consulta&quot;) e para implementar `GLOBAL IN` (consulte a seção &quot;operadores IN&quot;).

Limites máximo e mínimo podem ser especificados para limitar o tamanho da tabela do motor Memory, permitindo efetivamente que ela funcione como um buffer circular (consulte [Parâmetros do motor](#engine-parameters)).

<div id="engine-parameters">
  ## Parâmetros do motor
</div>

* `min_bytes_to_keep` — Quantidade mínima de bytes a manter quando a tabela Memory tem limite de tamanho.
  * Valor padrão: `0`
  * Requer `max_bytes_to_keep`
* `max_bytes_to_keep` — Quantidade máxima de bytes a manter na tabela Memory, em que as linhas mais antigas são excluídas a cada inserção (ou seja, um buffer circular). O total máximo de bytes pode exceder o limite indicado se o lote mais antigo de linhas a ser removido ficar abaixo do limite de `min_bytes_to_keep` ao adicionar um bloco grande.
  * Valor padrão: `0`
* `min_rows_to_keep` — Quantidade mínima de linhas a manter quando a tabela Memory tem limite de tamanho.
  * Valor padrão: `0`
  * Requer `max_rows_to_keep`
* `max_rows_to_keep` — Quantidade máxima de linhas a manter na tabela Memory, em que as linhas mais antigas são excluídas a cada inserção (ou seja, um buffer circular). O total máximo de linhas pode exceder o limite indicado se o lote mais antigo de linhas a ser removido ficar abaixo do limite de `min_rows_to_keep` ao adicionar um bloco grande.
  * Valor padrão: `0`
* `compress` - Define se os dados devem ser compactados na memória.
  * Valor padrão: `false`

<div id="usage">
  ## Uso
</div>

**Inicializar as configurações**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Alterar configurações**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Nota:** Os parâmetros de limitação `bytes` e `rows` podem ser definidos ao mesmo tempo; no entanto, os limites mais baixos de `max` e `min` serão respeitados.

<div id="examples">
  ## Exemplos
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

também para linhas:

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```